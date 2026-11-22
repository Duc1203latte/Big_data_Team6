"""entity_resolution: ghép cùng sản phẩm giữa các sàn, ghi Gold product_master (tuần 3, bước 1).

Luồng:
    (A) Spark đọc Silver, gom về 1 dòng / product_key (~12.000 dòng) rồi đưa về driver (pandas)
    (B) chuẩn hoá bằng common/product_text.py: name_clean, brand, category, ram/storage, variants, model_nums
    (C) embedding tên (fastembed, paraphrase-multilingual-MiniLM-L12-v2, CPU)
    (D) Qdrant: top-k láng giềng ở sàn khác, cùng category -> cặp ứng viên
    (E) luật cứng + gom cụm (common/entity_rules.py) -> master_id
    (F) ghi Gold product_master, er_candidates (cho bước 2 gán nhãn), in tỉ lệ khớp

Spark chỉ lo đọc/ghi (đọc được Silver trên HDFS); phần giữa chạy trên driver vì dữ liệu nhỏ và cần
thư viện Python (fastembed, qdrant-client) -> trên cụm dùng image Spark có sẵn các thư viện này
(infra/spark/Dockerfile).

Chạy trên máy cá nhân (Qdrant: docker run hoặc docker compose --profile serving):
    bash spark/run_local.sh jobs/entity_resolution.py \
        --silver-root ~/data/silver_20261116 --gold-root datalake_local/gold \
        --qdrant-url http://localhost:6333
Trên máy chính:
    bash spark/submit.sh --mem 3g --conf spark.driver.memory=2g jobs/entity_resolution.py \
        --qdrant-url http://qdrant:6333 --model-cache /opt/spark/work-dir/models
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import uuid
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from fastembed import TextEmbedding
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PayloadSchemaType,
    PointStruct,
    QueryRequest,
    SearchParams,
    VectorParams,
)

from common import entity_rules as er
from common import product_text as pt

log = logging.getLogger("entity_resolution")

EMBED_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
COLLECTION = "products"
UPSERT_BATCH = 500      # số điểm gửi lên Qdrant mỗi lần
QUERY_BATCH = 256       # số truy vấn gửi mỗi lần

EMBED_BATCH_SIZE = 256
# Dung lượng bỏ khỏi chuỗi embed: luật storage_gb ở khâu E đã lo, để lại thì fptshop (không ghi GB) bị đẩy xa
_EMBED_CAPACITY_RE = re.compile(r"\b\d+(?:[.,]\d+)?\s*(?:gb|tb)\b")

PRICE_WINDOW_DAYS = 7                       # giá tiêu biểu = trung vị trong 7 ngày gần nhất
SPEC_STORAGE_KEYS = ("Bộ nhớ trong",)       # khoá trong specs_raw (cellphones, hoangha)
SPEC_RAM_KEYS = ("RAM", "Dung lượng RAM")
CANDIDATE_MIN_SCORE = 0.6                   # cặp ứng viên lưu lại cho bước 2 (gán nhãn)

MASTER_SCHEMA = T.StructType([
    T.StructField("product_key", T.StringType(), False),
    T.StructField("platform", T.StringType(), False),
    T.StructField("master_id", T.StringType(), False),
    T.StructField("cluster_size", T.IntegerType(), False),
    T.StructField("match_score", T.DoubleType()),
    T.StructField("name_rep", T.StringType()),
    T.StructField("name_clean", T.StringType()),
    T.StructField("brand", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("ram_gb", T.IntegerType()),
    T.StructField("storage_gb", T.IntegerType()),
    T.StructField("variants", T.ArrayType(T.StringType())),
    T.StructField("model_nums", T.ArrayType(T.StringType())),
    T.StructField("price_median", T.DoubleType()),
    T.StructField("n_obs", T.LongType()),
])

CANDIDATE_SCHEMA = T.StructType(
    [T.StructField("key_a", T.StringType()), T.StructField("key_b", T.StringType()),
     T.StructField("score", T.DoubleType()), T.StructField("price_ratio", T.DoubleType())]
    + [T.StructField(c, T.BooleanType()) for c in (*er.CHECKS, "matched", "clustered")]
)


def build_products(spark: SparkSession, silver_root: str) -> pd.DataFrame:
    """(A) 1 dòng / product_key: platform, n_obs, name_rep (tên xuất hiện nhiều nhất, bỏ "FLASH SALE"),
    price_median (7 ngày gần nhất, không có thì toàn bộ), brand_raw/category_raw/specs_raw (bản mới nhất)."""
    price = (spark.read.format("delta").load(f"{silver_root}/price")
             .select("product_key", "platform", "name_raw", "price_current", "date"))

    stats = price.groupBy("product_key").agg(F.first("platform").alias("platform"),
                                             F.count("*").alias("n_obs"))

    # Tên nhiều nhất; hoà thì lấy tên nhỏ nhất theo thứ tự chữ để chạy lại ra cùng tên
    names = (price.where(F.upper(F.trim("name_raw")) != "FLASH SALE")
             .groupBy("product_key", "name_raw").count())
    by_count = Window.partitionBy("product_key").orderBy(F.desc("count"), F.asc("name_raw"))
    name_rep = (names.withColumn("rn", F.row_number().over(by_count)).where("rn = 1")
                .select("product_key", F.col("name_raw").alias("name_rep")))

    priced = price.where(F.col("price_current").isNotNull())
    last_day = priced.agg(F.max("date")).first()[0]
    recent = (priced.where(F.col("date") > F.lit(last_day - timedelta(days=PRICE_WINDOW_DAYS)))
              .groupBy("product_key").agg(F.median("price_current").alias("med_recent")))
    overall = priced.groupBy("product_key").agg(F.median("price_current").alias("med_all"))

    product = (spark.read.format("delta").load(f"{silver_root}/product")
               .select("product_key", "brand_raw", "category_raw", "specs_raw", "event_time"))
    newest = Window.partitionBy("product_key").orderBy(F.desc("event_time"))
    latest = (product.withColumn("rn", F.row_number().over(newest)).where("rn = 1")
              .drop("rn", "event_time"))

    out = (stats.join(name_rep, "product_key")          # inner: key chỉ có "FLASH SALE" thì bỏ
           .join(recent, "product_key", "left").join(overall, "product_key", "left")
           .withColumn("price_median", F.coalesce("med_recent", "med_all").cast("double"))
           .drop("med_recent", "med_all")
           .join(latest, "product_key", "left")          # 46% key tiki không có trang chi tiết
           .orderBy("product_key"))
    return pd.DataFrame([r.asDict() for r in out.collect()])


def _none_if_na(value):
    """NaN của pandas -> None. Cần thiết vì `if NaN:` là True (xem giải thích)."""
    return None if pd.isna(value) else value


def _capacity_from_specs(specs_raw: str | None) -> tuple[int | None, int | None]:
    """Lấy (ram_gb, storage_gb) từ specs_raw (chuỗi JSON) khi tên không ghi."""
    if not specs_raw:
        return (None, None)
    try:
        specs = json.loads(specs_raw)
    except (TypeError, ValueError):
        return (None, None)
    if not isinstance(specs, dict):
        return (None, None)
    # Giá trị kiểu "128 GB", "8GB": dùng lại extract_capacity để đọc số
    ram = next((pt.extract_capacity(str(specs[k]))[0] for k in SPEC_RAM_KEYS if k in specs), None)
    storage = next((pt.extract_capacity(str(specs[k]))[1] for k in SPEC_STORAGE_KEYS if k in specs), None)
    return (ram, storage)

def add_features(products: pd.DataFrame) -> pd.DataFrame:
    """(B) Mục 1.1-1.2. Thêm name_clean, brand, category, ram_gb, storage_gb, variants."""
    df = products.copy()
    names = df["name_rep"].tolist()
    brand_raw = [_none_if_na(v) for v in df["brand_raw"]]
    category_raw = [_none_if_na(v) for v in df["category_raw"]]
    specs_raw = [_none_if_na(v) for v in df["specs_raw"]]

    df["name_clean"] = [pt.clean_name(n) for n in names]
    df["brand"] = [pt.normalize_brand(b, n) for b, n in zip(brand_raw, names)]
    df["category"] = [pt.coarse_category(c, n) for c, n in zip(category_raw, names)]

    # RAM / bộ nhớ: ưu tiên tên; thiếu thì lấy từ specs
    ram, storage = [], []
    for name, specs in zip(names, specs_raw):
        r, s = pt.extract_capacity(name)
        if r is None or s is None:
            spec_r, spec_s = _capacity_from_specs(specs)
            r = r if r is not None else spec_r
            s = s if s is not None else spec_s
        ram.append(r)
        storage.append(s)
    df["ram_gb"] = pd.array(ram, dtype="Int64")
    df["storage_gb"] = pd.array(storage, dtype="Int64")

    df["variants"] = [pt.variant_tokens(c) for c in df["name_clean"]]
    df["model_nums"] = [pt.model_numbers(c) for c in df["name_clean"]]
    df["chip"] = [pt.chip_config(c) for c in df["name_clean"]]
    df["chip_tier"] = [pt.chip_tier(c) for c in df["name_clean"]]
    return df

def embed_text(brand: str | None, name_clean: str) -> str:
    """Chuỗi đưa vào model: name_clean bỏ dung lượng, thêm brand ở đầu nếu tên chưa có."""
    s = _EMBED_CAPACITY_RE.sub(" ", name_clean).replace("/", " ")
    s = " ".join(s.split())
    if brand and f" {brand} " not in f" {s} ":
        s = f"{brand} {s}"
    return s


def embed(texts: list[str], cache_dir: Path) -> np.ndarray:
    """(C) Mục 1.3. Trả mảng (n, 384) float32, mỗi hàng đã chuẩn hoá độ dài = 1."""
    model = TextEmbedding(model_name=EMBED_MODEL, cache_dir=str(cache_dir))
    vectors = np.vstack(list(model.embed(texts, batch_size=EMBED_BATCH_SIZE)))
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    return vectors.astype(np.float32)


def point_id(product_key: str) -> str:
    """Qdrant chỉ nhận id là số nguyên hoặc UUID. uuid5 cho cùng product_key -> luôn cùng UUID."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, product_key))


def find_candidates(products: pd.DataFrame, vectors: np.ndarray, qdrant_path: str | None,
                    qdrant_url: str | None, top_k: int) -> pd.DataFrame:
    """(D) Mục 1.4. Nạp Qdrant, tìm top_k láng giềng cùng category, khác platform.
    Trả cặp ứng viên: key_a < key_b, score (cặp trùng giữ điểm cao nhất)."""
    client = QdrantClient(url=qdrant_url) if qdrant_url else QdrantClient(path=qdrant_path)

    # 1) Tạo lại collection mỗi lần chạy -> chạy lại cho cùng kết quả, không sót điểm cũ
    if client.collection_exists(COLLECTION):
        client.delete_collection(COLLECTION)
    client.create_collection(
        COLLECTION,
        vectors_config=VectorParams(size=vectors.shape[1], distance=Distance.COSINE),
    )
    # Chỉ mục cho 2 trường dùng trong bộ lọc (server dùng; chế độ cục bộ bỏ qua và chỉ cảnh báo)
    for field in ("category", "platform"):
        client.create_payload_index(COLLECTION, field_name=field, field_schema=PayloadSchemaType.KEYWORD)

    keys = products["product_key"].tolist()
    platforms = products["platform"].tolist()
    categories = products["category"].tolist()
    brands = [_none_if_na(b) for b in products["brand"]]

    # 2) Nạp điểm theo lô
    for start in range(0, len(keys), UPSERT_BATCH):
        end = start + UPSERT_BATCH
        client.upsert(
            COLLECTION,
            points=[
                PointStruct(
                    id=point_id(k),
                    vector=v.tolist(),
                    payload={"product_key": k, "platform": p, "category": c, "brand": b},
                )
                for k, v, p, c, b in zip(keys[start:end], vectors[start:end], platforms[start:end],
                                         categories[start:end], brands[start:end])
            ],
        )

    # 3) Tìm láng giềng theo lô: cùng category, khác platform
    rows = []
    for start in range(0, len(keys), QUERY_BATCH):
        end = start + QUERY_BATCH
        requests = [
            QueryRequest(
                query=v.tolist(),
                limit=top_k,
                with_payload=["product_key"],
                params=SearchParams(exact=True),     # tìm chính xác: chạy lại ra đúng cặp cũ (HNSW là gần đúng)
                filter=Filter(
                    must=[FieldCondition(key="category", match=MatchValue(value=c))],
                    must_not=[FieldCondition(key="platform", match=MatchValue(value=p))],
                ),
            )
            for v, p, c in zip(vectors[start:end], platforms[start:end], categories[start:end])
        ]
        results = client.query_batch_points(COLLECTION, requests=requests)
        for key, result in zip(keys[start:end], results):
            for hit in result.points:
                other = hit.payload["product_key"]
                a, b = (key, other) if key < other else (other, key)
                rows.append((a, b, hit.score))
    client.close()

    # 4) A tìm thấy B và B tìm thấy A -> 2 dòng cho cùng một cặp; giữ dòng điểm cao nhất
    pairs = pd.DataFrame(rows, columns=["key_a", "key_b", "score"])
    return (pairs.sort_values("score", ascending=False)
            .drop_duplicates(["key_a", "key_b"])
            .reset_index(drop=True))


def apply_rules(pairs: pd.DataFrame, products: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """(E) Thêm price_ratio và một cột cho mỗi luật (ok_score ... ok_price) cùng cột matched."""
    info = products.set_index("product_key")[
        ["brand", "storage_gb", "ram_gb", "variants", "model_nums", "chip", "chip_tier", "price_median"]].to_dict("index")
    rows = []
    for a, b, score in pairs[["key_a", "key_b", "score"]].itertuples(index=False):
        pa, pb = info[a], info[b]
        checks = er.check_pair(pa, pb, float(score), threshold)
        rows.append({"price_ratio": er.price_ratio(pa["price_median"], pb["price_median"]), **checks})
    return pd.concat([pairs.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def cluster(products: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    """(E) Gom cụm các cặp matched (tối đa 1 sản phẩm / sàn / cụm), thêm master_id, cluster_size, match_score."""
    platform_of = dict(zip(products["product_key"], products["platform"]))
    matched = pairs[pairs["matched"]]
    matches = [er.Match(a, b, float(s), None if pd.isna(r) else float(r))
               for a, b, s, r in matched[["key_a", "key_b", "score", "price_ratio"]].itertuples(index=False)]
    attrs_of = {r["product_key"]: er.cluster_attrs(r)
                for r in products[["product_key", *er.CLUSTER_ATTRS]].to_dict("records")}
    clusters = er.cluster(platform_of, matches, attrs_of)
    out = products.copy()
    out["master_id"] = [clusters[k]["master_id"] for k in out["product_key"]]
    out["cluster_size"] = [clusters[k]["cluster_size"] for k in out["product_key"]]
    out["match_score"] = [clusters[k]["match_score"] for k in out["product_key"]]
    return out


def _int_or_none(v):
    return None if pd.isna(v) else int(v)


def write_outputs(spark: SparkSession, master: pd.DataFrame, pairs: pd.DataFrame, gold_root: str) -> None:
    """(F) Ghi product_master và er_candidates (Delta, ghi đè trọn bảng: chạy lại ra cùng kết quả)."""
    master_rows = [
        (r.product_key, r.platform, r.master_id, int(r.cluster_size),
         None if r.match_score is None or pd.isna(r.match_score) else float(r.match_score),
         r.name_rep, r.name_clean, _none_if_na(r.brand), r.category, _int_or_none(r.ram_gb),
         _int_or_none(r.storage_gb), sorted(r.variants), sorted(r.model_nums),
         None if pd.isna(r.price_median) else float(r.price_median), int(r.n_obs))
        for r in master.itertuples(index=False)
    ]
    df = spark.createDataFrame(master_rows, MASTER_SCHEMA).withColumn("computed_at", F.current_timestamp())
    (df.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .save(f"{gold_root}/product_master"))

    cluster_of = dict(zip(master["product_key"], master["master_id"]))
    keep = pairs[pairs["score"] >= CANDIDATE_MIN_SCORE]
    cand_rows = [
        (r.key_a, r.key_b, float(r.score), None if pd.isna(r.price_ratio) else float(r.price_ratio),
         *[bool(getattr(r, c)) for c in (*er.CHECKS, "matched")],
         cluster_of[r.key_a] == cluster_of[r.key_b])
        for r in keep.itertuples(index=False)
    ]
    (spark.createDataFrame(cand_rows, CANDIDATE_SCHEMA).withColumn("computed_at", F.current_timestamp())
     .coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .save(f"{gold_root}/er_candidates"))
    log.info("Wrote %d products -> %s/product_master, %d candidate pairs (score >= %.2f) -> %s/er_candidates",
             len(master_rows), gold_root, len(cand_rows), CANDIDATE_MIN_SCORE, gold_root)


def report(master: pd.DataFrame, pairs: pd.DataFrame) -> None:
    """Tỉ lệ sản phẩm khớp với ít nhất một sàn khác (tiêu chí tuần 3), theo sàn và theo loại."""
    m = master.assign(matched=master["cluster_size"] >= 2)
    log.info("Matched with >= 1 other platform: %d / %d products (%.1f%%)",
             m["matched"].sum(), len(m), 100 * m["matched"].mean())
    for (name, col) in (("platform", "platform"), ("category", "category")):
        rate = m.groupby(col)["matched"].agg(["sum", "count", "mean"])
        for k, r in rate.iterrows():
            log.info("  by %s %-11s %5d / %5d  %5.1f%%", name, k, r["sum"], r["count"], 100 * r["mean"])
    sizes = m.drop_duplicates("master_id")["cluster_size"].value_counts().sort_index()
    log.info("Clusters by size: %s", ", ".join(f"{k} sàn: {v}" for k, v in sizes.items()))
    fails = {c: int((~pairs.loc[pairs["ok_score"], c]).sum()) for c in er.CHECKS if c != "ok_score"}
    log.info("Pairs above threshold: %d, matched: %d; rejected by rule (above threshold): %s",
             int(pairs["ok_score"].sum()), int(pairs["matched"].sum()), fails)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--silver-root", default="/datalake/silver")
    ap.add_argument("--gold-root", default="/datalake/gold")
    ap.add_argument("--qdrant-path", help="Qdrant cục bộ (chậm, chỉ để thử)")
    ap.add_argument("--qdrant-url", help="Qdrant server, vd http://localhost:6333 hoặc http://qdrant:6333")
    ap.add_argument("--threshold", type=float, default=0.85)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--model-cache", type=Path, default=Path("datalake_local/models"),
                    help="thư mục lưu model embedding (tải 1 lần ~240 MB)")
    args = ap.parse_args(argv)
    if not (args.qdrant_url or args.qdrant_path):
        ap.error("cần --qdrant-url (server) hoặc --qdrant-path (cục bộ)")
    silver_root = str(Path(args.silver_root).expanduser()) if args.silver_root.startswith("~") else args.silver_root

    spark = SparkSession.builder.appName("entity_resolution").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    products = add_features(build_products(spark, silver_root))                      # A, B
    log.info("%d products", len(products))
    texts = [embed_text(_none_if_na(b), c) for b, c in zip(products["brand"], products["name_clean"])]
    vectors = embed(texts, args.model_cache.expanduser())                             # C
    pairs = find_candidates(products, vectors, args.qdrant_path, args.qdrant_url, args.top_k)   # D
    log.info("%d candidate pairs from Qdrant", len(pairs))
    pairs = apply_rules(pairs, products, args.threshold)                              # E
    master = cluster(products, pairs)
    write_outputs(spark, master, pairs, args.gold_root)                               # F
    report(master, pairs)
    spark.stop()


if __name__ == "__main__":
    main()
