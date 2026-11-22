"""entity_resolution: ghép cùng sản phẩm giữa các sàn, ghi Gold product_master (tuần 3, Bước 1).

Luồng (xem hướng dẫn Bước 1, mục 1.0):
    (A) Silver -> 1 dòng / product_key
    (B) chuẩn hoá bằng common/product_text.py
    (C) embedding tên
    (D) Qdrant: top-k láng giềng ở sàn khác, cùng loại
    (E) luật cứng + gom cụm (union-find, tối đa 1 sản phẩm / sàn / cụm) -> master_id
    (F) ghi gold/product_master + er_candidates, in thống kê

Chạy thử trên máy (trong crawler/, đã kích hoạt .venv):
    python -m jobs.entity_resolution \
        --silver-root ~/Documents/data_porject_Big_data_Team6/silver_20261116 \
        --gold-root datalake_local/gold --qdrant-path datalake_local/qdrant
"""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

from common import product_text as pt

import re

import numpy as np
from fastembed import TextEmbedding

import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PayloadSchemaType,
    PointStruct,
    QueryRequest,
    VectorParams,
)

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


def build_products(silver_root: Path) -> pd.DataFrame:
    """(A) Mục 1.2. Trả 1 dòng / product_key: product_key, platform, name_rep (bỏ "FLASH SALE"),
    price_median (7 ngày gần nhất), n_obs, brand_raw, category_raw, specs_raw (bản mới nhất)."""
    # 1) Đọc Silver price, chỉ những cột cần dùng. Chuỗi -> category cho đỡ RAM, rồi đổi lại thành str.
    price = (
        DeltaTable(str(silver_root / "price"))
        .to_pyarrow_table(columns=["product_key", "platform", "name_raw", "price_current", "date"])
        .to_pandas(strings_to_categorical=True)
    )
    for col in ("product_key", "platform", "name_raw"):
        price[col] = price[col].astype(str)

    # 2) Sàn và số lần quan sát của mỗi product_key
    stats = price.groupby("product_key").agg(
        platform=("platform", "first"),
        n_obs=("name_raw", "size"),
    )

    # 3) Tên đại diện = tên xuất hiện nhiều nhất, bỏ tên bị nhãn "FLASH SALE" đè (Bước 0)
    names = price[price["name_raw"].str.strip().str.upper() != "FLASH SALE"]
    name_counts = names.groupby(["product_key", "name_raw"]).size().reset_index(name="n")
    name_rep = (
        name_counts.sort_values(["product_key", "n"], ascending=[True, False])
        .drop_duplicates("product_key")  # giữ dòng ĐẦU của mỗi key = tên nhiều nhất
        .set_index("product_key")["name_raw"]
        .rename("name_rep")
    )

    # 4) Giá tiêu biểu: trung vị 7 ngày gần nhất; key không có giá trong 7 ngày đó thì lấy trung vị toàn bộ
    priced = price.dropna(subset=["price_current"])
    last_day = priced["date"].max()
    recent = priced[priced["date"] > last_day - timedelta(days=PRICE_WINDOW_DAYS)]
    med_recent = recent.groupby("product_key")["price_current"].median()
    med_all = priced.groupby("product_key")["price_current"].median()
    price_median = med_recent.combine_first(med_all).rename("price_median")

    products = stats.join(name_rep).join(price_median).reset_index()

    # 5) Thông tin từ silver_product: bản MỚI NHẤT của mỗi key (bảng giữ mọi lần quan sát)
    product = DeltaTable(str(silver_root / "product")).to_pandas(
        columns=["product_key", "brand_raw", "category_raw", "specs_raw", "event_time"]
    )
    latest = (
        product.sort_values("event_time")
        .drop_duplicates("product_key", keep="last")
        .drop(columns="event_time")
    )
    # left join: 46% key của tiki không có trong silver_product, vẫn phải giữ lại
    products = products.merge(latest, on="product_key", how="left")

    # Key chỉ có tên "FLASH SALE" thì không có tên đại diện -> bỏ
    return products.dropna(subset=["name_rep"])

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
    """(E) Mục 1.5. Thêm cột ok_score, ok_brand, ok_storage, ok_variant, ok_price, matched."""
    raise NotImplementedError


def cluster(products: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    """(E) Mục 1.5. Union-find theo score giảm dần, chỉ gộp khi tập sàn không giao nhau.
    Thêm master_id (sha1 product_key nhỏ nhất trong cụm, 16 ký tự), match_score, cluster_size."""
    raise NotImplementedError


def write_outputs(master: pd.DataFrame, pairs: pd.DataFrame, gold_root: Path) -> None:
    """(F) Mục 1.6. Ghi gold_root/product_master (Delta, overwrite) và gold_root/er_candidates.parquet
    (mọi cặp score >= 0.6). In tỉ lệ khớp theo sàn, theo category, phân bố cluster_size."""
    raise NotImplementedError


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--silver-root", required=True, type=Path)
    ap.add_argument("--gold-root", required=True, type=Path)
    ap.add_argument("--qdrant-path", help="Qdrant cục bộ (chạy thử trên máy)")
    ap.add_argument("--qdrant-url", help="Qdrant server, vd http://qdrant:6333 (máy chính)")
    ap.add_argument("--threshold", type=float, default=0.85)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--model-cache", type=Path, default=Path("datalake_local/models"),
                    help="thư mục lưu model embedding (tải 1 lần ~240 MB)")
    args = ap.parse_args()

    # TODO: nối các khâu A -> F theo thứ tự ở docstring đầu file
    #raise NotImplementedError
    products = build_products(args.silver_root.expanduser())
    products = add_features(products)

    # --- Kiểm tra khâu A (tạm thời; sẽ thay bằng các khâu C-F) ---
    print(f"{len(products):,} sản phẩm")
    print(pd.crosstab(products["platform"], products["category"]).to_string())
    devices = products[products["category"].isin(["phone", "tablet", "laptop"])]
    print("Thiết bị có storage_gb:",
          devices.groupby("platform")["storage_gb"].apply(lambda s: f"{s.notna().mean():.0%}").to_dict())
    cols = ["platform", "name_rep", "name_clean", "brand", "ram_gb", "storage_gb", "variants", "model_nums",
            "price_median"]
    print(products[products["category"] == "phone"].sample(12, random_state=1)[cols].to_string(index=False))

    # --- Khâu C ---
    texts = [embed_text(_none_if_na(b), c) for b, c in zip(products["brand"], products["name_clean"])]
    products["embed_text"] = texts
    vectors = embed(texts, args.model_cache.expanduser())
    print("vectors:", vectors.shape, vectors.dtype)

    # Kiểm tra: 8 sản phẩm gần nhất (sàn khác) của một chiếc iPhone 15 ở cellphones
    i = products.index.get_loc(
        products[(products["platform"] == "cellphones")
                 & products["name_rep"].str.startswith("iPhone 15 128GB")].index[0]
    )
    scores = vectors @ vectors[i]  # cosine với TẤT CẢ sản phẩm, vì đã chuẩn hoá
    other = (products["platform"] != "cellphones").to_numpy()
    top = np.argsort(-np.where(other, scores, -1))[:8]
    print(f"\nGần nhất với: {products.iloc[i]['name_rep']}")
    for j in top:
        r = products.iloc[j]
        print(f"  {scores[j]:.3f}  {r['platform']:<9} {r['category']:<9} {r['name_rep'][:70]}")

    # --- Khâu D ---
    if not (args.qdrant_url or args.qdrant_path):
        ap.error("cần --qdrant-url (server) hoặc --qdrant-path (cục bộ)")
    pairs = find_candidates(products, vectors, args.qdrant_path, args.qdrant_url, args.top_k)

    print(f"\n{len(pairs):,} cặp ứng viên")
    print(pairs["score"].describe(percentiles=[0.1, 0.5, 0.9]).round(3).to_string())
    info = products.set_index("product_key")[["platform", "category", "name_rep"]]
    view = (pairs.join(info, on="key_a").join(info, on="key_b", lsuffix="_a", rsuffix="_b"))
    print("Lọt bộ lọc (phải = 0): cùng sàn", (view["platform_a"] == view["platform_b"]).sum(),
          "| khác loại", (view["category_a"] != view["category_b"]).sum())
    print(view[view["category_a"] == "phone"].head(15)[
        ["score", "platform_a", "name_rep_a", "platform_b", "name_rep_b"]].to_string(index=False))

if __name__ == "__main__":
    main()
