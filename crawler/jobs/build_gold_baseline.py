import argparse
import logging
import re
import unicodedata

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger("build_gold_baseline")

BRANDS = (
    "apple", "iphone", "ipad", "macbook", "airpods", "samsung", "galaxy",
    "xiaomi", "redmi", "poco", "oppo", "vivo", "realme", "asus", "rog", "tuf",
    "acer", "nitro", "aspire", "lenovo", "thinkpad", "ideapad", "legion", "yoga",
    "dell", "inspiron", "xps", "latitude", "hp", "pavilion", "victus", "envy",
    "msi", "huawei", "honor", "nokia", "sony", "jbl", "anker", "baseus", "ugreen",
    "garmin", "amazfit", "lg", "casio", "canon", "fujifilm", "logitech", "razer",
)

BRAND_NORM = {
    "iphone": "apple",
    "ipad": "apple",
    "macbook": "apple",
    "airpods": "apple",
    "galaxy": "samsung",
    "redmi": "xiaomi",
    "poco": "xiaomi",
    "rog": "asus",
    "tuf": "asus",
    "nitro": "acer",
    "aspire": "acer",
    "thinkpad": "lenovo",
    "ideapad": "lenovo",
    "legion": "lenovo",
    "yoga": "lenovo",
    "inspiron": "dell",
    "xps": "dell",
    "latitude": "dell",
    "pavilion": "hp",
    "victus": "hp",
    "envy": "hp",
}

STOPWORDS = {
    "chinh", "hang", "vn", "vna", "freeship", "gia", "re", "moi", "fullbox",
    "bh", "bao", "hanh", "thang", "nam", "tra", "gop", "dien", "thoai",
    "may", "tinh", "xach", "tay", "bang", "laptop", "smart", "watch", "dong",
    "ho", "thong", "minh", "tai", "nghe", "pin", "sac", "du", "phong", "cap",
    "cu", "likenew", "box", "seal", "nhap", "khau", "quoc", "te", "ban",
    "ram", "rom", "ssd", "hdd", "inch", "oled", "amoled", "5g", "4g", "lte",
    "wifi", "gb", "tb",
}

PROMO_RE = re.compile(r"\([^)]*\)|\[[^\]]*\]")
STORAGE_RE = re.compile(r"\b(\d{2,4})\s*(gb|tb)\b")
TOKEN_RE = re.compile(r"[a-z0-9]+")


def remove_accents(text):
    text = text.replace("đ", "d").replace("Đ", "D")
    norm = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in norm if unicodedata.category(ch) != "Mn")


def extract_entity_fields(name_raw, brand_raw):
    """Chuẩn hóa tên sản phẩm để lấy (name_clean, brand, storage, canonical_id)."""
    if not name_raw:
        return ("unknown", "unknown", "", "unknown")

    s = remove_accents(name_raw.lower())
    s = PROMO_RE.sub(" ", s)

    # Tách dung lượng (ví dụ 128gb, 256gb, 1tb) rồi xóa khỏi chuỗi để tránh lặp trong model_slug
    storages = []
    for num_str, unit in STORAGE_RE.findall(s):
        val = int(num_str)
        if unit == "tb" and val <= 8:
            storages.append(f"{val}tb")
        elif unit == "gb" and val in (16, 32, 64, 128, 256, 512, 1024):
            storages.append("1tb" if val == 1024 else f"{val}gb")
    storage = storages[-1] if storages else ""
    s_no_storage = STORAGE_RE.sub(" ", s)

    tokens = TOKEN_RE.findall(s_no_storage)
    brand = ""
    if brand_raw:
        b_tokens = TOKEN_RE.findall(remove_accents(brand_raw.lower()))
        if b_tokens:
            b0 = b_tokens[0]
            brand = BRAND_NORM.get(b0, b0)

    if not brand:
        for t in tokens:
            if t in BRANDS:
                brand = BRAND_NORM.get(t, t)
                break
    if not brand:
        brand = tokens[0] if tokens else "unknown"

    clean_tokens = [t for t in tokens if t not in STOPWORDS]
    name_clean = " ".join(clean_tokens[:8]) if clean_tokens else s.strip()

    model_tokens = [t for t in clean_tokens if t != brand][:5]
    model_slug = "-".join(model_tokens) if model_tokens else "item"
    canonical_id = f"{brand}:{model_slug}:{storage}" if storage else f"{brand}:{model_slug}"

    return (name_clean, brand, storage, canonical_id)


def build_product_master(spark, silver_root, gold_root):
    price_df = spark.read.format("delta").load(f"{silver_root}/price")
    w_price = Window.partitionBy("product_key").orderBy(F.col("event_time").desc())

    distinct_products = (
        price_df.select("product_key", "platform", "product_id", "name_raw", "url", "event_time")
        .withColumn("rn", F.row_number().over(w_price))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    try:
        prod_df = spark.read.format("delta").load(f"{silver_root}/product")
        w_prod = Window.partitionBy("product_key").orderBy(F.col("event_time").desc())
        prod_meta = (
            prod_df.select("product_key", "brand_raw", "category_raw")
            .withColumn("rn", F.row_number().over(w_prod))
            .where(F.col("rn") == 1)
            .drop("rn")
        )
        joined = distinct_products.join(prod_meta, on="product_key", how="left")
    except Exception as exc:
        log.warning("Could not read silver/product, skipping brand_raw/category_raw: %s", exc)
        joined = (
            distinct_products
            .withColumn("brand_raw", F.lit(None).cast("string"))
            .withColumn("category_raw", F.lit(None).cast("string"))
        )

    schema = T.StructType([
        T.StructField("name_clean", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("storage", T.StringType()),
        T.StructField("canonical_id", T.StringType()),
    ])
    extract_udf = F.udf(extract_entity_fields, schema)

    master_df = (
        joined.withColumn("ent", extract_udf(F.col("name_raw"), F.col("brand_raw")))
        .select(
            "product_key",
            "platform",
            "product_id",
            "name_raw",
            F.col("ent.name_clean").alias("name_clean"),
            F.col("ent.brand").alias("brand"),
            F.col("ent.storage").alias("storage"),
            F.col("ent.canonical_id").alias("canonical_id"),
            "category_raw",
            "url",
            F.current_timestamp().alias("updated_at"),
        )
    )

    out_path = f"{gold_root}/product_master"
    master_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

    count = spark.read.format("delta").load(out_path).count()
    log.info("Built product_master at %s (%d products)", out_path, count)
    return count


def build_price_baseline(spark, silver_root, gold_root):
    price_df = (
        spark.read.format("delta")
        .load(f"{silver_root}/price")
        .where(F.col("price_current").isNotNull() & (F.col("price_current") > 0))
    )

    medians = price_df.groupBy("product_key", "platform", "product_id").agg(
        F.percentile_approx("price_current", 0.5).cast("double").alias("median_price"),
        F.avg("price_current").cast("double").alias("mean_price"),
        F.stddev_pop("price_current").cast("double").alias("std_price"),
        F.min("price_current").alias("min_price"),
        F.max("price_current").alias("max_price"),
        F.countDistinct("date").cast("int").alias("days_count"),
        F.count("*").alias("obs_count"),
    )

    # Tính MAD = median(|price_current - median_price|)
    with_dev = (
        price_df.select("product_key", "price_current")
        .join(medians.select("product_key", "median_price"), on="product_key", how="inner")
        .withColumn("abs_dev", F.abs(F.col("price_current") - F.col("median_price")))
    )

    mads = with_dev.groupBy("product_key").agg(
        F.percentile_approx("abs_dev", 0.5).cast("double").alias("mad_price")
    )

    baseline_df = (
        medians.join(mads, on="product_key", how="left")
        .withColumn("mad_price", F.coalesce(F.col("mad_price"), F.lit(0.0)))
        .withColumn("updated_at", F.current_timestamp())
    )

    out_path = f"{gold_root}/price_baseline"
    baseline_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

    count = spark.read.format("delta").load(out_path).count()
    log.info("Built price_baseline at %s (%d products)", out_path, count)
    return count


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Build Gold product_master and price_baseline")
    parser.add_argument("--silver-root", default="/datalake/silver")
    parser.add_argument("--gold-root", default="/datalake/gold")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("build_gold_baseline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    build_product_master(spark, args.silver_root, args.gold_root)
    build_price_baseline(spark, args.silver_root, args.gold_root)
    spark.stop()


if __name__ == "__main__":
    main()