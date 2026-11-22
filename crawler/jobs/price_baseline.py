"""price_baseline: giá tham chiếu của từng SKU, dùng cho luật bất thường của speed layer (tuần 3, bước 3).

Đơn vị: (product_key, sku_id). sku_id null (cellphones) ghi thành "" để speed layer join được bằng `=`
(NULL = NULL là false trong Spark). Speed layer làm giống vậy với sự kiện trong stream.

Cách tính (docs/gold_schema.md mục price_baseline):
  1. Silver price, bỏ giá null (MISSING_PRICE/ZERO_PRICE), lấy cửa sổ 30 ngày UTC kết thúc ở --as-of.
     Silver chưa đủ 30 ngày thì dùng hết những ngày đang có (kế hoạch tuần 3: tối thiểu 7 ngày).
  2. Gom về TRUNG VỊ THEO NGÀY trước: SKU crawl 30-60 phút/lần và SKU chỉ có lượt đêm có cùng trọng số,
     và ngày máy tắt chỉ làm giảm n_days, không bị coi là "giá không đổi".
  3. Trên các giá trị ngày: median_price, mad (median |x - median|), min/max, n_days; tương tự cho
     price_original (luật khuyến mãi ảo).
  4. SKU chưa đủ --min-days ngày nhưng cả product_key đủ -> dùng baseline của product_key (level=product).
     Không đủ cả hai -> status=insufficient (speed layer không chấm luật 1, 3).

Ghi đè trọn bảng mỗi lần chạy (Delta overwrite, atomic): chạy lại cùng --as-of ra cùng kết quả.

Chạy:
    bash spark/submit.sh jobs/price_baseline.py                          # cụm, Silver/Gold trên HDFS
    bash spark/run_local.sh jobs/price_baseline.py \
        --silver-root ~/data/silver_20261116 --gold-root datalake_local/gold
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("price_baseline")

KEY = ["product_key", "sku_id"]
HOT_MIN_OBS_PER_DAY = 6        # nhóm theo dõi sát: crawl 30-60 phút/lần (docs/gold_step0_findings.md)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute Gold price_baseline from Silver price")
    p.add_argument("--silver-root", default="/datalake/silver")
    p.add_argument("--gold-root", default="/datalake/gold")
    p.add_argument("--as-of", type=date.fromisoformat,
                   help="ngày UTC cuối cùng của cửa sổ (mặc định: ngày mới nhất có trong Silver)")
    p.add_argument("--window-days", type=int, default=30)
    p.add_argument("--min-days", type=int, default=7)
    return p.parse_args(argv)


def daily_medians(prices: DataFrame, key: list[str]) -> DataFrame:
    """Một dòng / (key, ngày): trung vị giá bán, trung vị giá gốc, số quan sát."""
    return prices.groupBy(*key, "date").agg(
        F.median("price_current").alias("day_price"),
        F.median("price_original").alias("day_original"),
        F.count("*").alias("day_obs"),
    )


def summarize(daily: DataFrame, key: list[str]) -> DataFrame:
    """Gom các giá trị ngày thành baseline: median, MAD, min/max, số ngày (bán và gốc)."""
    stats = daily.groupBy(*key).agg(
        F.median("day_price").alias("median_price"),
        F.min("day_price").alias("min_price"),
        F.max("day_price").alias("max_price"),
        F.count("*").alias("n_days"),
        F.sum("day_obs").alias("n_obs"),
        F.median("day_original").alias("median_price_original"),
        F.count("day_original").alias("n_days_original"),        # count bỏ qua null
        F.min("date").alias("first_day"),
        F.max("date").alias("last_day"),
    )
    mad = (daily.join(stats.select(*key, "median_price"), key)
           .groupBy(*key)
           .agg(F.median(F.abs(F.col("day_price") - F.col("median_price"))).alias("mad")))
    return stats.join(mad, key)


def build_baseline(silver_price: DataFrame, as_of: date, window_days: int, min_days: int) -> DataFrame:
    window_start = as_of - timedelta(days=window_days - 1)
    prices = (silver_price
              .where(F.col("price_current").isNotNull())
              .where(F.col("date").between(F.lit(window_start), F.lit(as_of)))
              .select("product_key", "platform", F.coalesce("sku_id", F.lit("")).alias("sku_id"),
                      "price_current", "price_original", "date"))

    sku = summarize(daily_medians(prices, KEY), KEY)
    product = summarize(daily_medians(prices, ["product_key"]), ["product_key"])
    platform = prices.select("product_key", "platform").distinct()

    value_cols = ["median_price", "mad", "min_price", "max_price", "n_days", "n_obs",
                  "median_price_original", "n_days_original", "first_day", "last_day"]
    p = product.select("product_key", *[F.col(c).alias(f"p_{c}") for c in value_cols])

    joined = sku.join(p, "product_key", "left")
    use_product = (F.col("n_days") < min_days) & (F.col("p_n_days") >= min_days)
    picked = [F.when(use_product, F.col(f"p_{c}")).otherwise(F.col(c)).alias(c) for c in value_cols]

    out = (joined.select(*KEY, F.when(use_product, F.lit("product")).otherwise(F.lit("sku")).alias("level"),
                         *picked)
           .join(platform, "product_key")
           .withColumn("status", F.when(F.col("n_days") >= min_days, "ok").otherwise("insufficient"))
           .withColumn("mad", F.col("mad").cast("double"))
           .withColumn("median_price", F.col("median_price").cast("double"))
           .withColumn("median_price_original", F.col("median_price_original").cast("double"))
           .withColumn("window_start", F.lit(window_start))
           .withColumn("window_end", F.lit(as_of))
           .withColumn("computed_at", F.current_timestamp()))
    return out.select(
        "product_key", "sku_id", "platform", "level", "status",
        "median_price", "mad", "min_price", "max_price", "n_days", "n_obs",
        "median_price_original", "n_days_original",
        "first_day", "last_day", "window_start", "window_end", "computed_at",
    )


def report(spark: SparkSession, silver_price: DataFrame, baseline: DataFrame, as_of: date) -> None:
    """In số liệu kiểm tra, gồm tiêu chí 'mọi sản phẩm theo dõi sát đều có baseline'."""
    by = baseline.groupBy("platform", "level", "status").count().orderBy("platform", "level", "status")
    for r in by.collect():
        log.info("  %-10s level=%-7s status=%-12s %6d", r["platform"], r["level"], r["status"], r["count"])

    # Nhóm theo dõi sát: SKU có >= HOT_MIN_OBS_PER_DAY quan sát/ngày trong 7 ngày cuối
    last7 = (silver_price.where(F.col("price_current").isNotNull())
             .where(F.col("date").between(F.lit(as_of - timedelta(days=6)), F.lit(as_of)))
             .select("product_key", F.coalesce("sku_id", F.lit("")).alias("sku_id"), "date"))
    hot = (last7.groupBy(*KEY).agg((F.count("*") / F.countDistinct("date")).alias("obs_per_day"))
           .where(F.col("obs_per_day") >= HOT_MIN_OBS_PER_DAY))
    hot_total = hot.count()
    missing = hot.join(baseline.where(F.col("status") == "ok").select(*KEY), KEY, "left_anti")
    n_missing = missing.count()
    log.info("Hot SKUs (>= %d obs/day, last 7 days): %d, without an ok baseline: %d",
             HOT_MIN_OBS_PER_DAY, hot_total, n_missing)
    if n_missing:
        first_seen = (silver_price.groupBy("product_key").agg(F.min("date").alias("first_seen")))
        for r in (missing.join(first_seen, "product_key").groupBy("first_seen").count()
                  .orderBy("first_seen").collect()):
            log.info("  hot SKU without baseline, product first seen %s: %d", r["first_seen"], r["count"])


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.appName("price_baseline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_price = spark.read.format("delta").load(f"{args.silver_root}/price")
    as_of = args.as_of or silver_price.agg(F.max("date")).first()[0]
    first_day = silver_price.agg(F.min("date")).first()[0]
    window_start = max(first_day, as_of - timedelta(days=args.window_days - 1))
    log.info("Silver %s .. %s; window %s .. %s (%d days)", first_day, as_of, window_start, as_of,
             (as_of - window_start).days + 1)

    baseline = build_baseline(silver_price, as_of, args.window_days, args.min_days).cache()
    out_path = f"{args.gold_root}/price_baseline"
    (baseline.coalesce(1).write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(out_path))
    log.info("Wrote %d rows -> %s", spark.read.format("delta").load(out_path).count(), out_path)
    report(spark, silver_price, baseline, as_of)
    baseline.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
