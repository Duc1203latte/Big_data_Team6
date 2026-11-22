"""daily_stats: thống kê giá theo ngày và batch view trong ClickHouse (tuần 3, bước 4-5).

Với mỗi ngày UTC trong khoảng [--date-from, --date-to] (mặc định: mọi ngày có trong Silver):
  1. SKU-ngày: min/max/trung bình/trung vị/giá cuối, giá gốc, % giảm, số quan sát (từ Silver price,
     gắn master_id, category, brand từ Gold product_master).
  2. Gold daily_price_stats: theo ngày x sàn x danh mục x thương hiệu, tính trên giá trung vị ngày của
     từng SKU (SKU crawl dày không nặng ký hơn SKU crawl thưa).
  3. Gold daily_price_spread: chênh giá cùng sản phẩm (cùng master_id) giữa các sàn.
  4. ClickHouse: price_batch_daily, daily_price_stats, daily_price_spread (xoá partition của ngày rồi ghi lại),
     rồi cập nhật serving_cutoff = 00:00 UTC của ngày sau ngày batch mới nhất.
  5. (tuỳ chọn --redis-url) nạp sẵn Redis price:last:{master_id} từ giá mới nhất của mỗi sàn.

Mọi bước ghi lại trọn từng ngày (Delta replaceWhere, ClickHouse DROP PARTITION) nên chạy lại cùng ngày
không sinh dòng trùng.

Chạy:
    bash spark/submit.sh jobs/daily_stats.py --date 2026-11-15 --clickhouse-url http://clickhouse:8123
    bash spark/run_local.sh jobs/daily_stats.py --silver-root ~/data/silver_20261116 \
        --gold-root datalake_local/gold --clickhouse-url http://localhost:8123
Tài khoản ClickHouse lấy từ biến môi trường CLICKHOUSE_USER, CLICKHOUSE_PASSWORD (submit.sh tự truyền).
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("daily_stats")

BATCH_COLS = ["date", "platform", "product_key", "sku_id", "master_id", "category", "brand", "name",
              "price_min", "price_max", "price_avg", "price_median", "price_last", "price_original_median",
              "discount_pct_avg", "n_obs", "first_event_time", "last_event_time", "updated_at"]
STATS_COLS = ["date", "platform", "category", "brand", "n_skus", "n_obs", "price_avg", "price_median",
              "price_std", "updated_at"]
SPREAD_COLS = ["date", "master_id", "name", "category", "brand", "n_platforms", "price_min", "price_max",
               "spread_pct", "cheapest_platform", "priciest_platform", "updated_at"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Daily price statistics and ClickHouse batch view")
    p.add_argument("--silver-root", default="/datalake/silver")
    p.add_argument("--gold-root", default="/datalake/gold")
    p.add_argument("--date", type=date.fromisoformat)
    p.add_argument("--date-from", type=date.fromisoformat)
    p.add_argument("--date-to", type=date.fromisoformat)
    p.add_argument("--clickhouse-url", help="vd http://clickhouse:8123; bỏ trống thì không ghi ClickHouse")
    p.add_argument("--redis-url", help="vd redis://redis:6379/0; có thì nạp sẵn price:last")
    args = p.parse_args(argv)
    if args.date:
        args.date_from = args.date_to = args.date
    return args


def sku_days(silver_price: DataFrame, master: DataFrame) -> DataFrame:
    """Một dòng / (ngày, product_key, sku_id) có giá."""
    priced = (silver_price.where(F.col("price_current").isNotNull())
              .withColumn("sku_id", F.coalesce("sku_id", F.lit(""))))
    day = priced.groupBy("date", "platform", "product_key", "sku_id").agg(
        F.min("price_current").alias("price_min"),
        F.max("price_current").alias("price_max"),
        F.avg("price_current").alias("price_avg"),
        F.median("price_current").cast("double").alias("price_median"),
        F.max_by("price_current", "event_time").alias("price_last"),
        F.median("price_original").cast("double").alias("price_original_median"),
        F.avg("discount_pct").alias("discount_pct_avg"),
        F.count("*").cast("int").alias("n_obs"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
        F.max_by("name_raw", "event_time").alias("name_last"),
    )
    m = master.select("product_key", "master_id", "category", "brand", "name_rep")
    return (day.join(m, "product_key", "left")
            .withColumn("category", F.coalesce("category", F.lit("other")))
            .withColumn("name", F.coalesce("name_rep", "name_last"))
            .drop("name_rep", "name_last")
            .withColumn("updated_at", F.current_timestamp()))


def stats(batch: DataFrame) -> DataFrame:
    return (batch.withColumn("brand", F.coalesce("brand", F.lit("unknown")))
            .groupBy("date", "platform", "category", "brand").agg(
                F.count("*").cast("int").alias("n_skus"),
                F.sum("n_obs").alias("n_obs"),
                F.avg("price_median").alias("price_avg"),
                F.median("price_median").cast("double").alias("price_median"),
                F.stddev_samp("price_median").alias("price_std"))
            .withColumn("updated_at", F.current_timestamp()))


def spread(batch: DataFrame, master: DataFrame) -> DataFrame:
    """Giá của một sản phẩm trên một sàn trong ngày = trung vị giá trung vị các SKU của nó."""
    clustered = master.where(F.col("cluster_size") >= 2).select("product_key", "name_rep")
    product_day = (batch.join(clustered, "product_key")
                   .groupBy("date", "master_id", "platform", "product_key")
                   .agg(F.median("price_median").alias("price"), F.first("category").alias("category"),
                        F.first("brand").alias("brand"), F.first("name_rep").alias("name")))
    agg = product_day.groupBy("date", "master_id").agg(
        F.count("*").cast("int").alias("n_platforms"),
        F.min("price").alias("price_min"), F.max("price").alias("price_max"),
        F.min_by("platform", "price").alias("cheapest_platform"),
        F.max_by("platform", "price").alias("priciest_platform"),
        F.min_by("name", "platform").alias("name"),
        F.first("category").alias("category"), F.first("brand").alias("brand"))
    return (agg.where(F.col("n_platforms") >= 2)
            .withColumn("spread_pct", (F.col("price_max") - F.col("price_min")) / F.col("price_min") * 100)
            .withColumn("updated_at", F.current_timestamp()))


def write_delta_days(df: DataFrame, path: str, d_from: date, d_to: date) -> None:
    """Ghi đè đúng các ngày trong khoảng, giữ nguyên ngày khác."""
    (df.write.format("delta").mode("overwrite").partitionBy("date")
     .option("replaceWhere", f"date >= '{d_from}' AND date <= '{d_to}'")
     .save(path))


def _utc(micros: int | None) -> datetime | None:
    return None if micros is None else datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)


def collect_utc(df: DataFrame, cols: list[str]) -> list[tuple]:
    """collect() với timestamp đổi sang datetime UTC có múi giờ.

    collect() trả timestamp dạng naive theo GIỜ ĐỊA PHƯƠNG của tiến trình Python (máy VN: +7), không phải
    theo spark.sql.session.timeZone, nên đi qua số micro giây kể từ epoch để không lệch.
    """
    ts_cols = {f.name for f in df.schema.fields if f.dataType.typeName() == "timestamp"}
    sel = [F.unix_micros(c).alias(c) if c in ts_cols else F.col(c) for c in cols]
    return [tuple(_utc(r[c]) if c in ts_cols else r[c] for c in cols) for r in df.select(*sel).collect()]


def to_clickhouse(client, table: str, df: DataFrame, cols: list[str], days: list[date]) -> int:
    total = 0
    for d in days:
        client.command(f"ALTER TABLE bigdata.{table} DROP PARTITION '{d.isoformat()}'")
        rows = collect_utc(df.where(F.col("date") == F.lit(d)), cols)
        if rows:
            client.insert(f"bigdata.{table}", rows, column_names=cols)
        total += len(rows)
    return total


def update_cutoff(client) -> None:
    last = client.query("SELECT max(date) FROM bigdata.price_batch_daily").result_rows[0][0]
    cutoff = datetime.combine(last + timedelta(days=1), time(0), tzinfo=timezone.utc)
    client.insert("bigdata.serving_cutoff", [("price", cutoff, last, datetime.now(timezone.utc))],
                  column_names=["view_name", "cutoff", "batch_last_date", "updated_at"])
    log.info("serving_cutoff(price) = %s (batch covers days up to %s)", cutoff.isoformat(), last)


def seed_redis(redis_url: str, silver_price: DataFrame, master: DataFrame) -> None:
    """price:last:{master_id}: hash {platform: "giá|event_time ISO"} theo lần quan sát mới nhất mỗi sàn.
    Speed layer cập nhật tiếp mỗi sự kiện; ghi ở đây chỉ để khởi động không trống."""
    import redis

    latest = (silver_price.where(F.col("price_current").isNotNull())
              .join(master.where(F.col("cluster_size") >= 2).select("product_key", "master_id"), "product_key")
              .groupBy("master_id", "platform")
              .agg(F.max_by("price_current", "event_time").alias("price"), F.max("event_time").alias("ts")))
    r = redis.Redis.from_url(redis_url)
    pipe = r.pipeline(transaction=False)
    n = 0
    for master_id, platform, price, ts in collect_utc(latest, ["master_id", "platform", "price", "ts"]):
        iso = ts.isoformat(timespec="seconds").replace("+00:00", "Z")
        pipe.hset(f"price:last:{master_id}", platform, f"{price}|{iso}")
        n += 1
    pipe.execute()
    log.info("Seeded %d platform prices into Redis price:last:*", n)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.appName("daily_stats").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_price = spark.read.format("delta").load(f"{args.silver_root}/price")
    master = spark.read.format("delta").load(f"{args.gold_root}/product_master")
    bounds = silver_price.agg(F.min("date"), F.max("date")).first()
    d_from, d_to = args.date_from or bounds[0], args.date_to or bounds[1]
    days = [d_from + timedelta(days=i) for i in range((d_to - d_from).days + 1)]
    log.info("Days %s .. %s (%d)", d_from, d_to, len(days))

    in_range = silver_price.where(F.col("date").between(F.lit(d_from), F.lit(d_to)))
    batch = sku_days(in_range, master).cache()
    day_stats = stats(batch).cache()
    day_spread = spread(batch, master).cache()

    write_delta_days(day_stats, f"{args.gold_root}/daily_price_stats", d_from, d_to)
    write_delta_days(day_spread, f"{args.gold_root}/daily_price_spread", d_from, d_to)
    log.info("Gold: %d stats rows, %d spread rows -> %s/daily_price_{stats,spread}",
             day_stats.count(), day_spread.count(), args.gold_root)

    if args.clickhouse_url:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            dsn=args.clickhouse_url, username=os.environ.get("CLICKHOUSE_USER", "bigdata"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""), database="bigdata")
        n_batch = to_clickhouse(client, "price_batch_daily", batch, BATCH_COLS, days)
        n_stats = to_clickhouse(client, "daily_price_stats", day_stats, STATS_COLS, days)
        n_spread = to_clickhouse(client, "daily_price_spread", day_spread, SPREAD_COLS, days)
        log.info("ClickHouse: price_batch_daily %d, daily_price_stats %d, daily_price_spread %d rows",
                 n_batch, n_stats, n_spread)
        update_cutoff(client)
    if args.redis_url:
        seed_redis(args.redis_url, silver_price, master)

    batch.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
