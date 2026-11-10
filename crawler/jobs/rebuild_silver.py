"""Bronze -> Silver (Delta) trên HDFS bằng Spark batch (README-2A mục 10.3).

Mỗi (event_type, ngày UTC):
  1. Đọc Bronze /datalake/bronze/<event_type> (thư mục gốc, có _spark_metadata), lọc partition date=<ngày>.
  2. Bỏ trùng theo event_id (Bronze cho phép trùng, ví dụ replay 2 lần); giữ bản vào Bronze sớm nhất.
  3. Parse JSON đầy đủ -> common.normalize (validate + dòng Silver). KHÔNG có logic làm sạch nào ở đây.
  4. Hợp lệ -> MERGE vào Silver theo event_id; lỗi -> quarantine (ghi đè trọn ngày đó).
Chạy lại cùng một ngày ra đúng số bản ghi như cũ.

    /datalake/silver/price                 silver_price, partition date, platform
    /datalake/silver/product               silver_product, partition platform (giữ lịch sử, mỗi event 1 dòng)
    /datalake/quarantine/<event_type>      bản ghi lỗi (dq_errors + raw), partition date

ingest_source (live/replay): lấy từ cột ingest_source của Bronze (header Kafka). Bronze ghi trước khi có
cột đó thì suy theo mốc chuyển crawler sang Kafka (--switch-time): event trước mốc chỉ có thể đến từ
replay, event sau mốc do crawler gửi.

Dòng Bronze ở partition date=_unknown (event_time không đọc được) không thuộc ngày nào nên không xử lý ở đây.

Chạy (trong thư mục crawler/):
    bash spark/submit.sh jobs/rebuild_silver.py --date 2026-11-08 --event-type price
    bash spark/submit.sh jobs/rebuild_silver.py --date-from 2026-11-01 --date-to 2026-11-08
"""

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.normalize import BAD_JSON, normalize_price, normalize_product

log = logging.getLogger("rebuild_silver")

# Mốc crawler chuyển sang SINK=file,kafka (README-2A bước 9), dùng cho Bronze chưa có cột ingest_source
SWITCH_TIME = "2026-11-09T15:28:38Z"
EVENT_TYPES = ("price", "product")
NORMALIZERS = {"price": normalize_price, "product": normalize_product}

TS = T.TimestampType()

# Bronze do kafka_to_bronze ghi (JSON). Khai báo schema để khỏi suy kiểu (hour=07 bị đọc thành số 7).
BRONZE_SCHEMA = T.StructType([
    T.StructField("raw", T.StringType()),
    T.StructField("kafka_key", T.StringType()),
    T.StructField("topic", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", TS),
    T.StructField("ingest_time", TS),
    T.StructField("ingest_source", T.StringType()),
    T.StructField("platform", T.StringType()),
    T.StructField("date", T.StringType()),
    T.StructField("hour", T.StringType()),
])

# silver_price, đúng thứ tự cột README-2A mục 10.2 và docs/bronze_silver_schema.md
PRICE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("platform", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("product_key", T.StringType()),
    T.StructField("sku_id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("name_raw", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("price_current", T.LongType()),
    T.StructField("price_current_raw", T.StringType()),
    T.StructField("price_original", T.LongType()),
    T.StructField("discount_pct", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("in_stock", T.BooleanType()),
    T.StructField("seller_id", T.StringType()),
    T.StructField("seller_name", T.StringType()),
    T.StructField("rating_avg", T.DoubleType()),
    T.StructField("rating_count", T.IntegerType()),
    T.StructField("review_count", T.IntegerType()),
    T.StructField("sold_count", T.IntegerType()),
    T.StructField("event_time", TS),
    T.StructField("ingest_time", TS),
    T.StructField("ingest_source", T.StringType()),
    T.StructField("date", T.DateType()),
    T.StructField("crawler_version", T.StringType()),
    T.StructField("dq_flags", T.ArrayType(T.StringType())),
])

# silver_product: các trường của ProductEvent + product_key và metadata nạp
PRODUCT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("platform", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("product_key", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("name_raw", T.StringType()),
    T.StructField("brand_raw", T.StringType()),
    T.StructField("category_raw", T.StringType()),
    T.StructField("specs_raw", T.StringType()),           # chuỗi JSON, key đã sắp xếp
    T.StructField("image_url", T.StringType()),
    T.StructField("rating_avg", T.DoubleType()),
    T.StructField("rating_count", T.IntegerType()),
    T.StructField("rating_1_count", T.IntegerType()),
    T.StructField("rating_2_count", T.IntegerType()),
    T.StructField("rating_3_count", T.IntegerType()),
    T.StructField("rating_4_count", T.IntegerType()),
    T.StructField("rating_5_count", T.IntegerType()),
    T.StructField("event_time", TS),
    T.StructField("ingest_time", TS),
    T.StructField("ingest_source", T.StringType()),
    T.StructField("crawler_version", T.StringType()),
])

# Bản ghi lỗi: giữ nguyên dòng gốc để xem lại, kèm lý do và vị trí trong Kafka
QUARANTINE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),            # null nếu dòng hỏng tới mức không đọc được
    T.StructField("platform", T.StringType()),            # partition platform của Bronze
    T.StructField("date", T.DateType()),                  # partition date của Bronze (event_time có thể hỏng)
    T.StructField("dq_errors", T.ArrayType(T.StringType())),
    T.StructField("raw", T.StringType()),
    T.StructField("kafka_partition", T.IntegerType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("ingest_time", TS),
    T.StructField("ingest_source", T.StringType()),
])

SILVER_SCHEMAS = {"price": PRICE_SCHEMA, "product": PRODUCT_SCHEMA}
PARTITIONS = {"price": ["date", "platform"], "product": ["platform"]}


def to_tuple(row: dict, schema: T.StructType) -> tuple:
    """dict -> tuple theo thứ tự cột của schema. Số nguyên ở cột double (rating_avg = 5) đổi sang float,
    vì Spark không nhận int cho DoubleType. Không đổi gì khác: kiểu sai thật thì để Spark báo lỗi."""
    values = []
    for field in schema.fields:
        value = row.get(field.name)
        if isinstance(field.dataType, T.DoubleType) and isinstance(value, int) and not isinstance(value, bool):
            value = float(value)
        values.append(value)
    return tuple(values)


def split_rows(event_type: str, day: date, rows):
    """Chạy trên executor: mỗi dòng Bronze -> (dòng silver, None) hoặc (None, dòng quarantine)."""
    normalize = NORMALIZERS[event_type]
    silver_schema = SILVER_SCHEMAS[event_type]
    for r in rows:
        try:
            event = json.loads(r.raw)
            if not isinstance(event, dict):
                raise ValueError("not a JSON object")
        except (TypeError, ValueError):         # raw null hoặc không phải JSON
            event, row, errors = {}, None, [BAD_JSON]
        else:
            row, errors = normalize(event)

        if errors:
            event_id = event.get("event_id")
            yield None, (event_id if isinstance(event_id, str) else None, r.platform, day, errors, r.raw,
                         r.kafka_partition, r.kafka_offset, r.ingest_time, r.ingest_source)
        else:
            row["ingest_time"] = r.ingest_time
            row["ingest_source"] = r.ingest_source
            yield to_tuple(row, silver_schema), None


def read_bronze_day(spark: SparkSession, bronze_root: str, event_type: str, day: date,
                    switch_time: str) -> DataFrame:
    """Bronze của một ngày, đã bỏ trùng event_id, có cột ingest_source đầy đủ."""
    bronze = (spark.read.schema(BRONZE_SCHEMA).json(f"{bronze_root}/{event_type}")
              .where(F.col("date") == day.isoformat()))
    event_time = F.get_json_object("raw", "$.event_time")
    ingest_source = F.coalesce(
        F.col("ingest_source"),
        F.when(event_time < switch_time, F.lit("replay")).otherwise(F.lit("live")),
    )
    # Khoá bỏ trùng: event_id; dòng không có event_id thì mỗi dòng là một khoá riêng (vào quarantine cả)
    dedup_key = F.coalesce(F.get_json_object("raw", "$.event_id"),
                           F.concat_ws(":", F.lit("_no_event_id"), "kafka_partition", "kafka_offset"))
    first_seen = Window.partitionBy("_key").orderBy("ingest_time", "kafka_partition", "kafka_offset")
    return (bronze.withColumn("ingest_source", ingest_source)
            .withColumn("_key", dedup_key)
            .withColumn("_rn", F.row_number().over(first_seen))
            .where(F.col("_rn") == 1)
            .select("raw", "platform", "ingest_time", "ingest_source", "kafka_partition", "kafka_offset"))


def merge_silver(spark: SparkSession, path: str, df: DataFrame, event_type: str) -> None:
    """MERGE theo event_id: có rồi thì cập nhật, chưa có thì thêm. Tạo bảng rỗng nếu chưa có."""
    partitions = PARTITIONS[event_type]
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], df.schema).write.format("delta").partitionBy(*partitions).save(path)
    # Chỉ quét những partition có trong dữ liệu nguồn, không quét cả bảng
    first = partitions[0]
    values = sorted(str(r[0]) for r in df.select(first).distinct().collect())
    if not values:
        return
    in_list = ", ".join(f"'{v}'" for v in values)
    condition = f"t.event_id = s.event_id AND t.{first} = s.{first} AND t.{first} IN ({in_list})"
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(df.alias("s"), condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())


def replace_quarantine(spark: SparkSession, path: str, df: DataFrame, day: date, rows: int) -> None:
    """Ghi đè trọn ngày `day` của bảng quarantine (kể cả khi lần này không còn dòng lỗi nào)."""
    if rows == 0 and not DeltaTable.isDeltaTable(spark, path):
        return                                   # không tạo bảng rỗng
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"date = '{day.isoformat()}'")
     .partitionBy("date").save(path))


def count_codes(df: DataFrame, column: str) -> dict[str, int]:
    """Đếm từng mã trong cột mảng (dq_flags / dq_errors)."""
    rows = df.select(F.explode(column).alias("code")).groupBy("code").count().collect()
    return {r["code"]: r["count"] for r in sorted(rows, key=lambda r: r["code"])}


def rebuild_day(spark: SparkSession, args: argparse.Namespace, event_type: str, day: date) -> dict:
    output_schema = T.StructType([
        T.StructField("silver", SILVER_SCHEMAS[event_type]),
        T.StructField("quarantine", QUARANTINE_SCHEMA),
    ])
    bronze = read_bronze_day(spark, args.bronze_root, event_type, day, args.switch_time)
    out = spark.createDataFrame(bronze.rdd.mapPartitions(lambda rows: split_rows(event_type, day, rows)),
                                output_schema).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        silver = out.where(F.col("silver").isNotNull()).select("silver.*")
        quarantine = out.where(F.col("quarantine").isNotNull()).select("quarantine.*")
        stats = {"distinct": out.count(), "silver": silver.count(), "quarantine": quarantine.count()}
        if event_type == "price":
            stats["flags"] = count_codes(silver, "dq_flags")
        stats["errors"] = count_codes(quarantine, "dq_errors")

        silver_path = f"{args.silver_root}/{event_type}"
        merge_silver(spark, silver_path, silver, event_type)
        replace_quarantine(spark, f"{args.quarantine_root}/{event_type}", quarantine, day, stats["quarantine"])

        # Số dòng của ngày này trong bảng Silver sau khi MERGE (chạy lại phải ra y hệt)
        table = spark.read.format("delta").load(silver_path)
        if event_type == "price":
            table = table.where(F.col("date") == F.lit(day))
        else:
            table = table.where(F.to_date("event_time") == F.lit(day))    # session timezone = UTC
        stats["silver_table_day"] = table.count()      # merge_silver luôn tạo bảng nên bảng đã tồn tại
    finally:
        out.unpersist()
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild Silver Delta tables from the HDFS Bronze layer")
    parser.add_argument("--date", type=date.fromisoformat, help="one UTC day, YYYY-MM-DD")
    parser.add_argument("--date-from", type=date.fromisoformat, help="first UTC day")
    parser.add_argument("--date-to", type=date.fromisoformat, help="last UTC day")
    parser.add_argument("--event-type", action="append", choices=EVENT_TYPES, dest="event_types",
                        help="default: price and product")
    parser.add_argument("--bronze-root", default="/datalake/bronze")
    parser.add_argument("--silver-root", default="/datalake/silver")
    parser.add_argument("--quarantine-root", default="/datalake/quarantine")
    parser.add_argument("--switch-time", default=SWITCH_TIME,
                        help="UTC time the crawler switched to Kafka; used when Bronze has no ingest_source")
    args = parser.parse_args(argv)
    if args.date:
        if args.date_from or args.date_to:
            parser.error("use either --date or --date-from/--date-to")
        args.date_from = args.date_to = args.date
    if not (args.date_from and args.date_to):
        parser.error("--date or both --date-from and --date-to are required")
    if args.date_to < args.date_from:
        parser.error("--date-to is before --date-from")
    args.event_types = args.event_types or list(EVENT_TYPES)
    return args


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    if args.date_to >= datetime.now(timezone.utc).date():
        log.warning("%s is today or later (UTC): that day is still being crawled, Silver will be incomplete",
                    args.date_to)

    spark = SparkSession.builder.appName("rebuild_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    totals = {et: {"silver": 0, "quarantine": 0} for et in args.event_types}
    day = args.date_from
    while day <= args.date_to:
        for event_type in args.event_types:
            stats = rebuild_day(spark, args, event_type, day)
            totals[event_type]["silver"] += stats["silver"]
            totals[event_type]["quarantine"] += stats["quarantine"]
            log.info("%s %s: %d distinct events -> %d silver %s, %d quarantine %s; silver table has %d rows "
                     "for the day", event_type, day, stats["distinct"], stats["silver"], stats.get("flags", ""),
                     stats["quarantine"], stats["errors"] or "", stats["silver_table_day"])
        day += timedelta(days=1)

    for event_type, t in totals.items():
        log.info("Done %s: %d silver rows, %d quarantined -> %s/%s", event_type, t["silver"], t["quarantine"],
                 args.silver_root, event_type)
    spark.stop()


if __name__ == "__main__":
    main()
