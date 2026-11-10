"""Kafka -> Bronze trên HDFS bằng Spark Structured Streaming (README-2A mục 9).

Mỗi event_type một query riêng, một checkpoint riêng:
    raw.price   -> /datalake/bronze/price/platform=<sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-*.json.gz
    raw.product -> /datalake/bronze/product/...

Bronze giữ THÔ: cột `raw` là nguyên chuỗi value của message, không parse cả event. Chỉ đọc
`platform` và `event_time` để làm cột partition; `date`/`hour` theo event_time (UTC) để dữ liệu
replay rơi đúng giờ gốc. Không đọc được thì vào `_unknown`, không làm chết job.
Cột `ingest_source` (live/replay) lấy từ header Kafka; Bronze ghi trước 10/11/2026 không có cột này.

Ghi bằng file sink (format json, gzip): exactly-once nhờ `_spark_metadata` + checkpoint, tắt bật
lại không mất không trùng. KHÔNG xoá thư mục checkpoint: xoá là đọc lại từ đầu, nạp trùng.

Chạy (trong thư mục crawler/):
    bash spark/submit.sh --name kafka-to-bronze --detach --conf spark.cores.max=1 jobs/kafka_to_bronze.py
Thử trên topic tạm, chạy hết dữ liệu đang có rồi dừng:
    bash spark/submit.sh jobs/kafka_to_bronze.py --topic-prefix test.k2b. \
        --bronze-root /tmp/k2b/bronze --checkpoint-root /tmp/k2b/checkpoints --available-now
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

log = logging.getLogger("kafka_to_bronze")

EVENT_TYPES = ("price", "product")
PARTITION_COLS = ("platform", "date", "hour")
UNKNOWN = "_unknown"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream raw Kafka events into the HDFS Bronze layer")
    parser.add_argument("--event-type", action="append", choices=EVENT_TYPES, dest="event_types",
                        help="event type to stream (repeatable, default: all)")
    parser.add_argument("--bootstrap", default="kafka:29092",
                        help="Kafka bootstrap servers (listener nội bộ mạng Docker)")
    parser.add_argument("--topic-prefix", default="raw.",
                        help="topic = prefix + event_type (đổi khi thử trên topic tạm)")
    parser.add_argument("--bronze-root", default="/datalake/bronze")
    parser.add_argument("--checkpoint-root", default="/checkpoints/kafka_to_bronze")
    parser.add_argument("--trigger", default="5 minutes", help="processingTime trigger interval")
    parser.add_argument("--max-offsets", type=int, default=50000,
                        help="maxOffsetsPerTrigger: số message tối đa mỗi micro-batch (chặn tải, tránh OOM)")
    parser.add_argument("--available-now", action="store_true",
                        help="xử lý hết dữ liệu đang có trong topic rồi dừng (thử nghiệm / đuổi kịp)")
    args = parser.parse_args(argv)
    args.event_types = args.event_types or list(EVENT_TYPES)
    return args


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Thêm metadata nạp và cột partition, giữ nguyên payload gốc trong cột raw."""
    value = F.col("value").cast("string")
    platform = F.get_json_object(value, "$.platform")
    event_time = F.get_json_object(value, "$.event_time")
    # event_time chuẩn dạng 2026-11-08T07:49:05Z (UTC). Chỉ cắt chuỗi khi đúng khuôn ngày giờ,
    # nếu không (null, sai định dạng) thì vào _unknown thay vì tạo partition rác.
    time_ok = event_time.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}")

    out = kafka_df.select(
        value.alias("raw"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
        # live (crawler gửi) hay replay (replay_producer gửi): chỉ có trong header Kafka, Silver cần.
        # Message không có header này thì null.
        F.expr("cast(filter(headers, h -> h.key = 'ingest_source')[0].value as string)").alias("ingest_source"),
        F.when(F.length(platform) > 0, platform).otherwise(F.lit(UNKNOWN)).alias("platform"),
        F.when(time_ok, F.substring(event_time, 1, 10)).otherwise(F.lit(UNKNOWN)).alias("date"),
        F.when(time_ok, F.substring(event_time, 12, 2)).otherwise(F.lit(UNKNOWN)).alias("hour"),
    )
    # Gom mỗi (sàn, ngày, giờ) về một task: mỗi micro-batch ghi tối đa 1 file cho mỗi partition,
    # thay vì 1 file cho mỗi cặp (partition Kafka x partition Bronze) -> ít file nhỏ hơn nhiều lần.
    return out.repartition(*PARTITION_COLS)


def start_query(spark: SparkSession, event_type: str, args: argparse.Namespace) -> StreamingQuery:
    topic = f"{args.topic_prefix}{event_type}"
    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")      # chỉ dùng lần đầu; sau đó checkpoint quyết định
                .option("maxOffsetsPerTrigger", args.max_offsets)
                .option("includeHeaders", "true")           # để lấy header ingest_source
                .load())

    writer = (to_bronze(kafka_df).writeStream
              .queryName(f"bronze_{event_type}")
              .format("json")
              .option("compression", "gzip")
              .option("path", f"{args.bronze_root}/{event_type}")
              .option("checkpointLocation", f"{args.checkpoint_root}/{event_type}")
              .partitionBy(*PARTITION_COLS)
              .outputMode("append"))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=args.trigger)
    query = writer.start()
    log.info("Started query %s: topic=%s -> %s/%s", query.name, topic, args.bronze_root, event_type)
    return query


class ProgressLogger(StreamingQueryListener):
    """Mỗi micro-batch in 1 dòng: số message, thời gian xử lý, offset cuối -> theo dõi lag qua docker logs."""

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        p = event.progress
        if p.numInputRows == 0:
            return
        end_offsets = [s.endOffset for s in p.sources]
        log.info("Batch %s of %s: rows=%d rate=%.0f rows/s duration=%sms end_offsets=%s",
                 p.batchId, p.name, p.numInputRows, p.processedRowsPerSecond or 0.0,
                 p.durationMs.get("triggerExecution"), end_offsets)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("Query %s terminated with error: %s", event.id, event.exception)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    spark = SparkSession.builder.appName("kafka_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")    # log Spark INFO quá nhiều cho job chạy 24/7
    spark.streams.addListener(ProgressLogger())

    queries = [start_query(spark, et, args) for et in args.event_types]

    if args.available_now:
        # Chạy hết dữ liệu đang có của mọi query rồi thoát
        for q in queries:
            q.awaitTermination()
        log.info("All queries finished (available-now)")
    else:
        # Một query lỗi thì thoát cả tiến trình; container restart unless-stopped sẽ bật lại,
        # đọc tiếp từ checkpoint.
        spark.streams.awaitAnyTermination()
    spark.stop()


if __name__ == "__main__":
    main()
