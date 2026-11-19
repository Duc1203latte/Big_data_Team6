import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.rebuild_silver import QUARANTINE_SCHEMA

log = logging.getLogger("dlq_sink")


def parse_dlq_rows(rows):
    today = datetime.now(timezone.utc).date()
    parsed = []

    for r in rows:
        try:
            obj = json.loads(r.raw)
            ev_id = obj.get("event_id")
            platform = obj.get("platform") or "_unknown"
            dq_errors = obj.get("dq_errors") or ["DLQ_ERROR"]
            raw_line = obj.get("raw") or r.raw
            part = obj.get("kafka_partition") if obj.get("kafka_partition") is not None else r.kafka_partition
            offset = obj.get("kafka_offset") if obj.get("kafka_offset") is not None else r.kafka_offset
        except Exception:
            ev_id = None
            platform = "_unknown"
            dq_errors = ["DLQ_RAW"]
            raw_line = r.raw
            part = r.kafka_partition
            offset = r.kafka_offset

        parsed.append((
            ev_id,
            platform,
            today,
            dq_errors,
            raw_line,
            int(part),
            int(offset),
            r.ingest_time,
            "stream_dlq",
        ))

    return parsed


def process_dlq_batch(batch_df, batch_id, spark, args):
    rows = batch_df.collect()
    if not rows:
        return

    parsed = parse_dlq_rows(rows)
    out_df = spark.createDataFrame(parsed, QUARANTINE_SCHEMA)
    out_path = f"{args.quarantine_root}/price"

    (
        out_df.write.format("delta")
        .mode("append")
        .partitionBy("date")
        .save(out_path)
    )
    log.info("Batch %d: appended %d invalid records to %s", batch_id, len(parsed), out_path)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Write dlq.invalid messages to HDFS quarantine")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="dlq.invalid")
    parser.add_argument("--quarantine-root", default="/datalake/quarantine")
    parser.add_argument("--checkpoint-dir", default="/checkpoints/dlq_sink")
    parser.add_argument("--trigger", default="1 hour")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("dlq_sink").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            F.col("value").cast("string").alias("raw"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("ingest_time"),
        )
    )

    writer = (
        df.writeStream
        .queryName("dlq_sink")
        .foreachBatch(lambda bdf, bid: process_dlq_batch(bdf, bid, spark, args))
        .option("checkpointLocation", args.checkpoint_dir)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()