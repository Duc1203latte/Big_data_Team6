"""Kiểm tra cụm Spark (README-2A bước 7): một job chạm đủ mọi thành phần sẽ dùng ở bước 8 và 11.

  1. Đọc 1 file JSONL trên HDFS
  2. Đọc topic Kafka qua listener nội bộ kafka:29092 (batch, không streaming)
  3. Ghi rồi đọc lại một bảng Delta trên HDFS
  4. Executor import được common.normalize (code mount ở /app) và chạy được trên Python của image

Chạy: bash spark/submit.sh spark/smoke.py --hdfs-file /tmp/spark_smoke/sample.jsonl --topic <topic>
Không ghi gì vào /datalake: chỉ dùng /tmp/spark_smoke trên HDFS.
"""

import argparse
import platform
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-file", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--delta-path", default="/tmp/spark_smoke/delta")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    results = {"driver_python": platform.python_version()}

    # 1. HDFS
    df = spark.read.json(args.hdfs_file)
    results["hdfs_rows"] = df.count()

    # 2. Kafka (listener nội bộ của mạng Docker)
    kafka = (spark.read.format("kafka")
             .option("kafka.bootstrap.servers", "kafka:29092")
             .option("subscribe", args.topic)
             .option("startingOffsets", "earliest")
             .option("endingOffsets", "latest")
             .load())
    results["kafka_rows"] = kafka.count()
    results["kafka_partitions"] = kafka.select("partition").distinct().count()

    # 3. Delta trên HDFS
    df.select("event_id", "platform", "price_current").write.format("delta").mode("overwrite").save(args.delta_path)
    results["delta_rows"] = spark.read.format("delta").load(args.delta_path).count()

    # 4. common.normalize chạy trên executor (Python của image, không phải của WSL)
    def parse_price(raw):
        from common.normalize import parse_price_vnd
        return parse_price_vnd(raw)

    def time_ok(ts):
        from common.normalize import to_utc
        return to_utc(ts) is not None

    def executor_python(_):
        return platform.python_version()

    checked = df.select(
        F.udf(parse_price, LongType())("price_current_raw").alias("price"),
        F.udf(time_ok, BooleanType())("event_time").alias("time_ok"),
        F.udf(executor_python)("event_id").alias("py"),
    )
    agg = checked.agg(F.count("price").alias("parsed"), F.sum(F.col("time_ok").cast("int")).alias("times"),
                      F.first("py").alias("py")).first()
    results["executor_python"] = agg["py"]
    results["normalize_price_parsed"] = agg["parsed"]
    results["normalize_time_ok"] = agg["times"]

    print("SMOKE_RESULT", results, flush=True)
    ok = (results["hdfs_rows"] > 0 and results["kafka_rows"] > 0 and results["delta_rows"] == results["hdfs_rows"]
          and results["normalize_time_ok"] == results["hdfs_rows"])
    print("SMOKE_OK" if ok else "SMOKE_FAILED", flush=True)
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
