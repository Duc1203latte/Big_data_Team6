"""compact_bronze: gộp file nhỏ của một ngày Bronze đã kết thúc thành file lớn (~128 MB), không đổi nội dung.

kafka_to_bronze ghi mỗi 5 phút một loạt file .json.gz nhỏ cho từng (sàn, giờ): khoảng 288 lượt/ngày.
Job này đọc trọn một ngày UTC đã xong, ghi lại thành ít file lớn ở thư mục RIÊNG:

    /datalake/bronze/<loại>/platform=/date=/hour=/part-*.json.gz          (nguồn, giữ nguyên)
    /datalake/bronze_compacted/<loại>/date=<ngày>/platform=<sàn>/part-*.json.gz

Vì sao không thay file ngay trong /datalake/bronze: Bronze do file sink của Structured Streaming ghi, danh
sách file hợp lệ nằm trong /datalake/bronze/<loại>/_spark_metadata. Xoá hay thay file ở đó làm metadata trỏ
tới file không còn, rebuild_silver (đọc qua metadata) sẽ lỗi; README cũng cấm sửa Bronze. Bản gộp dùng để lưu
trữ lâu dài và đọc lại nhanh khi cần dựng lại Silver cho ngày cũ.

Kiểm tra: số dòng bản gộp phải bằng số dòng nguồn của ngày đó, sai thì xoá bản gộp và báo lỗi.
Chạy lại cùng ngày ghi đè đúng ngày đó (idempotent).

Chạy:
    bash spark/submit.sh jobs/compact_bronze.py --date 2026-11-15
"""

from __future__ import annotations

import argparse
import logging
import math
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.rebuild_silver import BRONZE_SCHEMA, EVENT_TYPES

log = logging.getLogger("compact_bronze")
TARGET_FILE_BYTES = 128 * 1024 * 1024


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact one closed UTC day of Bronze into large files")
    p.add_argument("--date", type=date.fromisoformat,
                   help="ngày UTC đã kết thúc (mặc định: hôm qua theo UTC)")
    p.add_argument("--event-type", action="append", choices=EVENT_TYPES, dest="event_types")
    p.add_argument("--bronze-root", default="/datalake/bronze")
    p.add_argument("--out-root", default="/datalake/bronze_compacted")
    args = p.parse_args(argv)
    args.date = args.date or (datetime.now(timezone.utc).date() - timedelta(days=1))
    args.event_types = args.event_types or list(EVENT_TYPES)
    return args


def _fs(spark: SparkSession, path: str):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def _count_json(fs, dir_path) -> tuple[int, int]:
    n = size = 0
    it = fs.listFiles(dir_path, True)
    while it.hasNext():
        f = it.next()
        if ".json" in f.getPath().getName():
            n += 1
            size += f.getLen()
    return n, size


def source_files(spark: SparkSession, root: str, day: date) -> tuple[int, int]:
    """(số file, tổng byte) của một ngày trong Bronze: <root>/platform=*/date=<ngày>/hour=*/."""
    fs, path = _fs(spark, root)
    if not fs.exists(path):
        return 0, 0
    n = size = 0
    for platform_dir in fs.listStatus(path):
        day_dir = spark._jvm.org.apache.hadoop.fs.Path(platform_dir.getPath(), f"date={day.isoformat()}")
        if platform_dir.isDirectory() and fs.exists(day_dir):
            dn, ds = _count_json(fs, day_dir)
            n, size = n + dn, size + ds
    return n, size


def compact(spark: SparkSession, args: argparse.Namespace, event_type: str) -> None:
    src_root = f"{args.bronze_root}/{event_type}"
    out_day = f"{args.out_root}/{event_type}/date={args.date.isoformat()}"
    n_files, n_bytes = source_files(spark, src_root, args.date)
    if n_files == 0:
        log.warning("%s %s: no Bronze files, skip", event_type, args.date)
        return

    # Đọc thư mục gốc (đi qua _spark_metadata như rebuild_silver), lọc partition của ngày
    src = (spark.read.schema(BRONZE_SCHEMA).json(src_root)
           .where(F.col("date") == args.date.isoformat()))
    n_rows = src.count()
    n_out = max(1, math.ceil(n_bytes / TARGET_FILE_BYTES))
    (src.drop("date").repartition(n_out, "platform")
     .write.mode("overwrite").option("compression", "gzip").partitionBy("platform").json(out_day))

    written = spark.read.json(out_day).count()
    fs, out_path = _fs(spark, out_day)
    if written != n_rows:
        fs.delete(out_path, True)
        raise RuntimeError(f"{event_type} {args.date}: compacted {written} rows != source {n_rows}, removed {out_day}")
    out_files, out_bytes = _count_json(fs, out_path)
    log.info("%s %s: %d rows; %d files (%.1f MB) -> %d files (%.1f MB) in %s",
             event_type, args.date, n_rows, n_files, n_bytes / 1e6, out_files, out_bytes / 1e6, out_day)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    if args.date >= datetime.now(timezone.utc).date():
        raise SystemExit(f"{args.date} is not a closed UTC day yet")
    spark = SparkSession.builder.appName("compact_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for event_type in args.event_types:
        compact(spark, args, event_type)
    spark.stop()


if __name__ == "__main__":
    main()
