"""Bảng Silver mẫu dựng thẳng từ landing/, để thành viên 2 làm Gold khi chưa có Kafka/HDFS/Spark
(README-2A mục 10.4).

Đọc landing/<event_type>/platform=*/date=<ngày>/hour=*/*.jsonl -> common.normalize -> ghi bảng Delta:
    <out>/price/                 silver_price, partition theo date, platform
    <out>/product/               silver_product, partition theo platform
    <out>/quarantine/<type>/     bản ghi lỗi (dq_errors + raw), partition theo date

- Không cần Java, Spark hay Docker: ghi Delta bằng thư viện deltalake (delta-rs) + pyarrow.
- Logic làm sạch nằm hết trong common/normalize.py, job này chỉ đọc file, gọi normalize, ghi bảng.
- Cột giống Silver thật, riêng ingest_time = null và ingest_source = "sample" (không đi qua Bronze).
- Chạy lại được: mỗi ngày được ghi đè trọn (Delta overwrite theo điều kiện), các ngày khác giữ nguyên.
  Chạy 1 hay 5 lần trên cùng khoảng ngày cũng ra cùng số dòng.
- Bỏ event trùng event_id trong cùng ngày (event_id chứa event_time nên bản trùng luôn cùng ngày).
- landing/ chỉ được ĐỌC. Log chỉ in ra màn hình, không ghi vào file log của crawler.

Chạy (trong thư mục crawler/):
    python -m jobs.build_silver_sample --date-from 2026-11-05 --date-to 2026-11-07
    python -m jobs.build_silver_sample --date-from 2026-11-05 --date-to 2026-11-05 --event-type price
"""

import argparse
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import orjson
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from common.normalize import normalize_price, normalize_product

logger = logging.getLogger("build_silver_sample")

INGEST_SOURCE = "sample"
# Dòng không đọc được JSON (ví dụ dòng cuối bị cắt khi crawler chết giữa chừng).
# Không thuộc validate vì chưa có event nào để kiểm tra; vẫn vào quarantine để đếm.
BAD_JSON = "BAD_JSON"
# Đổi dict sang Arrow theo từng lô, để không giữ cả ngày (~280.000 dict) trong RAM cùng lúc
BATCH_ROWS = 50_000

UTC_TS = pa.timestamp("us", tz="UTC")

# Schema silver_price, đúng thứ tự cột README-2A mục 10.2
PRICE_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("platform", pa.string()),
    ("product_id", pa.string()),
    ("product_key", pa.string()),
    ("sku_id", pa.string()),
    ("url", pa.string()),
    ("name_raw", pa.string()),
    ("source", pa.string()),
    ("price_current", pa.int64()),
    ("price_current_raw", pa.string()),
    ("price_original", pa.int64()),
    ("discount_pct", pa.float64()),
    ("currency", pa.string()),
    ("in_stock", pa.bool_()),
    ("seller_id", pa.string()),
    ("seller_name", pa.string()),
    ("rating_avg", pa.float64()),
    ("rating_count", pa.int32()),
    ("review_count", pa.int32()),
    ("sold_count", pa.int32()),
    ("event_time", UTC_TS),
    ("ingest_time", UTC_TS),
    ("ingest_source", pa.string()),
    ("date", pa.date32()),
    ("crawler_version", pa.string()),
    ("dq_flags", pa.list_(pa.string())),
])

# Schema silver_product: các trường của ProductEvent + product_key và metadata nạp
PRODUCT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("platform", pa.string()),
    ("product_id", pa.string()),
    ("product_key", pa.string()),
    ("url", pa.string()),
    ("name_raw", pa.string()),
    ("brand_raw", pa.string()),
    ("category_raw", pa.string()),
    ("specs_raw", pa.string()),            # chuỗi JSON, key đã sắp xếp
    ("image_url", pa.string()),
    ("rating_avg", pa.float64()),
    ("rating_count", pa.int32()),
    ("rating_1_count", pa.int32()),
    ("rating_2_count", pa.int32()),
    ("rating_3_count", pa.int32()),
    ("rating_4_count", pa.int32()),
    ("rating_5_count", pa.int32()),
    ("event_time", UTC_TS),
    ("ingest_time", UTC_TS),
    ("ingest_source", pa.string()),
    ("crawler_version", pa.string()),
])

# Bản ghi lỗi: giữ nguyên dòng gốc để xem lại, kèm lý do
QUARANTINE_SCHEMA = pa.schema([
    ("event_id", pa.string()),             # null nếu dòng hỏng tới mức không đọc được
    ("platform", pa.string()),             # lấy từ thư mục platform=, không lấy từ event
    ("date", pa.date32()),                 # lấy từ thư mục date=, vì event_time có thể hỏng
    ("dq_errors", pa.list_(pa.string())),
    ("raw", pa.string()),
    ("source_file", pa.string()),
])

NORMALIZERS = {"price": normalize_price, "product": normalize_product}
SCHEMAS = {"price": PRICE_SCHEMA, "product": PRODUCT_SCHEMA}
PARTITIONS = {"price": ["date", "platform"], "product": ["platform"]}


@dataclass
class DayStats:
    """Số liệu của một (event_type, ngày), in ra log và dùng trong test."""

    lines: int = 0
    duplicates: int = 0
    silver: int = 0
    quarantine: int = 0
    flags: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)
    platforms: Counter = field(default_factory=Counter)


class BatchBuffer:
    """Gom dict thành RecordBatch theo lô BATCH_ROWS dòng."""

    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema
        self.rows: list[dict] = []
        self.batches: list[pa.RecordBatch] = []

    def add(self, row: dict) -> None:
        self.rows.append(row)
        if len(self.rows) >= BATCH_ROWS:
            self.flush()

    def flush(self) -> None:
        if self.rows:
            self.batches.append(pa.RecordBatch.from_pylist(self.rows, schema=self.schema))
            self.rows = []

    def table(self) -> pa.Table:
        self.flush()
        return pa.Table.from_batches(self.batches, schema=self.schema)


def day_files(landing_dir: Path, event_type: str, day: date) -> list[Path]:
    """Mọi file JSONL của một ngày (UTC), sắp xếp để thứ tự đọc luôn giống nhau giữa các lần chạy."""
    return sorted(landing_dir.glob(f"{event_type}/platform=*/date={day.isoformat()}/hour=*/*.jsonl"))


def platform_of(path: Path) -> str | None:
    """Tên sàn lấy từ thư mục platform=<sàn> trong đường dẫn."""
    for part in path.parts:
        if part.startswith("platform="):
            return part.removeprefix("platform=")
    return None


def build_day(landing_dir: Path, event_type: str, day: date) -> tuple[pa.Table, pa.Table, DayStats]:
    """Đọc landing/ của một ngày -> (bảng silver, bảng quarantine, số liệu)."""
    normalize = NORMALIZERS[event_type]
    silver = BatchBuffer(SCHEMAS[event_type])
    quarantine = BatchBuffer(QUARANTINE_SCHEMA)
    stats = DayStats()
    seen: set[str] = set()

    for path in day_files(landing_dir, event_type, day):
        platform = platform_of(path)
        with path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                stats.lines += 1
                try:
                    event = orjson.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("not a JSON object")
                except ValueError:          # orjson.JSONDecodeError là lớp con của ValueError
                    errors, event = [BAD_JSON], {}
                else:
                    event_id = event.get("event_id")
                    if isinstance(event_id, str) and event_id in seen:
                        stats.duplicates += 1
                        continue
                    row, errors = normalize(event)

                if errors:
                    stats.quarantine += 1
                    stats.errors.update(errors)
                    event_id = event.get("event_id")
                    quarantine.add({
                        "event_id": event_id if isinstance(event_id, str) else None,
                        "platform": platform,
                        "date": day,
                        "dq_errors": errors,
                        "raw": line.decode("utf-8", errors="replace"),
                        "source_file": str(path),
                    })
                    continue

                seen.add(row["event_id"])
                row["ingest_time"] = None
                row["ingest_source"] = INGEST_SOURCE
                silver.add(row)
                stats.silver += 1
                stats.platforms[row["platform"]] += 1
                stats.flags.update(row.get("dq_flags", []))

    return silver.table(), quarantine.table(), stats


def replace_rows(table_path: Path, data: pa.Table, partition_by: list[str], predicate: str) -> None:
    """Ghi đè đúng phần dữ liệu khớp predicate (một ngày), phần còn lại của bảng giữ nguyên.

    Bảng chưa tồn tại mà không có dòng nào thì không tạo bảng rỗng.
    Bảng đã tồn tại mà lần này không có dòng nào thì vẫn ghi, để xoá dữ liệu cũ của ngày đó.
    """
    if data.num_rows == 0 and not DeltaTable.is_deltatable(str(table_path)):
        return
    table_path.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(table_path), data, partition_by=partition_by, mode="overwrite", predicate=predicate)


def day_predicates(event_type: str, day: date) -> tuple[str, str]:
    """(predicate cho bảng silver, predicate cho bảng quarantine) của một ngày UTC."""
    if event_type == "price":
        silver = f"date = '{day.isoformat()}'"
    else:
        # silver_product không có cột date: chọn theo khoảng event_time của ngày đó
        start = f"{day.isoformat()}T00:00:00Z"
        end = f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z"
        silver = f"event_time >= '{start}' AND event_time < '{end}'"
    return silver, f"date = '{day.isoformat()}'"


def build_sample(landing_dir: Path, out_dir: Path, date_from: date, date_to: date,
                 event_types: list[str]) -> dict[tuple[str, date], DayStats]:
    """Dựng bảng mẫu cho mọi ngày trong [date_from, date_to]. Trả số liệu theo (event_type, ngày)."""
    results: dict[tuple[str, date], DayStats] = {}
    day = date_from
    while day <= date_to:
        for event_type in event_types:
            silver, quarantine, stats = build_day(landing_dir, event_type, day)
            silver_pred, quarantine_pred = day_predicates(event_type, day)
            replace_rows(out_dir / event_type, silver, PARTITIONS[event_type], silver_pred)
            replace_rows(out_dir / "quarantine" / event_type, quarantine, ["date"], quarantine_pred)
            results[(event_type, day)] = stats
            logger.info(
                "%s %s: %d lines, %d duplicates, %d silver %s, %d quarantine %s",
                event_type, day, stats.lines, stats.duplicates, stats.silver,
                dict(stats.flags) or "", stats.quarantine, dict(stats.errors) or "",
            )
        day += timedelta(days=1)
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a sample Silver Delta table directly from landing/")
    parser.add_argument("--date-from", type=date.fromisoformat, required=True, help="first UTC day, YYYY-MM-DD")
    parser.add_argument("--date-to", type=date.fromisoformat, required=True, help="last UTC day, YYYY-MM-DD")
    parser.add_argument("--event-type", action="append", choices=sorted(NORMALIZERS),
                        help="default: price and product")
    parser.add_argument("--landing", type=Path, default=Path("landing"))
    parser.add_argument("--out", type=Path, default=Path("datalake_local/silver_sample"))
    args = parser.parse_args(argv)
    if args.date_to < args.date_from:
        parser.error("--date-to is before --date-from")
    return args


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    if args.date_to >= datetime.now(timezone.utc).date():
        logger.warning("%s is today or later (UTC): that day is still being crawled, data is incomplete",
                       args.date_to)
    event_types = args.event_type or ["price", "product"]
    results = build_sample(args.landing, args.out, args.date_from, args.date_to, event_types)
    for event_type in event_types:
        total = sum(s.silver for (t, _), s in results.items() if t == event_type)
        quarantined = sum(s.quarantine for (t, _), s in results.items() if t == event_type)
        logger.info("Done %s: %d silver rows, %d quarantined -> %s", event_type, total, quarantined,
                    args.out / event_type)


if __name__ == "__main__":
    main()
