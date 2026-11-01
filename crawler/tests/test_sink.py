"""Test bước 2: FileSink ghi JSONL đúng partition. Chạy offline trong thư mục tạm."""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from pydantic import TypeAdapter

from common.schema import Event, PriceEvent, ProductEvent
from common.sink import FileSink

BASE_TIME = datetime(2026, 10, 25, 15, 0, 5, tzinfo=timezone.utc)


def make_price(i: int, platform: str = "tiki", event_time: datetime = BASE_TIME) -> PriceEvent:
    """Tạo một PriceEvent giả, mỗi i cho một product_id khác nhau."""
    return PriceEvent(
        platform=platform,
        product_id=str(1000 + i),
        url=f"https://example.com/p/{i}",
        name_raw=f"Product {i}",
        source="listing",
        price_current=100_000 + i,
        price_current_raw=f"{100_000 + i}",
        event_time=event_time,
        crawler_version="0.1.0",
        raw_hash="abc",
    )


def partition_file(root, event_type, platform, date, hour, run_id="test-run"):
    return root / event_type / f"platform={platform}" / f"date={date}" / f"hour={hour}" / f"part-{run_id}.jsonl"


def test_write_100_events_read_back_with_pandas(tmp_path):
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write([make_price(i) for i in range(100)])

    path = partition_file(tmp_path, "price", "tiki", "2026-10-25", "15")
    assert path.exists()
    # dtype=False: không để pandas tự đổi product_id "1000" thành số
    df = pd.read_json(path, lines=True, dtype=False)
    assert len(df) == 100
    assert df["event_id"].nunique() == 100
    assert sink.events_written == 100


def test_partition_uses_utc_not_vietnam_time(tmp_path):
    # 05:30 sáng 26/10 giờ VN = 22:30 ngày 25/10 UTC
    vn_time = datetime(2026, 10, 26, 5, 30, tzinfo=timezone(timedelta(hours=7)))
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write([make_price(0, event_time=vn_time)])
    assert partition_file(tmp_path, "price", "tiki", "2026-10-25", "22").exists()


def test_events_split_by_platform_and_hour(tmp_path):
    events = (
        [make_price(i, "tiki", BASE_TIME) for i in range(10)]
        + [make_price(i, "tiki", BASE_TIME + timedelta(hours=1)) for i in range(10)]
        + [make_price(i, "cellphones", BASE_TIME) for i in range(5)]
    )
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write(events)

    files = sorted(tmp_path.rglob("*.jsonl"))
    assert len(files) == 3
    total = sum(len(pd.read_json(f, lines=True, dtype=False)) for f in files)
    assert total == 25


def test_product_event_goes_to_product_folder(tmp_path):
    product = ProductEvent(
        platform="tiki", product_id="1", url="https://example.com/p/1", name_raw="P1",
        event_time=BASE_TIME, crawler_version="0.1.0", raw_hash="abc",
    )
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write([product])
    assert partition_file(tmp_path, "product", "tiki", "2026-10-25", "15").exists()


def test_append_only_never_overwrites(tmp_path):
    # Hai sink cùng run_id ghi vào cùng file: dòng cũ phải còn nguyên
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write([make_price(i) for i in range(10)])
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write([make_price(i) for i in range(10, 20)])

    path = partition_file(tmp_path, "price", "tiki", "2026-10-25", "15")
    assert len(path.read_bytes().splitlines()) == 20


def test_lines_read_back_as_same_events(tmp_path):
    # Mỗi dòng JSONL đọc lại bằng schema phải ra đúng event ban đầu (dùng cho replay tuần 2)
    events = [make_price(i) for i in range(5)]
    with FileSink(tmp_path, run_id="test-run") as sink:
        sink.write(events)

    adapter = TypeAdapter(Event)
    path = partition_file(tmp_path, "price", "tiki", "2026-10-25", "15")
    restored = [adapter.validate_json(line) for line in path.read_bytes().splitlines()]
    assert restored == events


def test_write_after_close_raises(tmp_path):
    sink = FileSink(tmp_path, run_id="test-run")
    sink.close()
    with pytest.raises(RuntimeError):
        sink.write([make_price(0)])


def test_invalid_run_id_rejected(tmp_path):
    with pytest.raises(ValueError):
        FileSink(tmp_path, run_id="a/b")
