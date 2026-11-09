"""Test jobs/build_silver_sample.py: dựng landing/ giả trong thư mục tạm từ event thật
(tests/fixtures/normalize), chạy job, đọc lại bảng Delta."""

import json
from datetime import date
from pathlib import Path

import orjson
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from common.normalize import MISSING_PRICE, UNKNOWN_PLATFORM, ZERO_PRICE
from jobs.build_silver_sample import (
    BAD_JSON,
    PRICE_SCHEMA,
    PRODUCT_SCHEMA,
    QUARANTINE_SCHEMA,
    build_sample,
    parse_args,
)

FIXTURES = Path(__file__).parent / "fixtures" / "normalize"
D1, D2 = date(2026, 11, 5), date(2026, 11, 6)


def real_events(platform: str, event_type: str) -> list[dict]:
    lines = (FIXTURES / f"{platform}.jsonl").read_text(encoding="utf-8").splitlines()
    return [d["event"] for d in map(json.loads, lines) if d["event"]["event_type"] == event_type]


def on_day(event: dict, day: date, hour: int = 10) -> dict:
    """Event thật nhưng dời event_time sang ngày cần test (để khớp thư mục date=)."""
    return event | {"event_time": f"{day.isoformat()}T{hour:02d}:15:00Z"}


def write_landing(landing: Path, event_type: str, platform: str, day: date, lines: list[bytes | dict],
                  name: str = "part-test.jsonl") -> Path:
    path = landing / event_type / f"platform={platform}" / f"date={day.isoformat()}" / "hour=10" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        for line in lines:
            f.write((line if isinstance(line, bytes) else orjson.dumps(line)) + b"\n")
    return path


def read(path: Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    """Hai ngày dữ liệu: ngày 1 có tgdd (4 price, đủ ca) + tiki (3 price), ngày 2 có fptshop (3 price)."""
    root = tmp_path / "landing"
    write_landing(root, "price", "tgdd", D1, [on_day(e, D1) for e in real_events("tgdd", "price")])
    write_landing(root, "price", "tiki", D1, [on_day(e, D1) for e in real_events("tiki", "price")])
    write_landing(root, "price", "fptshop", D2, [on_day(e, D2) for e in real_events("fptshop", "price")])
    for platform in ("tiki", "hoangha"):
        write_landing(root, "product", platform, D1, [on_day(e, D1) for e in real_events(platform, "product")])
    return root


def test_builds_price_and_product_tables(landing: Path, tmp_path: Path):
    out = tmp_path / "out"
    results = build_sample(landing, out, D1, D2, ["price", "product"])

    price = read(out / "price")
    assert price.schema.names == PRICE_SCHEMA.names
    assert price.num_rows == 4 + 3 + 3
    assert results[("price", D1)].silver == 7
    assert results[("price", D2)].silver == 3
    # Cờ chất lượng của tgdd: 1 thiếu giá, 1 giá 0 (xem fixture)
    assert results[("price", D1)].flags == {MISSING_PRICE: 1, ZERO_PRICE: 1}
    assert set(price.column("ingest_source").to_pylist()) == {"sample"}
    assert price.column("ingest_time").null_count == price.num_rows
    assert set(price.column("date").to_pylist()) == {D1, D2}

    product = read(out / "product")
    assert product.schema.names == PRODUCT_SCHEMA.names
    assert sorted(product.column("platform").to_pylist()) == ["hoangha", "tiki"]
    assert all(json.loads(s) for s in product.column("specs_raw").to_pylist())

    # Dữ liệu thật không có lỗi -> không tạo bảng quarantine rỗng
    assert not (out / "quarantine").exists()


def test_partitions_and_types(landing: Path, tmp_path: Path):
    out = tmp_path / "out"
    build_sample(landing, out, D1, D2, ["price"])
    dt = DeltaTable(str(out / "price"))
    assert dt.metadata().partition_columns == ["date", "platform"]
    assert (out / "price" / "date=2026-11-05" / "platform=tgdd").is_dir()
    price = dt.to_pyarrow_table()
    assert price.schema.field("price_current").type == pa.int64()
    assert price.schema.field("dq_flags").type == pa.list_(pa.string())
    assert str(price.schema.field("event_time").type) == "timestamp[us, tz=UTC]"


def test_rerun_gives_same_rows(landing: Path, tmp_path: Path):
    out = tmp_path / "out"
    build_sample(landing, out, D1, D2, ["price", "product"])
    first_price = read(out / "price").sort_by("event_id")
    first_product = read(out / "product").num_rows
    for _ in range(2):
        build_sample(landing, out, D1, D2, ["price", "product"])
    assert read(out / "price").sort_by("event_id").equals(first_price)
    assert read(out / "product").num_rows == first_product


def test_rerun_one_day_keeps_other_days(landing: Path, tmp_path: Path):
    out = tmp_path / "out"
    build_sample(landing, out, D1, D2, ["price"])
    build_sample(landing, out, D2, D2, ["price"])
    assert read(out / "price").num_rows == 10


def test_duplicates_are_dropped(landing: Path, tmp_path: Path):
    # Cùng event ghi 2 lần (ở 2 file khác nhau, như khi crawler chạy lại) -> Silver chỉ giữ 1
    dup = on_day(real_events("tiki", "price")[0], D1)
    write_landing(landing, "price", "tiki", D1, [dup], name="part-again.jsonl")
    out = tmp_path / "out"
    results = build_sample(landing, out, D1, D1, ["price"])
    assert results[("price", D1)].duplicates == 1
    ids = read(out / "price").column("event_id").to_pylist()
    assert len(ids) == len(set(ids)) == 7


def test_bad_lines_go_to_quarantine(landing: Path, tmp_path: Path):
    bad_platform = on_day(real_events("tiki", "price")[0], D1) | {"event_id": "x-1", "platform": "shopee"}
    write_landing(landing, "price", "tiki", D1, [b'{"event_id": "cut-off', bad_platform], name="part-bad.jsonl")
    out = tmp_path / "out"
    results = build_sample(landing, out, D1, D1, ["price"])

    assert results[("price", D1)].quarantine == 2
    assert results[("price", D1)].errors == {BAD_JSON: 1, UNKNOWN_PLATFORM: 1}
    q = read(out / "quarantine" / "price")
    assert q.schema.names == QUARANTINE_SCHEMA.names
    rows = sorted(q.to_pylist(), key=lambda r: r["dq_errors"])
    assert rows[0]["dq_errors"] == [BAD_JSON] and rows[0]["event_id"] is None
    assert rows[0]["raw"] == '{"event_id": "cut-off'
    assert rows[1]["dq_errors"] == [UNKNOWN_PLATFORM] and rows[1]["event_id"] == "x-1"
    assert all(r["platform"] == "tiki" and r["date"] == D1 for r in rows)
    # Bản ghi lỗi không lọt vào Silver
    assert "x-1" not in read(out / "price").column("event_id").to_pylist()


def test_day_without_data_clears_old_rows(landing: Path, tmp_path: Path):
    out = tmp_path / "out"
    build_sample(landing, out, D1, D2, ["price"])
    # Ngày 2 không còn file nào (ví dụ chạy với thư mục landing khác) -> dòng cũ của ngày 2 bị xoá
    for f in (landing / "price" / "platform=fptshop").rglob("*.jsonl"):
        f.unlink()
    build_sample(landing, out, D2, D2, ["price"])
    assert set(read(out / "price").column("date").to_pylist()) == {D1}


def test_no_data_at_all_creates_nothing(tmp_path: Path):
    out = tmp_path / "out"
    results = build_sample(tmp_path / "empty", out, D1, D1, ["price", "product"])
    assert results[("price", D1)].lines == 0
    assert not out.exists()


def test_landing_is_not_modified(landing: Path, tmp_path: Path):
    before = {p: p.read_bytes() for p in landing.rglob("*") if p.is_file()}
    build_sample(landing, tmp_path / "out", D1, D2, ["price", "product"])
    after = {p: p.read_bytes() for p in landing.rglob("*") if p.is_file()}
    assert before == after


def test_parse_args():
    args = parse_args(["--date-from", "2026-11-05", "--date-to", "2026-11-07"])
    assert (args.date_from, args.date_to, args.event_type) == (D1, date(2026, 11, 7), None)
    assert args.out == Path("datalake_local/silver_sample")
    with pytest.raises(SystemExit):
        parse_args(["--date-from", "2026-11-07", "--date-to", "2026-11-05"])
    with pytest.raises(SystemExit):
        parse_args(["--date-from", "2026-11-05"])
