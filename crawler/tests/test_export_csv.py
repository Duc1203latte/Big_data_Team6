"""Test jobs/export_csv.py: dữ liệu giả ghi bằng FileSink vào thư mục tạm, không đọc landing/ thật."""

import csv
import sqlite3
from datetime import date, datetime, timezone

import orjson

from common.schema import PriceEvent, ProductEvent
from common.sink import FileSink
from jobs.export_csv import (
    PRICE_COLUMNS,
    PRODUCT_COLUMNS,
    csv_value,
    export_all,
    fmt_int,
    quantile,
)

T1 = datetime(2026, 11, 3, 1, 0, 5, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 3, 1, 30, 5, tzinfo=timezone.utc)
T3 = datetime(2026, 11, 4, 2, 0, 5, tzinfo=timezone.utc)


def price(product_id, time, price_current, price_original=None, platform="tiki", sku_id=None, name="Máy A"):
    return PriceEvent(platform=platform, product_id=product_id, url=f"https://x/{product_id}", name_raw=name,
                      event_time=time, crawler_version="0.1.0", raw_hash="h", source="listing",
                      sku_id=sku_id, price_current=price_current,
                      price_current_raw=None if price_current is None else str(price_current),
                      price_original=price_original, in_stock=True)


def product(product_id, time, category, platform="tiki"):
    return ProductEvent(platform=platform, product_id=product_id, url=f"https://x/{product_id}",
                        name_raw="Máy A", event_time=time, crawler_version="0.1.0", raw_hash="h",
                        brand_raw="Hãng", category_raw=category, specs_raw={"RAM": "8 GB"})


def write(landing, events, run_id="run-1"):
    sink = FileSink(landing, run_id)
    sink.write(events)
    sink.close()


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def make_landing(tmp_path):
    landing = tmp_path / "landing"
    write(landing, [
        price("p1", T1, 100_000, 120_000),
        price("p1", T2, 90_000, 120_000),          # p1 đổi giá
        price("p2", T1, None),                      # "Liên hệ": không có giá
        price("p3", T3, 5_000_000),
        product("p1", T1, "Điện thoại|Android"),
        price("h1", T1, 200_000, platform="hoangha"),
    ])
    return landing


def test_exports_csv_per_platform_and_type(tmp_path):
    landing = make_landing(tmp_path)
    out = tmp_path / "exports"
    export_all(landing, out, None, None, None, None)

    assert sorted(p.name for p in out.iterdir()) == [
        "price_hoangha.csv", "price_tiki.csv", "product_tiki.csv", "summary.md"]
    rows = read_csv(out / "price_tiki.csv")
    assert list(rows[0]) == PRICE_COLUMNS
    assert len(rows) == 4
    assert rows[0]["name_raw"] == "Máy A"                 # tiếng Việt giữ nguyên
    assert rows[0]["in_stock"] == "true"
    p2 = next(r for r in rows if r["product_id"] == "p2")
    assert p2["price_current"] == ""                       # None -> ô rỗng
    prod = read_csv(out / "product_tiki.csv")
    assert list(prod[0]) == PRODUCT_COLUMNS
    assert orjson.loads(prod[0]["specs_raw"]) == {"RAM": "8 GB"}


def test_csv_has_bom_for_excel(tmp_path):
    out = tmp_path / "exports"
    export_all(make_landing(tmp_path), out, None, None, None, None)
    assert (out / "price_tiki.csv").read_bytes()[:3] == b"\xef\xbb\xbf"


def test_duplicates_and_bad_lines_skipped(tmp_path):
    landing = make_landing(tmp_path)
    write(landing, [price("p1", T1, 100_000, 120_000)], run_id="run-2")      # trùng event_id
    bad = next((landing / "price" / "platform=tiki").rglob("*.jsonl"))
    with bad.open("ab") as f:
        f.write(b'{"event_id": "cut-in-the-mid')                              # dòng ghi dở
    summaries, csv_rows = export_all(landing, tmp_path / "exports", None, None, None, None)
    assert csv_rows["tiki"] == {"price": 4, "product": 1}
    assert summaries["tiki"].duplicates["price"] == 1
    assert summaries["tiki"].bad_lines == 1


def test_landing_is_not_modified(tmp_path):
    landing = make_landing(tmp_path)
    before = {p: p.read_bytes() for p in landing.rglob("*.jsonl")}
    export_all(landing, tmp_path / "exports", None, None, None, None)
    assert {p: p.read_bytes() for p in landing.rglob("*.jsonl")} == before


def test_filters_by_platform_and_date(tmp_path):
    landing = make_landing(tmp_path)
    out = tmp_path / "exports"
    _, csv_rows = export_all(landing, out, None, ["tiki"], date(2026, 11, 4), None)
    assert csv_rows == {"tiki": {"price": 1}}               # chỉ p3 (ngày 04/11)
    assert not (out / "price_hoangha.csv").exists()


def test_summary_statistics(tmp_path):
    landing = make_landing(tmp_path)
    summaries, _ = export_all(landing, tmp_path / "exports", None, None, None, None)
    tiki = summaries["tiki"]
    assert tiki.events == {"price": 4, "product": 1}
    assert len(tiki.product_ids) == 3
    assert tiki.by_day == {"2026-11-03": 3, "2026-11-04": 1}
    assert tiki.filled["price.price_current"] == 3
    assert tiki.filled["price.seller_id"] == 0          # dữ liệu giả không có seller_id
    p1 = tiki.skus[("p1", None)]
    assert p1.observations == 2 and p1.last_price == 90_000 and p1.prices == {100_000, 90_000}
    assert tiki.categories == {"p1": "Điện thoại|Android"}


def test_summary_markdown(tmp_path):
    landing = make_landing(tmp_path)
    db = tmp_path / "crawler.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE crawl_runs (run_id TEXT, job TEXT, platform TEXT, started_at TEXT, "
                 "ended_at TEXT, status TEXT, events_count INTEGER, error TEXT)")
    conn.execute("INSERT INTO crawl_runs VALUES ('r1', 'crawl_hot', 'tiki', '', '', 'ok', 2304, NULL)")
    conn.commit()
    conn.close()
    out = tmp_path / "exports"
    export_all(landing, out, db, None, None, None, now=T3)
    text = (out / "summary.md").read_text(encoding="utf-8")
    assert "# Thống kê dữ liệu đã crawl" in text
    assert "| tiki | 4 | 1 | 3 | 3 |" in text                 # price, product, sản phẩm, SKU
    assert "| 2026-11-03 | 1 | 3 | 4 |" in text                 # hoangha, tiki, tổng
    assert "| Điện thoại/Android | 1 |" in text                 # dấu | trong danh mục được thay
    assert "| crawl_hot | tiki | ok | 1 | 2.304 |" in text
    # tiki: p1 đổi giá (1/3 SKU), p1 đang có giá gốc > giá bán (1/2 SKU có giá)
    assert "1 (33.3%)" in text and "50.0%" in text


def test_helpers():
    assert quantile([], 0.5) is None
    assert quantile([1, 2, 3, 4, 5], 0.5) == 3
    assert quantile([1, 2, 3, 4, 5], 0) == 1 and quantile([1, 2, 3, 4, 5], 1) == 5
    assert fmt_int(1234567) == "1.234.567" and fmt_int(None) == "-"
    assert csv_value(None) == "" and csv_value(False) == "false"
    assert csv_value({"a": 1}) == '{"a":1}' and csv_value(5) == 5
