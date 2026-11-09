"""Test common/normalize.py (README-2A mục 10.1).

Fixture trong tests/fixtures/normalize/<sàn>.jsonl là event THẬT lấy từ landing/, mỗi dòng
{"case": <tên ca>, "event": <event gốc, không sửa>}. Ca nào không có trong dữ liệu thật
(ví dụ tiki chưa bao giờ trả giá 0) thì dựng từ một event thật ngay trong test, ghi rõ là dựng.
"""

import copy
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.normalize import (
    BAD_TIMESTAMP,
    MISSING_EVENT_ID,
    MISSING_PRICE,
    MISSING_PRODUCT_ID,
    PRICE_OUT_OF_RANGE,
    UNKNOWN_PLATFORM,
    ZERO_PRICE,
    compute_discount_pct,
    normalize_price,
    normalize_product,
    parse_price_vnd,
    to_utc,
    validate,
)

FIXTURES = Path(__file__).parent / "fixtures" / "normalize"
PLATFORMS = ("tiki", "cellphones", "hoangha", "fptshop", "tgdd")


def load_cases(platform: str) -> dict[str, dict]:
    lines = (FIXTURES / f"{platform}.jsonl").read_text(encoding="utf-8").splitlines()
    return {d["case"]: d["event"] for d in map(json.loads, lines)}


def event(platform: str, case: str) -> dict:
    return copy.deepcopy(load_cases(platform)[case])


# ---------------------------------------------------------------------------
# parse_price_vnd
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    # Các dạng thật gặp trong landing/
    ("25990000", 25990000),             # fptshop, tgdd, tiki
    ("38.690.000đ", 38690000),          # cellphones, hoangha
    ("590.000", 590000),                # hoangha, không có chữ đ
    ("41,490,000 ₫", 41490000),         # hoangha trang chi tiết
    ("490,000\xa0₫", 490000),           # khoảng trắng không ngắt dòng
    ("7500000.0", 7500000),             # JSON-LD tgdd, hoangha
    ("0", 0),                           # giá 0: vẫn đọc ra 0, validate quyết định
    # Các dạng khác nên đọc được
    ("25.990.000 VND", 25990000),
    ("25.990.000 vnđ", 25990000),
    (" 1.000 ", 1000),
    (25990000, 25990000),
    (7500000.0, 7500000),
    # Không đọc được -> None
    ("Liên hệ", None),
    ("Giá Liên Hệ", None),
    ("lowPrice=490000.0;highPrice=690000.0", None),
    ("", None),
    ("   ", None),
    ("đ", None),
    (None, None),
    ("1.5", None),                      # có số lẻ: không phải giá VND
    ("12.34.567", None),                # nhóm không đủ 3 chữ số
    ("25.990,000", None),               # trộn dấu chấm và dấu phẩy
    ("-25990000", None),
    (-1, None),
    (1.5, None),
    (True, None),                       # bool là lớp con của int, phải loại
    ([25990000], None),
])
def test_parse_price_vnd(raw, expected):
    assert parse_price_vnd(raw) == expected


def test_parse_price_returns_plain_int():
    # Kết quả phải là int thật (không phải Decimal/float) để ghi được vào cột bigint
    assert type(parse_price_vnd("7500000.0")) is int
    assert type(parse_price_vnd(7500000.0)) is int


# ---------------------------------------------------------------------------
# to_utc
# ---------------------------------------------------------------------------

def test_to_utc_crawler_format():
    assert to_utc("2026-11-06T10:45:54Z") == datetime(2026, 11, 6, 10, 45, 54, tzinfo=timezone.utc)


def test_to_utc_converts_other_offset():
    assert to_utc("2026-11-06T17:45:54+07:00") == datetime(2026, 11, 6, 10, 45, 54, tzinfo=timezone.utc)


def test_to_utc_naive_string_is_vietnam_time():
    assert to_utc("2026-11-06 17:45:54") == datetime(2026, 11, 6, 10, 45, 54, tzinfo=timezone.utc)


def test_to_utc_datetime_inputs():
    naive = datetime(2026, 11, 6, 17, 45, 54)
    aware = datetime(2026, 11, 6, 17, 45, 54, tzinfo=timezone(timedelta(hours=7)))
    expected = datetime(2026, 11, 6, 10, 45, 54, tzinfo=timezone.utc)
    assert to_utc(naive) == expected
    assert to_utc(aware) == expected
    assert to_utc(aware).tzinfo == timezone.utc


@pytest.mark.parametrize("ts", [None, "", "yesterday", "2026-13-01T00:00:00Z", 1730000000, ["2026-11-06"]])
def test_to_utc_unreadable(ts):
    assert to_utc(ts) is None


# ---------------------------------------------------------------------------
# compute_discount_pct
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("current, original, expected", [
    (4990000, 6990000, 28.61),
    (100, 100, 0.0),
    (0, 100, 100.0),
    (None, 100, None),
    (100, None, None),
    (100, 0, None),
    (100, -5, None),
    (110, 100, None),          # giá gốc thấp hơn giá bán: dữ liệu sàn sai
    (-1, 100, None),
])
def test_compute_discount_pct(current, original, expected):
    assert compute_discount_pct(current, original) == expected


# ---------------------------------------------------------------------------
# validate + normalize_price trên event thật của 5 sàn
# ---------------------------------------------------------------------------

# (sàn, ca) -> (flags, price_current, price_original, discount_pct) mong đợi ở dòng Silver
EXPECTED = {
    ("tiki", "listing_discount"): ([], 4990000, 6990000, 28.61),
    ("tiki", "listing_no_discount"): ([], 33350000, 33350000, 0.0),
    ("tiki", "detail_with_seller"): ([], 380000, 380000, 0.0),
    ("cellphones", "listing_discount"): ([], 38690000, 38990000, 0.77),
    ("cellphones", "contact_price"): ([MISSING_PRICE], None, None, None),
    ("cellphones", "zero_price"): ([ZERO_PRICE], None, None, None),
    ("cellphones", "detail_in_stock"): ([], 18790000, None, None),
    ("hoangha", "dot_groups_dong"): ([], 1390000, 1690000, 17.75),
    ("hoangha", "comma_groups"): ([], 41490000, None, None),
    ("hoangha", "contact_price"): ([MISSING_PRICE], None, None, None),
    ("hoangha", "null_price_decimal_raw"): ([], 290000, None, None),        # lấy lại từ chữ gốc
    ("hoangha", "price_range_raw"): ([], 490000, None, None),               # giữ số crawler đã chọn
    ("hoangha", "original_below_current"): ([], 2490000, 2390000, None),
    ("hoangha", "missing_stock"): ([], 21990000, 23990000, 8.34),
    ("fptshop", "listing_discount"): ([], 6190000, 6990000, 11.44),
    ("fptshop", "no_discount"): ([], 103990000, 103990000, 0.0),
    ("fptshop", "zero_price_zero_original"): ([ZERO_PRICE], None, None, None),
    ("tgdd", "listing_discount"): ([], 5790000, 5990000, 3.34),
    ("tgdd", "no_price_no_raw"): ([MISSING_PRICE], None, None, None),
    ("tgdd", "null_price_decimal_raw"): ([], 5990000, None, None),          # lấy lại từ chữ gốc
    ("tgdd", "zero_price"): ([ZERO_PRICE], None, None, None),
}


def test_fixtures_cover_every_platform_with_at_least_3_price_cases():
    for platform in PLATFORMS:
        cases = load_cases(platform)
        price_cases = [c for c, e in cases.items() if e["event_type"] == "price"]
        assert len(price_cases) >= 3, platform
        assert all(e["platform"] == platform for e in cases.values())
        # Mọi ca giá trong fixture đều có kết quả mong đợi, không ca nào bị bỏ quên
        assert {(platform, c) for c in price_cases} <= EXPECTED.keys()


@pytest.mark.parametrize("platform, case", sorted(EXPECTED))
def test_real_price_events(platform, case):
    flags, price_current, price_original, discount = EXPECTED[(platform, case)]
    ev = event(platform, case)

    assert validate(ev) == ([], flags)
    row, errors = normalize_price(ev)
    assert errors == []
    assert row["dq_flags"] == flags
    assert row["price_current"] == price_current
    assert row["price_original"] == price_original
    assert row["discount_pct"] == discount
    # Các cột chép thẳng từ event
    for col in ("event_id", "platform", "product_id", "sku_id", "url", "name_raw", "source",
                "price_current_raw", "in_stock", "seller_id", "crawler_version"):
        assert row[col] == ev[col], col
    assert row["product_key"] == f"{platform}:{ev['product_id']}"
    assert row["event_time"] == to_utc(ev["event_time"])
    assert row["date"] == row["event_time"].date()


def test_tiki_zero_and_missing_price_built_from_real_event():
    # Dựng: tiki chưa từng trả giá 0 hay thiếu giá trong landing/, lấy event thật rồi đổi giá
    zero = event("tiki", "listing_discount") | {"price_current": 0, "price_current_raw": "0"}
    missing = event("tiki", "listing_discount") | {"price_current": None, "price_current_raw": None}
    assert validate(zero) == ([], [ZERO_PRICE])
    assert validate(missing) == ([], [MISSING_PRICE])
    assert normalize_price(zero)[0]["price_current"] is None
    assert normalize_price(missing)[0]["discount_pct"] is None


def test_silver_price_columns_match_schema():
    # Đúng các cột của silver_price ở README-2A mục 10.2, trừ ingest_time và ingest_source do job thêm
    row, _ = normalize_price(event("tiki", "listing_discount"))
    assert list(row) == [
        "event_id", "platform", "product_id", "product_key", "sku_id", "url", "name_raw", "source",
        "price_current", "price_current_raw", "price_original", "discount_pct", "currency", "in_stock",
        "seller_id", "seller_name", "rating_avg", "rating_count", "review_count", "sold_count",
        "event_time", "date", "crawler_version", "dq_flags",
    ]


def test_date_partition_is_utc_day():
    # 23:30 UTC ngày 01/11 là 06:30 sáng 02/11 giờ VN, nhưng partition vẫn là ngày UTC 01/11
    ev = event("tgdd", "listing_discount") | {"event_time": "2026-11-01T23:30:00Z"}
    row, _ = normalize_price(ev)
    assert row["date"] == date(2026, 11, 1)


def test_normalize_is_deterministic_and_does_not_mutate_input():
    ev = event("hoangha", "null_price_decimal_raw")
    before = copy.deepcopy(ev)
    assert normalize_price(ev) == normalize_price(ev)
    assert ev == before


# ---------------------------------------------------------------------------
# errors -> quarantine
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("change, expected", [
    ({"event_id": ""}, [MISSING_EVENT_ID]),
    ({"event_id": None}, [MISSING_EVENT_ID]),
    ({"product_id": "  "}, [MISSING_PRODUCT_ID]),
    ({"event_time": "not a time"}, [BAD_TIMESTAMP]),
    ({"event_time": None}, [BAD_TIMESTAMP]),
    ({"platform": "shopee"}, [UNKNOWN_PLATFORM]),
    ({"price_current": 999, "price_current_raw": "999"}, [PRICE_OUT_OF_RANGE]),
    ({"price_current": 2_000_000_001}, [PRICE_OUT_OF_RANGE]),
    ({"event_id": "", "platform": "shopee"}, [MISSING_EVENT_ID, UNKNOWN_PLATFORM]),
])
def test_errors_go_to_quarantine(change, expected):
    ev = event("fptshop", "listing_discount") | change
    errors, _ = validate(ev)
    assert errors == expected
    assert normalize_price(ev) == (None, expected)


def test_price_range_bounds_are_inclusive():
    ev = event("fptshop", "listing_discount")
    assert validate(ev | {"price_current": 1_000})[0] == []
    assert validate(ev | {"price_current": 2_000_000_000})[0] == []


def test_missing_keys_do_not_raise():
    # Bản ghi hỏng nặng (thiếu gần hết trường) vẫn phải ra lỗi, không được raise
    errors, flags = validate({"event_type": "price"})
    assert errors == [MISSING_EVENT_ID, MISSING_PRODUCT_ID, BAD_TIMESTAMP, UNKNOWN_PLATFORM]
    assert flags == [MISSING_PRICE]
    assert normalize_price({})[0] is None


def test_lazada_is_a_known_platform():
    ev = event("tiki", "listing_discount") | {"platform": "lazada"}
    assert validate(ev) == ([], [])


# ---------------------------------------------------------------------------
# normalize_product
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("platform", PLATFORMS)
def test_real_product_events(platform):
    ev = event(platform, "product")
    assert validate(ev) == ([], [])
    row, errors = normalize_product(ev)
    assert errors == []
    assert row["product_key"] == f"{platform}:{ev['product_id']}"
    assert row["event_time"] == to_utc(ev["event_time"])
    assert json.loads(row["specs_raw"]) == ev["specs_raw"]
    for col in ("event_id", "brand_raw", "category_raw", "image_url", "rating_avg", "rating_5_count"):
        assert row[col] == ev.get(col), col


def test_product_specs_json_is_stable():
    # Cùng specs nhưng thứ tự key khác vẫn ra cùng một chuỗi
    ev = event("tiki", "product")
    reordered = ev | {"specs_raw": dict(reversed(list(ev["specs_raw"].items())))}
    assert normalize_product(ev)[0]["specs_raw"] == normalize_product(reordered)[0]["specs_raw"]
    assert normalize_product(ev | {"specs_raw": {}})[0]["specs_raw"] is None


def test_product_errors():
    ev = event("tiki", "product") | {"product_id": ""}
    assert normalize_product(ev) == (None, [MISSING_PRODUCT_ID])
