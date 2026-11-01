"""Test bước 1: schema và sinh ID. Chạy hoàn toàn offline."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import TypeAdapter, ValidationError

from common.ids import make_event_id, make_product_key, make_raw_hash
from common.schema import Event, PriceEvent, ProductEvent

VN_TZ = timezone(timedelta(hours=7))
BASE_TIME = datetime(2026, 10, 25, 15, 0, 5, tzinfo=timezone.utc)


def price_kwargs(**overrides):
    """Bộ trường tối thiểu để tạo một PriceEvent hợp lệ; ghi đè từng trường khi cần."""
    data = dict(
        platform="tiki",
        product_id="278476178",
        sku_id="278476179",
        url="https://tiki.vn/p/278476178",
        name_raw="Điện thoại iPhone 15 128GB",
        source="listing",
        price_current=19_990_000,
        price_current_raw="19.990.000đ",
        event_time=BASE_TIME,
        crawler_version="0.1.0",
        raw_hash=make_raw_hash("{}"),
    )
    data.update(overrides)
    return data


def test_same_input_same_event_id():
    assert PriceEvent(**price_kwargs()).event_id == PriceEvent(**price_kwargs()).event_id


def test_one_second_later_different_event_id():
    a = PriceEvent(**price_kwargs())
    b = PriceEvent(**price_kwargs(event_time=BASE_TIME + timedelta(seconds=1)))
    assert a.event_id != b.event_id


def test_different_sku_different_event_id():
    a = PriceEvent(**price_kwargs(sku_id="A"))
    b = PriceEvent(**price_kwargs(sku_id="B"))
    assert a.event_id != b.event_id


def test_microseconds_are_truncated():
    # Cùng một giây, khác microsecond -> cùng event_id, event_time không còn microsecond
    a = PriceEvent(**price_kwargs())
    b = PriceEvent(**price_kwargs(event_time=BASE_TIME + timedelta(microseconds=999_999)))
    assert a.event_id == b.event_id
    assert b.event_time.microsecond == 0


def test_timezone_is_normalized_to_utc():
    # 22:00:05 giờ VN chính là 15:00:05 UTC -> cùng event_id
    vn_time = datetime(2026, 10, 25, 22, 0, 5, tzinfo=VN_TZ)
    a = PriceEvent(**price_kwargs())
    b = PriceEvent(**price_kwargs(event_time=vn_time))
    assert a.event_id == b.event_id
    assert b.event_time.utcoffset() == timedelta(0)


def test_naive_datetime_rejected():
    with pytest.raises(ValidationError):
        PriceEvent(**price_kwargs(event_time=datetime(2026, 10, 25, 15, 0, 5)))


def test_price_and_product_events_have_different_ids():
    price = PriceEvent(**price_kwargs(sku_id=None))
    product = ProductEvent(**{k: v for k, v in price_kwargs().items()
                              if k not in ("sku_id", "source", "price_current", "price_current_raw")})
    assert price.event_id != product.event_id


def test_missing_fields_are_allowed_as_none():
    event = PriceEvent(**price_kwargs(price_current=None, price_current_raw=None))
    assert event.price_current is None
    assert event.in_stock is None


def test_event_id_matches_helper():
    event = PriceEvent(**price_kwargs())
    expected = make_event_id("tiki", "278476178", "278476179", "price", BASE_TIME)
    assert event.event_id == expected


def test_existing_event_id_is_kept():
    event = PriceEvent(**price_kwargs(event_id="given-id"))
    assert event.event_id == "given-id"


def test_json_round_trip():
    # Ghi ra JSON rồi đọc lại qua kiểu Event chung: phải ra đúng class, đúng dữ liệu
    original = PriceEvent(**price_kwargs())
    restored = TypeAdapter(Event).validate_json(original.model_dump_json())
    assert isinstance(restored, PriceEvent)
    assert restored == original


def test_unknown_field_rejected():
    with pytest.raises(ValidationError):
        PriceEvent(**price_kwargs(discount_pct=20.0))


def test_product_key():
    assert make_product_key("tiki", "278476178") == "tiki:278476178"
