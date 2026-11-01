"""Test: parse Thế Giới Di Động trên fixture (offline) và hàm đọc số lượng dạng chữ."""

from pathlib import Path

import pytest

from common.schema import PriceEvent, ProductEvent, RawResponse
from platforms.parsing import parse_count_text
from platforms.tgdd import TgddPlatform

FIXTURES = Path(__file__).parent / "fixtures" / "tgdd"


def load(name: str) -> RawResponse:
    return RawResponse.model_validate_json((FIXTURES / name).read_text(encoding="utf-8"))


def split(events):
    products = [e for e in events if isinstance(e, ProductEvent)]
    return (products[0] if products else None), [e for e in events if isinstance(e, PriceEvent)]


@pytest.fixture
def tgdd():
    return TgddPlatform(http=None, crawler_version="0.1.0")


@pytest.mark.parametrize("text, expected", [
    ("Đã bán 60,2k", 60_200),
    ("Đã bán 1,5k", 1_500),
    ("Đã bán 726", 726),
    ("Đã bán 1.234", 1_234),        # không có hậu tố: dấu chấm là phân cách hàng nghìn
    ("5k+", 5_000),
    ("1,2tr", 1_200_000),
    ("Chưa có lượt bán", None),
    (None, None),
])
def test_parse_count_text(text, expected):
    assert parse_count_text(text) == expected


def test_listing(tgdd):
    raw = load("listing_dtdd.json")
    events = tgdd.parse(raw)
    assert len(events) == 20                                 # không lấy mảng "products" giả của menu
    first = events[0]
    assert first.product_id == "oppo-reno16f-5g-8gb-128gb"
    assert first.sku_id == "0131491005325"
    assert first.url == "https://www.thegioididong.com/dtdd/oppo-reno16f-5g-8gb-128gb"
    assert first.price_current == 14_380_000 and first.price_original == 14_990_000
    assert first.in_stock is True
    assert first.rating_avg == 4.9 and first.rating_count == 60_219
    assert first.sold_count == 60_200 and first.sold_count_raw == "Đã bán 60,2k"
    assert first.review_count is None and first.seller_id is None
    assert first.event_time == raw.fetched_at.replace(microsecond=0)


def test_detail_product_group_one_price_per_variant(tgdd):
    product, prices = split(tgdd.parse(load("detail_normal.json")))
    assert product.product_id == "oppo-reno16f-5g-8gb-128gb"
    assert product.brand_raw == "OPPO"
    assert product.rating_count == 60_212
    assert product.specs_raw["RAM"] == "8 GB"
    assert product.category_raw.endswith("Điện thoại OPPO")
    assert product.image_url.startswith("https://cdnv2.tgdd.vn/")
    assert len(prices) == 3                                  # 3 màu; phần tử rỗng bị bỏ
    assert {p.sku_id for p in prices} == {"0131491005325", "0131491005326", "0131491005327"}
    assert all(p.price_current == 14_380_000 and p.in_stock is True for p in prices)
    assert len({p.event_id for p in prices}) == 3            # khác sku -> khác event_id


def test_detail_out_of_stock(tgdd):
    _, prices = split(tgdd.parse(load("detail_out_of_stock_derived.json")))
    assert prices and all(p.in_stock is False for p in prices)


def test_detail_missing_fields(tgdd):
    product, prices = split(tgdd.parse(load("detail_missing_fields_derived.json")))
    assert product.brand_raw is None and product.rating_avg is None and product.specs_raw == {}
    assert prices and all(p.price_current is None for p in prices)


def test_detail_single_product_without_variants(tgdd):
    product, prices = split(tgdd.parse(load("detail_single_product_derived.json")))
    assert product.brand_raw == "OPPO"
    assert len(prices) == 1 and prices[0].price_current == 14_380_000


def test_errors_return_nothing(tgdd):
    raw = load("listing_dtdd.json")
    assert tgdd.parse(raw.model_copy(update={"status_code": 403})) == []
    assert tgdd.parse(raw.model_copy(update={"body": "<html></html>"})) == []
    detail = load("detail_normal.json")
    assert tgdd.parse(detail.model_copy(update={"body": "<html></html>"})) == []
