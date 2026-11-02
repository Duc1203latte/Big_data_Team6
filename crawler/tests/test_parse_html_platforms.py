"""Test bước 8: parse CellphoneS và Hoàng Hà trên fixture (offline), cùng các hàm đọc HTML dùng chung."""

from pathlib import Path

import httpx
import pytest
from bs4 import BeautifulSoup

from common.schema import PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.cellphones import CellphonesPlatform
from platforms.hoangha import HoanghaPlatform, page_count
from platforms.parsing import availability_to_bool, json_ld, parse_vnd, slug_from_url

FIXTURES = Path(__file__).parent / "fixtures"


def load(platform: str, name: str) -> RawResponse:
    return RawResponse.model_validate_json((FIXTURES / platform / name).read_text(encoding="utf-8"))


def split(events):
    products = [e for e in events if isinstance(e, ProductEvent)]
    prices = [e for e in events if isinstance(e, PriceEvent)]
    return (products[0] if products else None), prices


@pytest.fixture
def cellphones():
    return CellphonesPlatform(http=None, crawler_version="0.1.0")


@pytest.fixture
def hoangha():
    return HoanghaPlatform(http=None, crawler_version="0.1.0")


# ------------------------------ hàm dùng chung ------------------------------

@pytest.mark.parametrize("text, expected", [
    ("30.990.000đ", 30_990_000),
    ("41,490,000 ₫", 41_490_000),
    ("38690000", 38_690_000),
    ("  5.990.000 đ ", 5_990_000),
    ("Liên hệ", None),
    ("1.2tr", None),            # dạng viết tắt: không đoán, để Silver xử lý từ chữ gốc
    ("", None),
    (None, None),
])
def test_parse_vnd(text, expected):
    assert parse_vnd(text) == expected


def test_slug_from_url():
    assert slug_from_url("https://cellphones.com.vn/iphone-18-pro.html") == "iphone-18-pro"
    assert slug_from_url("https://hoanghamobile.com/dien-thoai/iphone-18-pro-max") == "iphone-18-pro-max"
    assert slug_from_url("https://hoanghamobile.com/dien-thoai/x?p=2") == "x"
    assert slug_from_url(None) is None


def test_availability_to_bool():
    assert availability_to_bool("https://schema.org/InStock") is True
    assert availability_to_bool("https://schema.org/OutOfStock") is False
    assert availability_to_bool("https://schema.org/PreOrder") is None
    assert availability_to_bool(None) is None


def test_json_ld_skips_broken_blocks():
    html = ('<script type="application/ld+json">{broken</script>'
            '<script type="application/ld+json">[{"@type": "Product", "name": "P"}]</script>')
    assert json_ld(BeautifulSoup(html, "html.parser"), "Product") == {"@type": "Product", "name": "P"}
    assert json_ld(BeautifulSoup(html, "html.parser"), "BreadcrumbList") == {}


# ------------------------------ CellphoneS ------------------------------

def test_cellphones_listing(cellphones):
    raw = load("cellphones", "listing_mobile.json")
    events = cellphones.parse(raw)
    assert len(events) == 20                                  # chỉ lưới chính, không lấy ô nổi bật
    assert len({e.event_id for e in events}) == 20
    first = events[0]
    assert first.product_id == "iphone-18-pro"
    assert first.url == "https://cellphones.com.vn/iphone-18-pro.html"
    assert first.name_raw == "iPhone 18 Pro 256GB"
    assert first.price_current == 38_690_000
    assert first.price_current_raw == "38.690.000đ"
    assert first.price_original == 38_990_000
    assert first.rating_avg == 5.0
    assert first.sku_id is None and first.seller_id is None and first.in_stock is None
    assert first.event_time == raw.fetched_at.replace(microsecond=0)


def test_cellphones_listing_missing_values_are_none(cellphones):
    events = cellphones.parse(load("cellphones", "listing_mobile.json"))
    assert any(e.price_original is None for e in events)     # sản phẩm không giảm giá
    assert any(e.rating_avg is None for e in events)         # sản phẩm chưa có đánh giá


def test_cellphones_detail_normal(cellphones):
    product, prices = split(cellphones.parse(load("cellphones", "detail_normal.json")))
    assert product.product_id == "iphone-18-pro"
    assert product.brand_raw == "Apple"
    assert product.category_raw == "Điện thoại > Apple > iPhone 18 Series"
    assert product.specs_raw["Chipset"] == "A20 Pro"
    assert product.rating_avg == 5.0 and product.rating_count == 4
    assert product.rating_5_count is None                    # CellphoneS không công bố phân bố sao
    price = prices[0]
    assert price.source == "detail"
    assert price.price_current == 38_690_000
    assert price.in_stock is True
    assert price.stock_status_raw == "https://schema.org/InStock"


def test_cellphones_detail_out_of_stock(cellphones):
    _, prices = split(cellphones.parse(load("cellphones", "detail_out_of_stock_derived.json")))
    assert prices[0].in_stock is False


def test_cellphones_detail_missing_fields(cellphones):
    product, prices = split(cellphones.parse(load("cellphones", "detail_missing_fields_derived.json")))
    assert product.brand_raw is None and product.rating_avg is None and product.specs_raw == {}
    assert prices[0].price_current is None and prices[0].price_current_raw is None


# ------------------------------ Hoàng Hà ------------------------------

def test_hoangha_listing(hoangha):
    raw = load("hoangha", "listing_dien_thoai_p1.json")
    events = hoangha.parse(raw)
    assert len(events) == 18
    first = events[0]
    assert first.product_id == "iphone-18-pro-max"
    assert first.sku_id == "MJXN4X"
    assert first.url == "https://hoanghamobile.com/dien-thoai/iphone-18-pro-max"
    assert first.price_current == 41_490_000
    assert first.price_current_raw == "41,490,000 ₫"
    assert first.in_stock is True and first.stock_status_raw == "Sẵn hàng"


def test_hoangha_listing_discount_and_contact_status(hoangha):
    events = hoangha.parse(load("hoangha", "listing_dien_thoai_p1.json"))
    nubia = next(e for e in events if e.product_id == "nubia-air-5g")
    assert nubia.price_current == 5_490_000
    assert nubia.price_original == 5_990_000                 # giá gạch ngang
    contact = [e for e in events if e.stock_status_raw == "Liên hệ"]
    assert contact and all(e.in_stock is None for e in contact)   # "Liên hệ" không đoán còn/hết


def test_hoangha_detail_normal(hoangha):
    product, prices = split(hoangha.parse(load("hoangha", "detail_normal.json")))
    assert product.product_id == "iphone-18-pro-max"
    assert product.brand_raw == "Apple"
    assert product.category_raw == "Điện thoại > Apple > iPhone 18 Series & iPhone Duo"
    assert product.specs_raw["Bộ nhớ trong"] == "256 GB"
    assert len(product.specs_raw) == 26
    assert product.rating_count == 21
    price = prices[0]
    assert price.sku_id == "MJXN4X"
    assert price.price_current == 41_490_000
    assert price.in_stock is True


def test_hoangha_detail_out_of_stock(hoangha):
    _, prices = split(hoangha.parse(load("hoangha", "detail_out_of_stock_derived.json")))
    assert prices[0].in_stock is False


def test_hoangha_detail_missing_fields(hoangha):
    product, prices = split(hoangha.parse(load("hoangha", "detail_missing_fields_derived.json")))
    assert product.specs_raw == {} and product.brand_raw is None and product.rating_avg is None
    assert prices[0].price_current is None


# ------------------------------ lỗi chung ------------------------------

@pytest.mark.parametrize("platform_cls, platform, fixture", [
    (CellphonesPlatform, "cellphones", "listing_mobile.json"),
    (HoanghaPlatform, "hoangha", "listing_dien_thoai_p1.json"),
])
def test_non_200_and_empty_page(platform_cls, platform, fixture):
    p = platform_cls(http=None, crawler_version="0.1.0")
    raw = load(platform, fixture)
    assert p.parse(raw.model_copy(update={"status_code": 503})) == []
    assert p.parse(raw.model_copy(update={"body": "<html><body>maintenance</body></html>"})) == []


def test_detail_without_json_ld_returns_nothing(cellphones, hoangha):
    raw = load("cellphones", "detail_normal.json").model_copy(update={"body": "<html></html>"})
    assert cellphones.parse(raw) == []
    assert hoangha.parse(raw.model_copy(update={"ref": raw.ref.model_copy(update={"platform": "hoangha"})})) == []


def test_listing_card_without_link_is_skipped(hoangha):
    raw = load("hoangha", "listing_dien_thoai_p1.json")
    # Xoá cả link ảnh lẫn link tiêu đề của sản phẩm đầu tiên
    broken = raw.body.replace('href="/dien-thoai/iphone-18-pro-max"', "")
    events = hoangha.parse(raw.model_copy(update={"body": broken}))
    assert len(events) == 17
    assert "iphone-18-pro-max" not in {e.product_id for e in events}


@pytest.mark.asyncio
async def test_fetch_uses_ref_url():
    class FakeHttp:
        def __init__(self):
            self.urls = []

        async def get(self, url, *, params=None):
            self.urls.append(url)
            return httpx.Response(200, html="<html></html>")

    http = FakeHttp()
    for cls in (CellphonesPlatform, HoanghaPlatform):
        ref = ProductRef(platform=cls.name, kind="listing", url=f"https://example.com/{cls.name}")
        raw = await cls(http=http, crawler_version="0.1.0").fetch(ref)
        assert raw.status_code == 200 and raw.fetched_at.tzinfo is not None
    assert http.urls == ["https://example.com/cellphones", "https://example.com/hoangha"]


# ------------------------------ Hoàng Hà giao diện mới (2026-11-02) ------------------------------

def test_hoangha_v2_listing(hoangha):
    raw = load("hoangha", "listing_dien_thoai_p1_v2.json")
    events = hoangha.parse(raw)
    assert len(events) == 20
    first = events[0]
    assert first.product_id == "iphone-18-pro-max"
    assert first.sku_id == "MJXN4X"
    assert first.url == "https://hoanghamobile.com/dien-thoai/iphone-18-pro-max"
    assert first.name_raw == "iPhone 18 Pro Max 256GB"
    assert first.price_current == 41_490_000 and first.price_current_raw == "41.490.000"
    assert first.price_original is None                        # thẻ giá gốc rỗng khi không giảm
    assert first.in_stock is True and first.stock_status_raw == "Sẵn hàng"


def test_hoangha_v2_listing_discount_and_contact(hoangha):
    events = hoangha.parse(load("hoangha", "listing_dien_thoai_p1_v2.json"))
    discounted = [e for e in events if e.price_original is not None]
    assert len(discounted) >= 10
    assert any(e.price_original > e.price_current for e in discounted)
    contact = [e for e in events if e.stock_status_raw == "Liên hệ"]
    assert contact and all(e.in_stock is None for e in contact)


def test_hoangha_v2_page_count():
    body = load("hoangha", "listing_dien_thoai_p1_v2.json").body
    assert page_count(BeautifulSoup(body, "html.parser")) == 10     # 20 thẻ + "Xem thêm 171 sản phẩm"


def test_hoangha_v2_detail(hoangha):
    product, prices = split(hoangha.parse(load("hoangha", "detail_normal_v2.json")))
    assert product.product_id == "iphone-18-pro-max"
    assert product.category_raw == "Điện thoại > Apple > iPhone 18 Series & iPhone Duo"
    # 27 dòng, 26 tên khác nhau (sàn lặp một dòng), dict giữ một bản
    assert len(product.specs_raw) == 26 and product.specs_raw["Bộ nhớ trong"] == "256 GB"
    assert product.rating_avg == 4.6 and product.rating_count == 21
    assert prices[0].sku_id == "MJXN4X" and prices[0].price_current == 41_490_000
    assert prices[0].price_current_raw == "lowPrice=41490000.0;highPrice=41990000.0"   # AggregateOffer
    assert prices[0].in_stock is True
