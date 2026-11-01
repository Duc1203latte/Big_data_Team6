"""Test bước 9: parse FPT Shop, discover của từng sàn, cấu hình crawl_full. Offline."""

import json
from pathlib import Path

import httpx
import pytest
from bs4 import BeautifulSoup

from common.config import PlatformLimits
from common.schema import PriceEvent, ProductEvent, RawResponse
from platforms import PLATFORM_CLASSES
from platforms.cellphones import CellphonesPlatform
from platforms.fptshop import FptshopPlatform, flight_text, main_product_list
from platforms.hoangha import HoanghaPlatform, listing_url, page_count
from platforms.tiki import TikiPlatform

FIXTURES = Path(__file__).parent / "fixtures"


def load(platform: str, name: str) -> RawResponse:
    return RawResponse.model_validate_json((FIXTURES / platform / name).read_text(encoding="utf-8"))


def split(events):
    products = [e for e in events if isinstance(e, ProductEvent)]
    return (products[0] if products else None), [e for e in events if isinstance(e, PriceEvent)]


@pytest.fixture
def fpt():
    return FptshopPlatform(http=None, crawler_version="0.1.0")


# ------------------------------ FPT Shop ------------------------------

def test_flight_text_joins_chunks():
    html = ('<script>self.__next_f.push([1,"{\\"a\\":"])</script>'
            '<script>self.__next_f.push([1,"1}"])</script>'
            '<script>self.__next_f.push([0])</script>')
    assert flight_text(html) == '{"a":1}'


def test_main_product_list_picks_longest():
    flight = ('{"side":{"items":[{"currentPrice":1,"slug":"a"}]},'
              '"main":{"items":[{"currentPrice":2,"slug":"b"},{"currentPrice":3,"slug":"c"}]},'
              '"menu":{"items":[{"name":"x"}]}}')
    assert [x["slug"] for x in main_product_list(flight)] == ["b", "c"]


def test_fpt_listing(fpt):
    raw = load("fptshop", "listing_dien_thoai.json")
    events = fpt.parse(raw)
    assert len(events) == 24                                  # danh sách chính, không lấy khối "Bán chạy"
    first = events[0]
    assert first.product_id == "iphone-duo"
    assert first.sku_id == "893493813588"
    assert first.url == "https://fptshop.com.vn/dien-thoai/iphone-duo"
    assert first.name_raw == "iPhone Duo"
    assert first.price_current == 103_990_000 and first.price_original == 103_990_000
    assert first.rating_avg == 5.0
    assert first.sold_count == 726
    assert json.loads(first.sold_count_raw)["realQuantity"] == 726
    assert first.event_time == raw.fetched_at.replace(microsecond=0)
    assert all(e.seller_id is None for e in events)


def test_fpt_listing_sold_counts_and_stock(fpt):
    events = fpt.parse(load("fptshop", "listing_dien_thoai.json"))
    assert all(e.sold_count is not None for e in events)
    assert all(e.in_stock in (True, False, None) for e in events)
    assert any(e.in_stock is not None for e in events)


def test_fpt_detail_normal(fpt):
    product, prices = split(fpt.parse(load("fptshop", "detail_normal.json")))
    assert product.product_id == "iphone-duo"
    assert product.brand_raw == "Apple"
    assert product.category_raw == "Điện thoại > Apple (iPhone)"
    assert product.specs_raw["Bộ nhớ trong"] == "256 GB"
    assert product.rating_avg == 4.9 and product.rating_count == 58
    price = prices[0]
    assert price.sku_id == "00930750"                         # biến thể khác với danh sách
    assert price.price_current == 64_990_000
    assert price.in_stock is True


def test_fpt_detail_out_of_stock_and_missing(fpt):
    _, prices = split(fpt.parse(load("fptshop", "detail_out_of_stock_derived.json")))
    assert prices[0].in_stock is False
    product, prices = split(fpt.parse(load("fptshop", "detail_missing_fields_derived.json")))
    assert product.brand_raw is None and product.specs_raw == {} and product.rating_avg is None
    assert prices[0].price_current is None


def test_fpt_page_without_data(fpt):
    raw = load("fptshop", "listing_dien_thoai.json").model_copy(update={"body": "<html></html>"})
    assert fpt.parse(raw) == []
    assert fpt.parse(raw.model_copy(update={"status_code": 500})) == []


def test_registry_has_all_platforms():
    assert set(PLATFORM_CLASSES) == {"tiki", "cellphones", "hoangha", "fptshop", "tgdd"}


# ------------------------------ discover ------------------------------

class FakeHttp:
    def __init__(self, body: str, status: int = 200):
        self.body, self.status = body, status
        self.urls: list[str] = []

    async def get(self, url, *, params=None):
        self.urls.append(url)
        return httpx.Response(self.status, text=self.body)


async def collect(agen):
    return [ref async for ref in agen]


def test_hoangha_page_count_from_fixture():
    body = load("hoangha", "listing_dien_thoai_p1.json").body
    assert page_count(BeautifulSoup(body, "html.parser")) == 11     # 18 thẻ + "Xem thêm 172 sản phẩm"
    assert page_count(BeautifulSoup("<html></html>", "html.parser")) == 0


def test_hoangha_listing_url():
    assert listing_url("laptop", 1) == "https://hoanghamobile.com/laptop"
    assert listing_url("laptop", 3) == "https://hoanghamobile.com/laptop?p=3"


@pytest.mark.asyncio
async def test_hoangha_discover_yields_all_pages():
    http = FakeHttp(load("hoangha", "listing_dien_thoai_p1.json").body)
    refs = await collect(HoanghaPlatform(http=http, crawler_version="0.1.0").discover("dien-thoai-di-dong"))
    assert [r.page for r in refs] == list(range(1, 12))
    assert refs[1].url == "https://hoanghamobile.com/dien-thoai-di-dong?p=2"
    assert http.urls == ["https://hoanghamobile.com/dien-thoai-di-dong"]     # chỉ gọi trang 1 để đếm


@pytest.mark.asyncio
async def test_hoangha_discover_http_error_yields_nothing():
    http = FakeHttp("error", status=503)
    assert await collect(HoanghaPlatform(http=http, crawler_version="0.1.0").discover("laptop")) == []


@pytest.mark.asyncio
async def test_tiki_discover_reads_last_page():
    http = FakeHttp(load("tiki", "listing_c1789_p1.json").body)
    refs = await collect(TikiPlatform(http=http, crawler_version="0.1.0").discover("1789"))
    assert [r.page for r in refs] == [1, 2, 3]
    assert refs[2].url.endswith("category=1789&page=3")


@pytest.mark.asyncio
async def test_tiki_discover_blocked_yields_nothing():
    http = FakeHttp("<html>challenge</html>")
    assert await collect(TikiPlatform(http=http, crawler_version="0.1.0").discover("1789")) == []


@pytest.mark.asyncio
async def test_single_page_platforms_discover_without_network():
    http = FakeHttp("")
    cp = await collect(CellphonesPlatform(http=http, crawler_version="0.1.0").discover("mobile/apple"))
    fp = await collect(FptshopPlatform(http=http, crawler_version="0.1.0").discover("dien-thoai/samsung"))
    assert [r.url for r in cp] == ["https://cellphones.com.vn/mobile/apple.html"]
    assert [r.url for r in fp] == ["https://fptshop.com.vn/dien-thoai/samsung"]
    assert http.urls == []


# ------------------------------ cấu hình ------------------------------

def test_full_crawl_rate():
    explicit = PlatformLimits(requests_per_second=0.2, full_requests_per_second=0.05)
    default = PlatformLimits(requests_per_second=0.2)
    assert explicit.for_full_crawl().requests_per_second == 0.05
    assert default.for_full_crawl().requests_per_second == 0.1
    assert explicit.for_full_crawl().max_details_per_run == explicit.max_details_per_run
