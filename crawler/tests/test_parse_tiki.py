"""Test bước 4: parse Tiki trên fixture thật (offline) và fetch với HTTP giả (không gọi mạng)."""

import json
from pathlib import Path

import httpx
import pytest

from common.schema import PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.tiki import TikiPlatform

FIXTURES = Path(__file__).parent / "fixtures" / "tiki"


def load(name: str) -> RawResponse:
    return RawResponse.model_validate_json((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def tiki():
    # parse() không dùng mạng nên http=None là đủ
    return TikiPlatform(http=None, crawler_version="0.1.0")


def split(events):
    """Tách list event thành (ProductEvent | None, list PriceEvent)."""
    products = [e for e in events if isinstance(e, ProductEvent)]
    prices = [e for e in events if isinstance(e, PriceEvent)]
    return (products[0] if products else None), prices


# ------------------------------ listing ------------------------------

def test_listing_gives_40_price_events(tiki):
    events = tiki.parse(load("listing_c1789_p1.json"))
    assert len(events) == 40
    assert all(isinstance(e, PriceEvent) and e.source == "listing" for e in events)
    assert len({e.event_id for e in events}) == 40


def test_listing_first_item_fields(tiki):
    raw = load("listing_c1789_p1.json")
    e = tiki.parse(raw)[0]
    assert e.platform == "tiki"
    assert e.product_id == "278628866"
    assert e.sku_id == "278630782"
    assert e.url.startswith("https://tiki.vn/apple-iphone-17-pro-max-p278628866.html")
    assert e.name_raw == "Apple iPhone 17 Pro Max"
    assert e.price_current == 33_350_000
    assert e.price_current_raw == "33350000"
    assert e.price_original == 33_350_000
    assert e.seller_id == "1"
    assert e.seller_name is None            # danh sách không có tên shop
    assert e.in_stock is None               # danh sách không có tồn kho
    assert e.rating_avg == 5.0
    assert e.rating_count == e.review_count == 75
    assert e.sold_count == 6285
    assert e.sold_count_raw == "Đã bán 6285"
    assert e.event_time == raw.fetched_at.replace(microsecond=0)
    assert e.crawler_version == "0.1.0"


def test_listing_zero_rating_becomes_none(tiki):
    events = tiki.parse(load("listing_c1789_p1.json"))
    no_rating = [e for e in events if e.review_count == 0]
    assert no_rating, "fixture phải có sản phẩm chưa ai đánh giá"
    assert all(e.rating_avg is None for e in no_rating)


def test_listing_missing_sold_count_is_none(tiki):
    events = tiki.parse(load("listing_c1789_p1.json"))
    assert sum(1 for e in events if e.sold_count is None) == 15


def test_listing_shares_raw_hash(tiki):
    events = tiki.parse(load("listing_c1789_p1.json"))
    assert len({e.raw_hash for e in events}) == 1


def test_listing_broken_item_skipped_others_kept(tiki):
    raw = load("listing_c1789_p1.json")
    payload = json.loads(raw.body)
    del payload["data"][0]["id"]             # sản phẩm đầu mất id -> không tạo được event
    payload["data"][1]["price"] = "abc"      # sản phẩm thứ hai giá hỏng -> vẫn giữ, giá None
    broken = raw.model_copy(update={"body": json.dumps(payload, ensure_ascii=False)})

    events = tiki.parse(broken)
    assert len(events) == 39
    assert events[0].price_current is None
    assert events[0].price_current_raw == "abc"


# ------------------------------ detail ------------------------------

def test_detail_normal(tiki):
    product, prices = split(tiki.parse(load("detail_normal.json")))
    assert product.product_id == "278628866"
    assert product.brand_raw == "Apple"
    assert product.category_raw == "Điện Thoại - Máy Tính Bảng > Điện thoại Smartphone"
    assert product.specs_raw["origin"] == "Trung Quốc"
    assert product.image_url.startswith("https://salt.tikicdn.com/")
    assert product.rating_avg == 4.9                     # lấy từ API review
    assert product.rating_count == 75
    assert (product.rating_1_count, product.rating_2_count, product.rating_3_count,
            product.rating_4_count, product.rating_5_count) == (1, 0, 0, 1, 73)

    assert len(prices) == 1
    price = prices[0]
    assert price.source == "detail"
    assert price.sku_id == "278630782"
    assert price.seller_id == "1"
    assert price.seller_name == "Tiki Trading"
    assert price.in_stock is True
    assert price.stock_status_raw == "available"
    assert price.price_current == 33_350_000


def test_detail_out_of_stock(tiki):
    _, prices = split(tiki.parse(load("detail_out_of_stock_derived.json")))
    assert prices[0].in_stock is False
    assert prices[0].stock_status_raw == "out_of_stock"


def test_detail_missing_fields(tiki):
    product, prices = split(tiki.parse(load("detail_missing_fields.json")))
    assert product.rating_avg is None
    assert product.rating_5_count is None               # không có review summary
    assert product.specs_raw                             # vẫn có thông số
    assert prices[0].sold_count is None
    assert prices[0].price_current == 2_890_000
    assert prices[0].seller_name == "GOTECHSOFT CO LTD"


def test_detail_no_price_still_emits_events(tiki):
    product, prices = split(tiki.parse(load("detail_no_price_derived.json")))
    assert product is not None
    assert prices[0].price_current is None
    assert prices[0].price_current_raw is None


# ------------------------------ lỗi và tính thuần ------------------------------

def test_parse_is_deterministic(tiki):
    raw = load("listing_c1789_p1.json")
    assert tiki.parse(raw) == tiki.parse(raw)


def test_non_200_returns_no_events(tiki):
    raw = load("listing_c1789_p1.json").model_copy(update={"status_code": 429})
    assert tiki.parse(raw) == []


def test_non_json_body_returns_no_events(tiki):
    raw = load("listing_c1789_p1.json").model_copy(update={"body": "<html>blocked</html>"})
    assert tiki.parse(raw) == []


# ------------------------------ fetch (HTTP giả) ------------------------------

class FakeHttp:
    """Thay cho httpx.AsyncClient: trả response dựng sẵn, ghi lại các URL đã gọi."""

    def __init__(self, responses: dict[str, httpx.Response]):
        self.responses = responses
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, url, *, params=None):
        self.calls.append((url, params))
        return self.responses[url]


@pytest.mark.asyncio
async def test_fetch_listing_uses_ref_url():
    url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category=1789&page=1"
    http = FakeHttp({url: httpx.Response(200, text='{"data": []}')})
    tiki = TikiPlatform(http=http, crawler_version="0.1.0")

    raw = await tiki.fetch(ProductRef(platform="tiki", kind="listing", url=url))
    assert raw.status_code == 200
    assert raw.body == '{"data": []}'
    assert raw.fetched_at.tzinfo is not None
    assert http.calls == [(url, None)]


@pytest.mark.asyncio
async def test_fetch_detail_combines_product_and_reviews():
    product = {"id": 1, "name": "P", "current_seller": {"id": 9, "product_id": "2"}}
    reviews = {"stars": {"5": {"count": 3}}, "data": [{"created_by": {"full_name": "Nguyen Van A"}}]}
    http = FakeHttp({
        "https://tiki.vn/api/v2/products/1": httpx.Response(200, json=product),
        "https://tiki.vn/api/v2/reviews": httpx.Response(200, json=reviews),
    })
    tiki = TikiPlatform(http=http, crawler_version="0.1.0")
    ref = ProductRef(platform="tiki", kind="detail", url="https://tiki.vn/p-p1.html?spid=2", product_id="1")

    raw = await tiki.fetch(ref)
    body = json.loads(raw.body)
    assert body["product"] == product
    assert body["reviews"]["stars"] == {"5": {"count": 3}}
    assert body["reviews"]["data"] == []                  # đã bỏ dữ liệu người mua
    assert http.calls[0] == ("https://tiki.vn/api/v2/products/1", {"spid": "2"})
    assert http.calls[1][1] == {"product_id": "1", "limit": 1, "page": 1, "spid": "2", "seller_id": 9}


@pytest.mark.asyncio
async def test_fetch_detail_reviews_failure_keeps_product():
    product = {"id": 1, "name": "P"}
    http = FakeHttp({
        "https://tiki.vn/api/v2/products/1": httpx.Response(200, json=product),
        "https://tiki.vn/api/v2/reviews": httpx.Response(500, text="error"),
    })
    tiki = TikiPlatform(http=http, crawler_version="0.1.0")
    ref = ProductRef(platform="tiki", kind="detail", url="https://tiki.vn/p-p1.html", product_id="1")

    body = json.loads((await tiki.fetch(ref)).body)
    assert body == {"product": product, "reviews": None}


@pytest.mark.asyncio
async def test_fetch_detail_http_error_returns_raw_status():
    http = FakeHttp({"https://tiki.vn/api/v2/products/1": httpx.Response(403, text="forbidden")})
    tiki = TikiPlatform(http=http, crawler_version="0.1.0")
    ref = ProductRef(platform="tiki", kind="detail", url="https://tiki.vn/p-p1.html", product_id="1")

    raw = await tiki.fetch(ref)
    assert raw.status_code == 403
    assert tiki.parse(raw) == []
