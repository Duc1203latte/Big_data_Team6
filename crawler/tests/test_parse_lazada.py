"""Test Lazada: parse trên fixture tự tạo (offline) và phát hiện chặn với HTTP giả."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.http import PlatformBlocked
from common.schema import PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.lazada import LazadaPlatform, is_block_payload

FIXTURE = Path(__file__).parent / "fixtures" / "lazada" / "listing_derived.json"


@pytest.fixture
def raw() -> RawResponse:
    return RawResponse.model_validate_json(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def lazada() -> LazadaPlatform:
    return LazadaPlatform(http=None, crawler_version="0.1.0")


def prices(events) -> dict[str, PriceEvent]:
    return {e.product_id: e for e in events if isinstance(e, PriceEvent)}


def products(events) -> dict[str, ProductEvent]:
    return {e.product_id: e for e in events if isinstance(e, ProductEvent)}


def test_item_without_name_is_skipped_others_kept(lazada, raw):
    events = lazada.parse(raw)
    assert set(prices(events)) == {"1001", "1002", "1003"}
    assert set(products(events)) == {"1001", "1002", "1003"}
    assert len({e.event_id for e in events}) == 6


def test_normal_item(lazada, raw):
    item = prices(lazada.parse(raw))["1001"]
    assert item.source == "listing"
    assert item.sku_id == "2001"
    assert item.url == "https://www.lazada.vn/products/pdp-i1001.html"
    assert (item.price_current, item.price_current_raw) == (250000, "250000")
    assert (item.price_original, item.price_original_raw) == (399000, "₫399,000")
    assert item.in_stock is True
    assert (item.seller_id, item.seller_name) == ("900", "Shop Mau")
    assert (item.rating_avg, item.rating_count, item.review_count) == (4.5, 12, None)
    assert (item.sold_count, item.sold_count_raw) == (5300, "5.3K sold")


def test_out_of_stock_keeps_raw_name_and_vietnamese_formats(lazada, raw):
    item = prices(lazada.parse(raw))["1002"]
    assert item.in_stock is False
    assert item.name_raw == "&lt; Chinh hang &gt; Cap sac"     # crawler không làm sạch tên
    assert item.price_original == 3328000
    assert item.sold_count == 4200


def test_missing_fields_become_none(lazada, raw):
    item = prices(lazada.parse(raw))["1003"]
    assert item.price_current is None and item.price_current_raw is None
    assert item.price_original is None and item.price_original_raw is None
    assert item.rating_avg is None and item.rating_count is None
    assert item.sold_count is None and item.seller_name is None


def test_product_event_has_brand_and_category(lazada, raw):
    item = products(lazada.parse(raw))["1001"]
    assert item.brand_raw == "No Brand"
    assert item.category_raw == "pin-sac-du-phong"
    assert item.specs_raw == {"categories": [1, 2, 3], "location": "Overseas", "brandId": "1"}
    assert item.rating_1_count is None


def test_parse_is_deterministic(lazada, raw):
    assert [e.event_id for e in lazada.parse(raw)] == [e.event_id for e in lazada.parse(raw)]


@pytest.mark.parametrize("body", ["<html>x</html>", "{}", '{"mods":{}}', '{"mods":{"listItems":[]}}'])
def test_unexpected_body_gives_no_events(lazada, raw, body):
    assert lazada.parse(raw.model_copy(update={"body": body})) == []


def test_non_200_gives_no_events(lazada, raw):
    assert lazada.parse(raw.model_copy(update={"status_code": 503})) == []


def test_detail_is_not_supported(lazada, raw):
    ref = raw.ref.model_copy(update={"kind": "detail"})
    assert lazada.parse(raw.model_copy(update={"ref": ref})) == []


@pytest.mark.parametrize("body, final_url, blocked", [
    ('{"ret":["FAIL_SYS_USER_VALIDATE"],"data":{}}', "https://www.lazada.vn/x", True),
    ('{"rgv587_flag":"sm","url":"https://www.lazada.vn/x/_____tmd_____/punish"}', "https://www.lazada.vn/x", True),
    ("{}", "https://www.lazada.vn/x/_____tmd_____/punish?x=1", True),
    ('{"mods":{"listItems":[{"name":"Ao captcha"}]}}', "https://www.lazada.vn/x", False),
    ("{}", "https://www.lazada.vn/x", False),
    ("<html>", "https://www.lazada.vn/x", False),
])
def test_is_block_payload(body, final_url, blocked):
    assert is_block_payload(body, final_url) is blocked


class FakeHttp:
    def __init__(self, body: str):
        self.body = body
        self.urls: list[str] = []

    async def get(self, url, *, params=None):
        self.urls.append(url)
        return SimpleNamespace(text=self.body, url=url, status_code=200,
                               headers={"content-type": "application/json"})


@pytest.mark.asyncio
async def test_discover_yields_one_page_per_category():
    platform = LazadaPlatform(http=FakeHttp("{}"), crawler_version="0.1.0")
    refs = [ref async for ref in platform.discover("pin-sac-du-phong")]
    assert [ref.url for ref in refs] == ["https://www.lazada.vn/pin-sac-du-phong/?ajax=true&page=1"]


@pytest.mark.asyncio
async def test_fetch_stops_platform_on_captcha_json(tmp_path, monkeypatch):
    monkeypatch.setattr("common.http.BLOCK_SAMPLE_DIR", tmp_path)
    http = FakeHttp(json.dumps({"ret": ["FAIL_SYS_USER_VALIDATE"]}))
    platform = LazadaPlatform(http=http, crawler_version="0.1.0")
    ref = ProductRef(platform="lazada", kind="listing", url="https://www.lazada.vn/a/?ajax=true&page=1")
    with pytest.raises(PlatformBlocked):
        await platform.fetch(ref)
    assert "FAIL_SYS_USER_VALIDATE" in next(tmp_path.iterdir()).read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_fetch_returns_raw_response():
    http = FakeHttp(json.dumps({"mods": {"listItems": []}}))
    platform = LazadaPlatform(http=http, crawler_version="0.1.0")
    ref = ProductRef(platform="lazada", kind="listing", url="https://www.lazada.vn/a/?ajax=true&page=1")
    raw = await platform.fetch(ref)
    assert raw.status_code == 200 and raw.kind == "listing"
