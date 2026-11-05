"""Test đọc robots.txt: so khớp quy tắc và cách HttpClient xử lý khi đọc lỗi (không gọi mạng)."""

import asyncio
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pytest

from common.config import PlatformLimits
from common.http import HttpClient, RobotsDisallowed, RobotsUnavailable
from common.robots import RobotsRules
from jobs.crawl_hot import load_watchlist

UA = "bigdata-crawler/0.1.0 (academic research project)"
FIXTURES = Path(__file__).parent / "fixtures" / "robots"


def rules(text: str, user_agent: str = UA) -> RobotsRules:
    return RobotsRules(text, user_agent)


def fixture_rules(platform: str) -> RobotsRules:
    return rules((FIXTURES / f"{platform}.txt").read_text(encoding="utf-8"))


# ------------------------------ so khớp quy tắc ------------------------------

@pytest.mark.parametrize("path, allowed", [
    ("/admin/x", False),
    ("/dien-thoai?muc-gia=duoi-5-trieu", False),    # thư viện chuẩn cho phép nhầm trường hợp này
    ("/search?q=abc.com", False),
    ("/search?q=abc.vn", True),
    ("/dien-thoai", True),
])
def test_wildcard_rules(path, allowed):
    robots = rules("User-agent: *\nDisallow: /admin/\nDisallow: /*muc-gia=\nDisallow: /search?q=*.com\n")
    assert robots.is_allowed(path) is allowed


def test_dollar_anchors_end_of_url():
    robots = rules("User-agent: *\nDisallow: /*.sql$\n")
    assert robots.is_allowed("/backup.sql") is False
    assert robots.is_allowed("/backup.sql?x=1") is True


def test_longest_match_wins():
    robots = rules("User-agent: *\nDisallow: /\nAllow: /kho\nDisallow: /kho/an\n")
    assert robots.is_allowed("/khac") is False
    assert robots.is_allowed("/kho/abc") is True
    assert robots.is_allowed("/kho/an/1") is False


def test_allow_wins_tie():
    robots = rules("User-agent: *\nDisallow: /a\nAllow: /a\n")
    assert robots.is_allowed("/a") is True


def test_empty_disallow_blocks_nothing():
    assert rules("User-agent: *\nDisallow:\n").is_allowed("/bat-ky") is True


def test_named_group_replaces_star_group():
    text = "User-agent: *\nDisallow: /chung\n\nUser-agent: BigData-Crawler\nDisallow: /rieng\n"
    robots = rules(text)
    assert robots.is_allowed("/chung") is True
    assert robots.is_allowed("/rieng") is False


def test_consecutive_user_agents_share_rules():
    text = "User-agent: Googlebot\nUser-agent: *\nDisallow: /a\n"
    assert rules(text).is_allowed("/a") is False


def test_rules_of_other_bots_do_not_apply():
    assert rules("User-agent: Googlebot\nDisallow: /a\n").is_allowed("/a") is True


def test_percent_encoding_matches_unicode_rule():
    robots = rules("User-agent: *\nDisallow: /*đánh lô đề*\n")
    assert robots.is_allowed("/search?q=%C4%91%C3%A1nh%20l%C3%B4%20%C4%91%E1%BB%81") is False


def test_comments_are_ignored():
    robots = rules("# ghi chú\nUser-agent: * # mọi crawler\nDisallow: /a # cấm\n")
    assert robots.is_allowed("/a") is False


# ------------------------------ quy tắc thật của các sàn ------------------------------

def test_real_fptshop_filter_rule():
    robots = fixture_rules("fptshop")
    assert robots.is_allowed("/dien-thoai?muc-gia=duoi-5-trieu") is False
    assert robots.is_allowed("/dien-thoai") is True


def test_real_tiki_rules_keep_listing_api_open():
    robots = fixture_rules("tiki")
    assert robots.is_allowed("/api/personalish/v1/blocks/listings?limit=40&category=1789&page=1") is True
    assert robots.is_allowed("/api/v2/me/orders") is False
    assert robots.is_allowed("/search?q=abc.com") is False


def test_real_hoangha_rules():
    robots = fixture_rules("hoangha")
    assert robots.is_allowed("/kho-san-pham-cu") is True
    assert robots.is_allowed("/gio-hang/") is False
    assert robots.is_allowed("/dien-thoai?filters=abc") is False


def test_enabled_watchlist_pages_are_allowed():
    """Mọi trang đang bật trong watchlist đều được phép theo bản robots.txt đã lưu."""
    watchlist = load_watchlist("config/watchlist.csv")
    assert watchlist
    for platform, items in watchlist.items():
        robots = fixture_rules(platform)
        for item in items:
            parts = urlsplit(item.url)
            target = parts.path + (f"?{parts.query}" if parts.query else "")
            assert robots.is_allowed(target), f"{platform} {target}"


# ------------------------------ HttpClient đọc robots.txt ------------------------------

def make_client(handler, **kwargs) -> HttpClient:
    return HttpClient(
        "fptshop",
        PlatformLimits(requests_per_second=1000, max_concurrency=2, timeout_seconds=5),
        UA,
        transport=httpx.MockTransport(handler),
        **{"max_attempts": 2, "backoff_base": 0.001, "backoff_max": 0.01, **kwargs},
    )


class Site:
    """Site giả: robots.txt trả lần lượt các response trong `robots`, trang thường trả 200."""

    def __init__(self, *robots):
        self.robots = list(robots)
        self.robots_hits = 0
        self.pages: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            self.robots_hits += 1
            item = self.robots.pop(0) if len(self.robots) > 1 else self.robots[0]
            if isinstance(item, Exception):
                raise item
            return item
        self.pages.append(str(request.url))
        return httpx.Response(200, json={"ok": True})


def robots_ok(body: str = "User-agent: *\nDisallow: /*muc-gia=\n") -> httpx.Response:
    return httpx.Response(200, text=body, headers={"content-type": "text/plain"})


@pytest.mark.asyncio
async def test_client_blocks_wildcard_rule():
    site = Site(robots_ok())
    async with make_client(site) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://x.vn/dien-thoai?muc-gia=re")
    assert site.pages == []


@pytest.mark.asyncio
async def test_client_checks_query_passed_through_params():
    site = Site(robots_ok())
    async with make_client(site) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://x.vn/dien-thoai", params={"muc-gia": "re"})
        # params được gộp vào query có sẵn, nên query cũ vẫn phải được kiểm tra
        with pytest.raises(RobotsDisallowed):
            await client.get("https://x.vn/dien-thoai?muc-gia=re", params={"page": "2"})
        assert (await client.get("https://x.vn/dien-thoai", params={"page": "2"})).status_code == 200
    assert len(site.pages) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 410])
async def test_missing_robots_allows_crawling(status):
    site = Site(httpx.Response(status))
    async with make_client(site) as client:
        assert (await client.get("https://x.vn/a")).status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    httpx.Response(500),
    httpx.Response(503),
    httpx.Response(401),
    httpx.Response(403),
    httpx.Response(429),
    httpx.Response(200, text="<html>checking your network</html>", headers={"content-type": "text/html"}),
    httpx.Response(200, text="<!doctype html><p>challenge</p>", headers={"content-type": "text/plain"}),
    httpx.ConnectError("connection refused"),
])
async def test_unreadable_robots_skips_host(response):
    site = Site(response)
    async with make_client(site) as client:
        with pytest.raises(RobotsUnavailable):
            await client.get("https://x.vn/a")
    assert site.pages == []


def test_unavailable_is_handled_like_disallowed():
    # crawl_hot và crawl_full bắt RobotsDisallowed nên trang lỗi bị bỏ qua, không làm chết vòng crawl
    assert issubclass(RobotsUnavailable, RobotsDisallowed)


@pytest.mark.asyncio
async def test_failed_read_is_not_retried_on_every_request():
    site = Site(httpx.Response(503))
    async with make_client(site) as client:
        for _ in range(3):
            with pytest.raises(RobotsUnavailable):
                await client.get("https://x.vn/a")
    assert site.robots_hits == 1


@pytest.mark.asyncio
async def test_recovers_after_retry_interval():
    site = Site(httpx.Response(503), robots_ok())
    async with make_client(site, robots_retry_seconds=0.01) as client:
        with pytest.raises(RobotsUnavailable):
            await client.get("https://x.vn/a")
        await asyncio.sleep(0.02)
        assert (await client.get("https://x.vn/a")).status_code == 200


@pytest.mark.asyncio
async def test_robots_is_reloaded_after_ttl():
    site = Site(robots_ok("User-agent: *\nDisallow: /cu\n"), robots_ok("User-agent: *\nDisallow: /moi\n"))
    async with make_client(site, robots_ttl_seconds=0.01) as client:
        assert (await client.get("https://x.vn/moi")).status_code == 200
        await asyncio.sleep(0.02)
        with pytest.raises(RobotsDisallowed):
            await client.get("https://x.vn/moi")
    assert site.robots_hits == 2


@pytest.mark.asyncio
async def test_failed_reload_keeps_previous_rules():
    site = Site(robots_ok("User-agent: *\nDisallow: /cam\n"), httpx.Response(503))
    async with make_client(site, robots_ttl_seconds=0.01) as client:
        await client.get("https://x.vn/a")
        await asyncio.sleep(0.02)
        assert (await client.get("https://x.vn/a")).status_code == 200     # vẫn crawl được
        with pytest.raises(RobotsDisallowed):
            await client.get("https://x.vn/cam")                           # vẫn tuân theo bản cũ
