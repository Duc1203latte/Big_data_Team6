"""Test bước 6: token bucket và HttpClient (dùng httpx.MockTransport, không gọi mạng)."""

import time

import httpx
import pytest

from common.config import PlatformLimits, load_platform_limits
from common.http import HttpClient, PlatformBlocked, RetryableStatus, RobotsDisallowed, describe_error
from common.ratelimit import TokenBucket

UA = "bigdata-crawler/0.1.0 (academic research project; contact: a@b.c)"
ROBOTS = "User-agent: *\nDisallow: /api/v2/me/\n"


# ------------------------------ token bucket ------------------------------

@pytest.mark.asyncio
async def test_token_bucket_spaces_requests():
    bucket = TokenBucket(rate=20)                 # 1 request mỗi 0,05 giây
    start = time.monotonic()
    for _ in range(5):
        await bucket.acquire()
    elapsed = time.monotonic() - start
    assert 0.18 <= elapsed < 0.5                  # request đầu đi ngay, 4 request sau chờ 0,05s


def test_token_bucket_slow_down_halves_with_floor():
    bucket = TokenBucket(rate=0.2)
    bucket.slow_down()
    assert bucket.rate == pytest.approx(0.1)
    for _ in range(10):
        bucket.slow_down()
    assert bucket.rate == pytest.approx(0.2 / 16)     # không thấp hơn mức sàn


def test_token_bucket_slow_down_at_floor_logs_once(caplog):
    bucket = TokenBucket(rate=0.2)
    with caplog.at_level("WARNING", logger="common.ratelimit"):
        for _ in range(10):
            bucket.slow_down()
    # 0,2 -> 0,1 -> 0,05 -> 0,025 -> 0,0125 (sàn): 4 lần giảm thật, 6 lần sau không log
    assert len([r for r in caplog.records if "Rate reduced" in r.message]) == 4


def test_token_bucket_recovers_to_base_rate():
    bucket = TokenBucket(rate=0.2)
    bucket.slow_down()
    for _ in range(14):
        bucket.recover()                              # 1,05^14 ≈ 1,98: gần về lại 0,2
    assert bucket.rate > 0.19
    for _ in range(10):
        bucket.recover()
    assert bucket.rate == pytest.approx(0.2)          # không vượt tốc độ gốc


def test_token_bucket_rejects_zero_rate():
    with pytest.raises(ValueError):
        TokenBucket(rate=0)


# ------------------------------ config ------------------------------

def test_load_platform_limits(tmp_path):
    path = tmp_path / "platforms.yaml"
    path.write_text("tiki:\n  requests_per_second: 0.2\n  max_concurrency: 2\n  timeout_seconds: 15\n")
    limits = load_platform_limits(path)
    assert limits["tiki"] == PlatformLimits(requests_per_second=0.2, max_concurrency=2, timeout_seconds=15)


def test_repo_platforms_yaml_is_valid():
    assert "tiki" in load_platform_limits("config/platforms.yaml")


# ------------------------------ HttpClient ------------------------------

def make_client(handler, expect_json: bool = False, **kwargs) -> HttpClient:
    """HttpClient chạy nhanh cho test: rate cao, backoff gần như 0, transport giả."""
    defaults = dict(max_attempts=4, backoff_base=0.001, backoff_max=0.01)
    defaults.update(kwargs)
    return HttpClient(
        "tiki",
        PlatformLimits(requests_per_second=1000, max_concurrency=2, timeout_seconds=5,
                       expect_json=expect_json),
        UA,
        transport=httpx.MockTransport(handler),
        **defaults,
    )


class Server:
    """Server giả: trả robots.txt, còn lại trả lần lượt các mã trong `statuses`."""

    def __init__(self, statuses, robots=ROBOTS):
        self.statuses = list(statuses)
        self.robots = robots
        self.requests: list[httpx.Request] = []
        self.robots_hits = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            self.robots_hits += 1
            return httpx.Response(200, text=self.robots)
        self.requests.append(request)
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if status == 0:
            raise httpx.ConnectError("connection refused", request=request)
        if status == -2:
            # httpx thật ném ReadTimeout với nội dung rỗng, giống log tgdd 2026-11-02
            raise httpx.ReadTimeout("", request=request)
        return httpx.Response(status, json={"ok": status == 200})


@pytest.mark.asyncio
async def test_sends_user_agent_and_params():
    server = Server([200])
    async with make_client(server) as client:
        resp = await client.get("https://tiki.vn/api/v2/products/1", params={"spid": "2"})
    assert resp.status_code == 200
    assert server.requests[0].headers["User-Agent"] == UA
    assert server.requests[0].url.params["spid"] == "2"


@pytest.mark.asyncio
async def test_robots_read_once_per_host():
    server = Server([200])
    async with make_client(server) as client:
        await client.get("https://tiki.vn/a")
        await client.get("https://tiki.vn/b")
    assert server.robots_hits == 1


@pytest.mark.asyncio
async def test_robots_disallow_blocks_request():
    server = Server([200])
    async with make_client(server) as client:
        with pytest.raises(RobotsDisallowed):
            await client.get("https://tiki.vn/api/v2/me/orders")
    assert server.requests == []                  # không gửi request bị cấm


@pytest.mark.asyncio
async def test_missing_robots_allows_all():
    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200)
    async with make_client(handler) as client:
        assert (await client.get("https://example.com/anything")).status_code == 200


@pytest.mark.asyncio
async def test_429_then_success_retries_and_slows_down():
    server = Server([429, 200])
    async with make_client(server) as client:
        resp = await client.get("https://tiki.vn/x")
        assert resp.status_code == 200
        assert len(server.requests) == 2
        # 1000 -> 500 khi gặp 429, rồi tăng 5% khi thành công
        assert client.bucket.rate == pytest.approx(525)


@pytest.mark.asyncio
async def test_server_error_gives_up_and_returns_last_response():
    server = Server([503])
    async with make_client(server, max_attempts=3) as client:
        resp = await client.get("https://tiki.vn/x")
    assert resp.status_code == 503
    assert len(server.requests) == 3


@pytest.mark.asyncio
async def test_404_returned_without_retry():
    server = Server([404])
    async with make_client(server) as client:
        assert (await client.get("https://tiki.vn/x")).status_code == 404
    assert len(server.requests) == 1


@pytest.mark.asyncio
async def test_network_error_retried():
    server = Server([0, 200])
    async with make_client(server) as client:
        assert (await client.get("https://tiki.vn/x")).status_code == 200
    assert len(server.requests) == 2


@pytest.mark.asyncio
async def test_network_error_raised_after_max_attempts():
    server = Server([0])
    async with make_client(server, max_attempts=2) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("https://tiki.vn/x")


@pytest.mark.asyncio
async def test_consecutive_403_stops_platform():
    # Mỗi request thử 4 lần; 403 liên tục quá 10 lần (tính dồn qua nhiều request) thì dừng
    server = Server([403])
    async with make_client(server, max_attempts=4) as client:
        for _ in range(2):
            assert (await client.get("https://tiki.vn/x")).status_code == 403   # 8 lần 403
        with pytest.raises(PlatformBlocked):
            await client.get("https://tiki.vn/x")                               # lần thứ 11
    assert len(server.requests) == 11


@pytest.mark.asyncio
async def test_success_resets_403_counter():
    server = Server([403, 403, 403, 200] * 5)
    async with make_client(server, max_attempts=4, max_consecutive_blocks=5) as client:
        for _ in range(5):
            assert (await client.get("https://tiki.vn/x")).status_code == 200


# ------------------------- chặn mềm (200 + trang HTML) -------------------------

CHALLENGE_HTML = "<!DOCTYPE html><html><body>the system is inspecting your current network environment</body></html>"


class ChallengeServer:
    """Server giả kiểu Tiki khi chặn: trả lần lượt 'challenge' (200 + HTML) hoặc 'json' (200 + JSON)."""

    def __init__(self, kinds):
        self.kinds = list(kinds)
        self.requests = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/robots.txt":
            return httpx.Response(200, text=ROBOTS)
        self.requests += 1
        kind = self.kinds.pop(0) if len(self.kinds) > 1 else self.kinds[0]
        if kind == "challenge":
            return httpx.Response(200, html=CHALLENGE_HTML)
        return httpx.Response(200, json={"data": []})


@pytest.mark.asyncio
async def test_challenge_page_retried_and_slows_down():
    server = ChallengeServer(["challenge", "json"])
    async with make_client(server, expect_json=True) as client:
        resp = await client.get("https://tiki.vn/api/x")
        assert resp.json() == {"data": []}
        assert server.requests == 2
        assert client.bucket.rate < 1000                # đã giảm tốc


@pytest.mark.asyncio
async def test_challenge_page_returned_after_max_attempts():
    server = ChallengeServer(["challenge"])
    async with make_client(server, expect_json=True, max_attempts=3) as client:
        resp = await client.get("https://tiki.vn/api/x")
    assert "inspecting" in resp.text                   # parse() sẽ log "not JSON" và bỏ qua
    assert server.requests == 3


@pytest.mark.asyncio
async def test_repeated_challenge_stops_platform():
    server = ChallengeServer(["challenge"])
    async with make_client(server, expect_json=True, max_attempts=4) as client:
        for _ in range(2):
            await client.get("https://tiki.vn/api/x")   # 8 lần bị chặn
        with pytest.raises(PlatformBlocked):
            await client.get("https://tiki.vn/api/x")   # lần thứ 11


@pytest.mark.asyncio
async def test_html_allowed_when_not_expecting_json():
    # Sàn HTML (CellphoneS, Hoàng Hà...) trả HTML là bình thường
    server = ChallengeServer(["challenge"])
    async with make_client(server, expect_json=False) as client:
        resp = await client.get("https://example.com/page")
    assert resp.status_code == 200
    assert server.requests == 1


def test_repo_tiki_expects_json():
    assert load_platform_limits("config/platforms.yaml")["tiki"].expect_json is True


# ------------------------------ timeout và log lỗi ------------------------------

@pytest.mark.asyncio
async def test_timeout_retried_and_slows_down():
    server = Server([-2, 200])
    async with make_client(server) as client:
        assert (await client.get("https://tiki.vn/x")).status_code == 200
        assert len(server.requests) == 2
        # 1000 -> 500 khi timeout, rồi tăng 5% khi thành công (giống 429)
        assert client.bucket.rate == pytest.approx(525)


@pytest.mark.asyncio
async def test_connect_error_does_not_slow_down():
    server = Server([0, 200])
    async with make_client(server) as client:
        await client.get("https://tiki.vn/x")
        assert client.bucket.rate == pytest.approx(1000)     # lỗi mạng khác: chỉ thử lại


@pytest.mark.asyncio
async def test_repeated_timeouts_do_not_stop_platform():
    # Mất mạng / sàn treo lâu: hết lượt thử thì ném lỗi cho trang đó, KHÔNG PlatformBlocked
    server = Server([-2])
    async with make_client(server, max_attempts=3, max_consecutive_blocks=1) as client:
        for _ in range(3):
            with pytest.raises(httpx.ReadTimeout):
                await client.get("https://tiki.vn/x")
        assert client._consecutive_blocks == 0


@pytest.mark.asyncio
async def test_retry_log_names_the_error(caplog):
    server = Server([-2, 503, 200])
    async with make_client(server) as client:
        with caplog.at_level("WARNING", logger="common.http"):
            await client.get("https://tiki.vn/x")
    retries = [r.message for r in caplog.records if " retry " in r.message]
    assert retries[0].endswith("after ReadTimeout")
    assert retries[1].endswith("after HTTP 503")


def test_describe_error():
    request = httpx.Request("GET", "https://tiki.vn/x")
    assert describe_error(httpx.ReadTimeout("", request=request)) == "ReadTimeout"
    assert describe_error(httpx.ConnectError("connection refused", request=request)) == \
        "ConnectError: connection refused"
    assert describe_error(RetryableStatus(httpx.Response(503))) == "HTTP 503"
    assert describe_error(None) == "unknown error"
