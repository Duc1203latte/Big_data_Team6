"""Test bước 6: token bucket và HttpClient (dùng httpx.MockTransport, không gọi mạng)."""

import time

import httpx
import pytest

from common.config import PlatformLimits, load_platform_limits
from common.http import HttpClient, PlatformBlocked, RobotsDisallowed
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

def make_client(handler, **kwargs) -> HttpClient:
    """HttpClient chạy nhanh cho test: rate cao, backoff gần như 0, transport giả."""
    defaults = dict(max_attempts=4, backoff_base=0.001, backoff_max=0.01)
    defaults.update(kwargs)
    return HttpClient(
        "tiki",
        PlatformLimits(requests_per_second=1000, max_concurrency=2, timeout_seconds=5),
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
    async with make_client(server, max_attempts=4, max_consecutive_403=5) as client:
        for _ in range(5):
            assert (await client.get("https://tiki.vn/x")).status_code == 200
