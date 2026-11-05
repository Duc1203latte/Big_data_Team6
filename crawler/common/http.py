"""HTTP client dùng chung cho mọi sàn (README mục 7).

Mỗi sàn một HttpClient, gồm:
- Token bucket theo platforms.yaml, giới hạn số request chạy song song.
- User-Agent học thuật kèm email, timeout.
- Đọc robots.txt của từng host (đọc lại mỗi 24 giờ), URL bị Disallow thì không gọi.
  Không đọc được robots.txt (5xx, lỗi mạng, 401/403/429, trả trang HTML) thì tạm không
  crawl host đó; các mã 4xx khác nghĩa là không có file, được phép crawl.
- Retry bằng tenacity, backoff theo số mũ 5s, 10s, 20s, 40s... tối đa 5 phút:
  * lỗi mạng / timeout, 5xx: thử lại
  * 429 / 403: thử lại VÀ giảm tốc độ của sàn còn một nửa (thành công thì tăng dần lại)
  * "chặn mềm": sàn khai báo expect_json mà trả 2xx không phải JSON (trang kiểm tra
    chống bot, Tiki trả mã 200 kèm HTML): xử lý y như 403
  * 403 / chặn mềm liên tục quá 10 lần: ném PlatformBlocked để job dừng sàn đó
  * mã khác (200, 404...): trả về ngay
Hết số lần thử mà vẫn bị chặn hoặc 5xx thì trả response cuối cho parse() tự bỏ qua.

Không lách chống bot: không đổi proxy, không giả trình duyệt, không giải captcha.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from common.config import PlatformLimits
from common.ratelimit import TokenBucket
from common.robots import RobotsRules

logger = logging.getLogger(__name__)

BLOCK_STATUSES = {403, 429}
SERVER_ERROR_STATUSES = {500, 502, 503, 504}
# Bị từ chối đọc robots.txt khác với không có file, nên không coi là được phép
ROBOTS_REFUSED_STATUSES = {401, 403, 429}


def looks_like_json(response: httpx.Response) -> bool:
    """Response có phải JSON không: xem content-type, không rõ thì xem ký tự đầu của body."""
    if "json" in response.headers.get("content-type", "").lower():
        return True
    return response.text.lstrip()[:1] in ("{", "[")


def is_soft_block(response: httpx.Response, expect_json: bool) -> bool:
    """Chặn mềm: mã 2xx nhưng nội dung không phải JSON ở sàn đáng lẽ trả JSON."""
    return expect_json and 200 <= response.status_code < 300 and not looks_like_json(response)


class PlatformBlocked(Exception):
    """Sàn chặn liên tục (403 hoặc chặn mềm): dừng crawl sàn này, ghi lại và báo nhóm."""


class RobotsDisallowed(Exception):
    """URL bị robots.txt cấm."""


class RobotsUnavailable(RobotsDisallowed):
    """Chưa đọc được robots.txt nên tạm không crawl. Là lớp con để job xử lý như URL bị cấm."""


@dataclass
class _RobotsEntry:
    """robots.txt đã đọc của một host. rules = None nghĩa là chưa từng đọc được."""

    rules: RobotsRules | None
    next_fetch: float       # thời điểm (time.monotonic) cần đọc lại


class RetryableStatus(Exception):
    """Dùng nội bộ: response có mã cần thử lại (429/403/5xx)."""

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"HTTP {response.status_code}")
        self.response = response


class HttpClient:
    def __init__(
        self,
        platform: str,
        limits: PlatformLimits,
        user_agent: str,
        *,
        max_attempts: int = 6,
        backoff_base: float = 5.0,
        backoff_max: float = 300.0,
        max_consecutive_blocks: int = 10,
        robots_ttl_seconds: float = 24 * 3600,
        robots_retry_seconds: float = 600,
        transport: httpx.AsyncBaseTransport | None = None,   # test truyền MockTransport
    ) -> None:
        self.platform = platform
        self.user_agent = user_agent
        self.expect_json = limits.expect_json
        self.bucket = TokenBucket(limits.requests_per_second)
        self._semaphore = asyncio.Semaphore(limits.max_concurrency)
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.max_consecutive_blocks = max_consecutive_blocks
        self._consecutive_blocks = 0        # số lần 403 / chặn mềm liên tiếp
        self.robots_ttl_seconds = robots_ttl_seconds         # bao lâu đọc lại robots.txt một lần
        self.robots_retry_seconds = robots_retry_seconds     # đọc lỗi thì bao lâu sau thử lại
        self._robots: dict[str, _RobotsEntry] = {}           # host -> robots đã đọc
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent, "Accept": "application/json, text/html;q=0.9"},
            timeout=limits.timeout_seconds,
            follow_redirects=True,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # API công khai: cùng chữ ký với httpx.AsyncClient.get (xem platforms/base.py)
    # ------------------------------------------------------------------

    async def get(self, url: str, *, params: dict[str, Any] | None = None) -> httpx.Response:
        if not await self._allowed_by_robots(url, params):
            raise RobotsDisallowed("disallowed by robots.txt")

        retrying = AsyncRetrying(
            retry=retry_if_exception_type((httpx.TransportError, RetryableStatus)),
            # 5s, 10s, 20s, 40s... tối đa backoff_max
            wait=wait_exponential(multiplier=self.backoff_base, min=self.backoff_base, max=self.backoff_max),
            stop=stop_after_attempt(self.max_attempts),
            before_sleep=self._log_retry,
            reraise=True,
        )
        try:
            async for attempt in retrying:
                with attempt:
                    response = await self._send(url, params)
                    self._check_status(response)
                    return response
        except RetryableStatus as exc:
            # Hết lượt thử: trả response cuối, để parse() log và bỏ qua
            logger.error("Giving up %s after %d attempts: %s", url, self.max_attempts, exc)
            return exc.response
        raise AssertionError("unreachable")     # tenacity luôn return hoặc raise ở trên

    # ------------------------------------------------------------------
    # Nội bộ
    # ------------------------------------------------------------------

    async def _send(self, url: str, params: dict[str, Any] | None) -> httpx.Response:
        """Một lần gửi: xin token của sàn rồi gọi mạng, giới hạn số request song song."""
        async with self._semaphore:
            await self.bucket.acquire()
            return await self._client.get(url, params=params)

    def _check_status(self, response: httpx.Response) -> None:
        """Phân loại mã trả về; mã cần thử lại thì ném RetryableStatus cho tenacity."""
        status = response.status_code
        soft_block = is_soft_block(response, self.expect_json)
        if status == 403 or soft_block:
            self._consecutive_blocks += 1
            if self._consecutive_blocks > self.max_consecutive_blocks:
                raise PlatformBlocked(
                    f"{self.platform}: {self._consecutive_blocks} consecutive blocked responses "
                    f"(HTTP 403 or anti-bot page), stopping"
                )
        else:
            self._consecutive_blocks = 0

        if soft_block:
            logger.warning("%s returned non-JSON HTTP %s for %s (likely anti-bot challenge page)",
                           self.platform, status, response.request.url)
            self.bucket.slow_down()
            raise RetryableStatus(response)
        if status in BLOCK_STATUSES:
            logger.warning("%s returned HTTP %s for %s", self.platform, status, response.request.url)
            self.bucket.slow_down()
            raise RetryableStatus(response)
        if status in SERVER_ERROR_STATUSES:
            raise RetryableStatus(response)
        if 200 <= status < 300:
            self.bucket.recover()

    def _log_retry(self, state: RetryCallState) -> None:
        exc = state.outcome.exception() if state.outcome else None
        logger.warning(
            "%s retry %d/%d in %.0fs after %s",
            self.platform, state.attempt_number, self.max_attempts,
            state.next_action.sleep if state.next_action else 0, exc,
        )

    async def _allowed_by_robots(self, url: str, params: dict[str, Any] | None = None) -> bool:
        """URL có được phép không. Chưa đọc được robots.txt thì ném RobotsUnavailable."""
        parts = urlsplit(url)
        host = f"{parts.scheme}://{parts.netloc}"
        entry = self._robots.get(host)
        if entry is None or time.monotonic() >= entry.next_fetch:
            entry = await self._load_robots(host, entry)
            self._robots[host] = entry
        if entry.rules is None:
            raise RobotsUnavailable("robots.txt unavailable")
        # Kiểm tra URL thật sẽ gửi đi: giống httpx, params được gộp vào query có sẵn trong URL
        target = httpx.URL(url)
        if params is not None:
            target = target.copy_merge_params(params)
        return entry.rules.is_allowed(target.raw_path.decode("ascii"))

    async def _load_robots(self, host: str, previous: _RobotsEntry | None) -> _RobotsEntry:
        """Đọc robots.txt. Lỗi thì giữ bản đã đọc trước đó (nếu có) và hẹn thử lại sau."""
        rules = None
        reason = ""
        try:
            await self.bucket.acquire()     # robots.txt cũng tính vào tốc độ của sàn
            resp = await self._client.get(f"{host}/robots.txt")
            status = resp.status_code
            if status == 200 and not self._is_html(resp):
                rules = RobotsRules(resp.text, self.user_agent)
            elif 400 <= status < 500 and status not in ROBOTS_REFUSED_STATUSES:
                rules = RobotsRules.allow_all()     # không có robots.txt = không cấm gì
            else:
                reason = f"HTTP {status}, {resp.headers.get('content-type', 'no content-type')}"
        except httpx.HTTPError as exc:
            reason = type(exc).__name__

        now = time.monotonic()
        if rules is not None:
            return _RobotsEntry(rules, now + self.robots_ttl_seconds)
        kept = previous.rules if previous else None
        logger.warning("Cannot read robots.txt of %s (%s), %s", host, reason,
                       "keeping previous copy" if kept else "skipping host for now")
        return _RobotsEntry(kept, now + self.robots_retry_seconds)

    @staticmethod
    def _is_html(response: httpx.Response) -> bool:
        """Trang HTML thay cho robots.txt thường là trang chống bot, không phải quy tắc."""
        if "html" in response.headers.get("content-type", "").lower():
            return True
        return response.text.lstrip()[:1] == "<"
