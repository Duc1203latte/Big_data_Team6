"""HTTP client dùng chung cho mọi sàn (README mục 7).

Mỗi sàn một HttpClient, gồm:
- Token bucket theo platforms.yaml, giới hạn số request chạy song song.
- User-Agent học thuật kèm email, timeout.
- Đọc robots.txt của từng host một lần, URL bị Disallow thì không gọi.
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
from typing import Any
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

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

logger = logging.getLogger(__name__)

BLOCK_STATUSES = {403, 429}
SERVER_ERROR_STATUSES = {500, 502, 503, 504}


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
        self._robots: dict[str, RobotFileParser] = {}        # host -> robots đã đọc
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
        if not await self._allowed_by_robots(url):
            raise RobotsDisallowed(url)

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

    async def _allowed_by_robots(self, url: str) -> bool:
        """Đọc robots.txt của host (một lần, có cache) rồi hỏi URL có được phép không."""
        parts = urlsplit(url)
        host = f"{parts.scheme}://{parts.netloc}"
        parser = self._robots.get(host)
        if parser is None:
            parser = RobotFileParser()
            try:
                await self.bucket.acquire()     # robots.txt cũng tính vào tốc độ của sàn
                resp = await self._client.get(f"{host}/robots.txt")
                # Không có robots.txt (4xx) = không cấm gì; lỗi khác cũng coi như không cấm
                parser.parse(resp.text.splitlines() if resp.status_code == 200 else [])
            except httpx.HTTPError as exc:
                logger.warning("Cannot read robots.txt of %s: %s", host, exc)
                parser.parse([])
            self._robots[host] = parser
        return parser.can_fetch(self.user_agent, url)
