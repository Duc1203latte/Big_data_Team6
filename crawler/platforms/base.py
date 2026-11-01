"""Lớp trừu tượng cho 1 sàn TMĐT
Mỗi sàn chỉ implement 3 hàm:
- discover(): liệt kê trang cần crawl của một danh mục (dùng cho crawl_full, bước 9)
- fetch():    gọi mạng, trả payload THÔ. Hàm duy nhất được đi ra mạng.
- parse():    payload thô -> list event. Hàm thuần, test offline bằng fixture."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, Protocol

import httpx

from common.schema import Event, ProductRef, RawResponse


class HttpGetter(Protocol):
    """Thứ gì có hàm get() giống httpx.AsyncClient.get() là dùng được."""

    async def get(self, url: str, *, params: dict[str, Any] | None = None) -> httpx.Response: ...


class Platform(ABC):
    name: str   # tên sàn, khớp với trường platform của event: tiki | cellphones | ...

    def __init__(self, http: HttpGetter, crawler_version: str) -> None:
        self.http = http
        self.crawler_version = crawler_version

    @abstractmethod
    def discover(self, category: str) -> AsyncIterator[ProductRef]:
        """Liệt kê các trang danh sách của một danh mục. Dùng cho crawl_full."""

    @abstractmethod
    async def fetch(self, ref: ProductRef) -> RawResponse:
        """Gọi API hoặc tải trang, trả payload THÔ chưa parse."""

    @abstractmethod
    def parse(self, raw: RawResponse) -> list[Event]:
        """Payload thô -> danh sách event.

        Hàm thuần: không mạng, không I/O, không đọc giờ hiện tại.
        event_time lấy từ raw.fetched_at. Lỗi ở một sản phẩm thì log cảnh báo
        và đi tiếp, không được làm hỏng cả trang.
        """

    def make_raw(
        self, ref: ProductRef, status_code: int, body: str, content_type: str | None
    ) -> RawResponse:
        """Đóng gói response thành RawResponse, đóng dấu thời điểm nhận (UTC)."""
        return RawResponse(
            ref=ref,
            status_code=status_code,
            body=body,
            content_type=content_type,
            fetched_at=datetime.now(timezone.utc),
        )