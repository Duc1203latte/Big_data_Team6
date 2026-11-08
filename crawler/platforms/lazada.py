"""Crawler cho Lazada, đọc JSON của trang danh sách (đã kiểm chứng ngày 2026-11-04).

Chỉ lấy trang công khai, không đăng nhập, không giả trình duyệt. Bị chặn thì dừng, không lách.
Lazada Terms of Use (mục 1.2, 4.2) hạn chế việc lưu nội dung khi chưa có văn bản cho phép:
dữ liệu chỉ dùng nội bộ cho bài tập, không công bố, không đưa dữ liệu thật lên repo.

- Trang danh sách: https://www.lazada.vn/<danh-muc>/?ajax=true&page=1
  JSON: mods.listItems (~40 sản phẩm), có giá, người bán, thương hiệu, điểm sao, lượt bán.
  HTML của cùng trang không có sản phẩm (dữ liệu do JavaScript tải sau).
- Phân trang không ổn định khi đo: page=2 trùng 40/40 ở danh mục, 31/40 ở tìm kiếm.
  Vì vậy mỗi danh mục chỉ lấy một trang.
- Trang chi tiết: chưa hỗ trợ. Trang danh sách đã có thương hiệu và người bán, nên mỗi sản
  phẩm sinh thêm một ProductEvent từ trang danh sách.

Quy ước:
- product_id = itemId; sku_id = skuId.
- rating_count = review (số lượt đánh giá); review_count = None vì sàn không tách riêng.
- sold_count ước lượng từ chữ ("5.3K sold" -> 5300), chữ gốc giữ ở sold_count_raw.
- name_raw giữ nguyên: sàn có thể trả tên tiếng Anh hoặc tiếng Việt, và còn mã HTML (&lt;).
"""

import json
import logging
from typing import Any

from pydantic import ValidationError

from common.http import PlatformBlocked, save_block_sample
from common.ids import make_raw_hash
from common.schema import Event, PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.base import Platform
from platforms.parsing import as_dict, parse_count_text, parse_vnd, to_int, to_rating, to_str

logger = logging.getLogger(__name__)

BASE_URL = "https://www.lazada.vn"
LISTING_URL = BASE_URL + "/{category}/?ajax=true&page=1"

# Dấu hiệu trang kiểm tra chống bot (captcha) trả về dưới dạng JSON hoặc URL chuyển hướng
BLOCK_MARKERS = ("rgv587", "fail_sys_user_validate", "punish", "x5secdata", "captcha", "_____tmd_____")


def is_block_payload(body: str, final_url: str = "") -> bool:
    """Response có phải trang chống bot không. JSON có sản phẩm (mods) thì không tính,
    vì tên sản phẩm có thể tình cờ chứa các từ trên."""
    if any(marker in final_url.lower() for marker in BLOCK_MARKERS):
        return True
    try:
        payload = json.loads(body)
    except ValueError:
        return False        # không phải JSON: expect_json của HttpClient đã xử lý
    if isinstance(payload, dict) and "mods" in payload:
        return False
    return any(marker in body.lower() for marker in BLOCK_MARKERS)


def product_url(item: dict) -> str | None:
    """itemUrl có dạng "//www.lazada.vn/products/...": thêm https:."""
    url = to_str(item.get("itemUrl"))
    if url and url.startswith("//"):
        return f"https:{url}"
    return url


def to_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


class LazadaPlatform(Platform):
    name = "lazada"

    async def discover(self, category: str):
        """Mỗi danh mục (ví dụ "pin-sac-du-phong") chỉ lấy một trang, xem ghi chú phân trang."""
        yield ProductRef(platform=self.name, kind="listing", url=LISTING_URL.format(category=category),
                         category=category, page=1)

    async def fetch(self, ref: ProductRef) -> RawResponse:
        resp = await self.http.get(ref.url)
        # Captcha của Lazada có thể là JSON mã 200 nên HttpClient không nhận ra: dừng sàn ngay
        if is_block_payload(resp.text, str(resp.url)):
            sample = save_block_sample(self.name, ref.url, resp.status_code, resp.text)
            raise PlatformBlocked(f"{self.name}: anti-bot response for {ref.url}, stopping, saved to {sample}")
        return self.make_raw(ref, resp.status_code, resp.text, resp.headers.get("content-type"))

    def parse(self, raw: RawResponse) -> list[Event]:
        if raw.status_code != 200:
            logger.warning("Skip %s: HTTP %s", raw.ref.url, raw.status_code)
            return []
        if raw.kind != "listing":
            logger.warning("Skip %s: detail pages are not supported", raw.ref.url)
            return []
        try:
            payload = as_dict(json.loads(raw.body))
        except ValueError:
            logger.warning("Skip %s: body is not JSON", raw.ref.url)
            return []
        items = as_dict(payload.get("mods")).get("listItems")
        if not items:
            logger.warning("No products in %s (response format changed?)", raw.ref.url)
            return []

        raw_hash = make_raw_hash(raw.body)
        events: list[Event] = []
        for item in items:
            item = as_dict(item)
            try:
                events.append(self._price_event(item, raw, raw_hash))
                events.append(self._product_event(item, raw, raw_hash))
            except ValidationError as exc:
                # Thiếu trường bắt buộc (id, tên, link) thì bỏ đúng sản phẩm này
                logger.warning("Skip listing item %s: %s", item.get("itemId"), exc.errors()[0]["msg"])
        return events

    def _price_event(self, item: dict, raw: RawResponse, raw_hash: str) -> PriceEvent:
        original_raw = to_str(item.get("originalPriceShow"))
        sold_raw = to_str(item.get("itemSoldCntShow"))
        return PriceEvent(
            platform=self.name,
            product_id=to_str(item.get("itemId")),
            sku_id=to_str(item.get("skuId")),
            url=product_url(item),
            name_raw=item.get("name"),
            source="listing",
            price_current=to_int(item.get("price")),
            price_current_raw=to_str(item.get("price")),
            # Giá gốc chỉ có dạng chữ, ví dụ "₫3,328,000" hoặc "3.328.000 ₫"
            price_original=parse_vnd(original_raw.lstrip("₫ ")) if original_raw else None,
            price_original_raw=original_raw,
            in_stock=to_bool(item.get("inStock")),
            stock_status_raw=to_str(item.get("inStock")),
            seller_id=to_str(item.get("sellerId")),
            seller_name=to_str(item.get("sellerName")),
            rating_avg=to_rating(item.get("ratingScore")),
            rating_count=to_int(item.get("review")),
            sold_count=parse_count_text(sold_raw),
            sold_count_raw=sold_raw,
            event_time=raw.fetched_at,
            crawler_version=self.crawler_version,
            raw_hash=raw_hash,
        )

    def _product_event(self, item: dict, raw: RawResponse, raw_hash: str) -> ProductEvent:
        return ProductEvent(
            platform=self.name,
            product_id=to_str(item.get("itemId")),
            url=product_url(item),
            name_raw=item.get("name"),
            brand_raw=to_str(item.get("brandName")),
            category_raw=raw.ref.category,
            specs_raw={key: item[key] for key in ("categories", "location", "brandId") if key in item},
            image_url=to_str(item.get("image")),
            rating_avg=to_rating(item.get("ratingScore")),
            rating_count=to_int(item.get("review")),
            event_time=raw.fetched_at,
            crawler_version=self.crawler_version,
            raw_hash=raw_hash,
        )
