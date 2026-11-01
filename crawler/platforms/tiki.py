"""Crawler cho Tiki, dùng API JSON công khai (đã kiểm chứng ngày 2026-11-01).

- Trang danh sách: api/personalish/v1/blocks/listings?limit=40&category=<id>&page=<n>
  Mỗi trang ~40 sản phẩm, có giá, seller_id, điểm sao, lượt bán. Không có tên shop, tồn kho.
- Trang chi tiết: api/v2/products/<id>?spid=<spid> + api/v2/reviews (phân bố 1–5 sao).
  fetch() gọi cả hai rồi ghép thành body {"product": ..., "reviews": ...}.

Quy ước gán trường (đã thống nhất):
- sku_id = spid (seller_product_id): mỗi shop bán cùng sản phẩm có spid riêng.
- rating_average = 0 nghĩa là chưa có đánh giá -> None.
- rating_count = review_count: trên Tiki mỗi review đi kèm đúng một lượt chấm sao.
"""

import json
import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from pydantic import ValidationError

from common.ids import make_raw_hash
from common.schema import Event, PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.base import Platform

logger = logging.getLogger(__name__)

BASE_URL = "https://tiki.vn"
PRODUCT_API = "https://tiki.vn/api/v2/products/{product_id}"
REVIEWS_API = "https://tiki.vn/api/v2/reviews"

# inventory_status của Tiki -> in_stock. Giá trị lạ hoặc rỗng -> None (không đoán).
STOCK_MAP = {"available": True, "out_of_stock": False}


# ---------------------------------------------------------------------------
# Hàm ép kiểu: không bao giờ ném lỗi, đọc không được thì trả None
# ---------------------------------------------------------------------------

def to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def to_rating(value: Any) -> float | None:
    """Sao trung bình. Tiki trả 0 khi chưa có đánh giá -> None."""
    try:
        rating = float(value)
    except (TypeError, ValueError):
        return None
    return rating if rating > 0 else None


def as_dict(value: Any) -> dict:
    """Trường lồng nhau có thể là null hoặc sai kiểu; luôn trả về dict để .get() an toàn."""
    return value if isinstance(value, dict) else {}


def product_url(url_path: str | None, product_id: Any) -> str | None:
    """Link trang sản phẩm. Thiếu url_path thì dùng link rút gọn theo id."""
    if url_path:
        return f"{BASE_URL}/{url_path.lstrip('/')}"
    if product_id:
        return f"{BASE_URL}/product-p{product_id}.html"
    return None


def category_path(breadcrumbs: Any) -> str | None:
    """Nối tên danh mục: "Điện Thoại - Máy Tính Bảng > Điện thoại Smartphone".
    Phần tử cuối là tên sản phẩm (category_id = 0) nên bị bỏ."""
    if not isinstance(breadcrumbs, list):
        return None
    names = [b.get("name") for b in breadcrumbs if isinstance(b, dict) and b.get("category_id")]
    return " > ".join(n for n in names if n) or None


def flatten_specs(specifications: Any) -> dict:
    """[{"attributes": [{"code": "origin", "value": "Trung Quốc"}, ...]}] -> {"origin": "Trung Quốc"}.
    Giữ nguyên key (code) và value của Tiki, không chuẩn hoá."""
    specs: dict = {}
    if not isinstance(specifications, list):
        return specs
    for group in specifications:
        for attr in as_dict(group).get("attributes") or []:
            attr = as_dict(attr)
            if attr.get("code"):
                specs[attr["code"]] = attr.get("value")
    return specs


def star_count(stars: dict, star: str) -> int | None:
    """Số lượt chấm một mức sao từ reviews.stars, ví dụ stars["5"]["count"]."""
    return to_int(as_dict(stars.get(star)).get("count"))


# ---------------------------------------------------------------------------
# Platform
# ---------------------------------------------------------------------------

class TikiPlatform(Platform):
    name = "tiki"

    def discover(self, category: str):
        # Liệt kê toàn bộ trang của danh mục: làm ở bước 9 (crawl_full)
        raise NotImplementedError("Tiki discover is implemented in step 9")

    # ----------------------------- fetch -----------------------------

    async def fetch(self, ref: ProductRef) -> RawResponse:
        if ref.kind == "listing":
            # ref.url là URL API danh sách đầy đủ (lấy từ watchlist.csv)
            resp = await self.http.get(ref.url)
            return self.make_raw(ref, resp.status_code, resp.text, resp.headers.get("content-type"))
        return await self._fetch_detail(ref)

    async def _fetch_detail(self, ref: ProductRef) -> RawResponse:
        """ref.url là link trang sản phẩm (có ?spid=...), ref.product_id là id Tiki."""
        if not ref.product_id:
            raise ValueError("detail ref requires product_id")
        spid = parse_qs(urlparse(ref.url).query).get("spid", [None])[0]
        params = {"spid": spid} if spid else None

        resp = await self.http.get(PRODUCT_API.format(product_id=ref.product_id), params=params)
        try:
            product = resp.json() if resp.status_code == 200 else None
        except ValueError:
            product = None
        if product is None:
            # Lỗi hoặc không phải JSON: trả nguyên văn để parse() log và bỏ qua
            return self.make_raw(ref, resp.status_code, resp.text, resp.headers.get("content-type"))

        reviews = await self._fetch_reviews_summary(ref.product_id, spid, product)
        body = json.dumps({"product": product, "reviews": reviews}, ensure_ascii=False)
        return self.make_raw(ref, 200, body, "application/json")

    async def _fetch_reviews_summary(self, product_id: str, spid: str | None, product: dict) -> dict | None:
        """Lấy phân bố sao. Lỗi thì trả None: thiếu phân bố sao không được làm mất cả sản phẩm."""
        params: dict[str, Any] = {"product_id": product_id, "limit": 1, "page": 1}
        if spid:
            params["spid"] = spid
        seller_id = as_dict(product.get("current_seller")).get("id")
        if seller_id:
            params["seller_id"] = seller_id
        try:
            resp = await self.http.get(REVIEWS_API, params=params)
            if resp.status_code != 200:
                logger.warning("Reviews API returned %s for product %s", resp.status_code, product_id)
                return None
            summary = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("Reviews API failed for product %s: %s", product_id, exc)
            return None
        # Bỏ nội dung review: có tên, avatar người mua (dữ liệu cá nhân, README mục 7)
        summary["data"] = []
        return summary

    # ----------------------------- parse -----------------------------

    def parse(self, raw: RawResponse) -> list[Event]:
        if raw.status_code != 200:
            logger.warning("Skip %s: HTTP %s", raw.ref.url, raw.status_code)
            return []
        try:
            payload = json.loads(raw.body)
        except ValueError:
            logger.warning("Skip %s: body is not JSON", raw.ref.url)
            return []

        raw_hash = make_raw_hash(raw.body)
        if raw.kind == "listing":
            return self._parse_listing(as_dict(payload), raw, raw_hash)
        return self._parse_detail(as_dict(payload), raw, raw_hash)

    def _parse_listing(self, payload: dict, raw: RawResponse, raw_hash: str) -> list[Event]:
        events: list[Event] = []
        for item in payload.get("data") or []:
            item = as_dict(item)
            try:
                events.append(
                    self._price_event(
                        item, raw, raw_hash,
                        source="listing",
                        sku_id=item.get("seller_product_id"),
                        seller_id=item.get("seller_id"),
                        seller_name=None,       # danh sách không có tên shop
                    )
                )
            except ValidationError as exc:
                # Thiếu trường bắt buộc (id, tên...) thì không tạo được event: bỏ đúng sản phẩm này
                logger.warning("Skip listing item %s: %s", item.get("id"), exc.errors()[0]["msg"])
        return events

    def _parse_detail(self, payload: dict, raw: RawResponse, raw_hash: str) -> list[Event]:
        product = as_dict(payload.get("product"))
        reviews = as_dict(payload.get("reviews"))
        seller = as_dict(product.get("current_seller"))
        stars = as_dict(reviews.get("stars"))
        try:
            product_event = ProductEvent(
                platform=self.name,
                product_id=to_str(product.get("id")),
                url=product_url(product.get("url_path"), product.get("id")),
                name_raw=product.get("name"),
                brand_raw=to_str(as_dict(product.get("brand")).get("name")),
                category_raw=category_path(product.get("breadcrumbs")),
                specs_raw=flatten_specs(product.get("specifications")),
                image_url=to_str(product.get("thumbnail_url")),
                # Ưu tiên số từ API review (chính xác hơn, ví dụ 4.9 thay vì 5)
                rating_avg=to_rating(reviews.get("rating_average", product.get("rating_average"))),
                rating_count=to_int(reviews.get("reviews_count", product.get("review_count"))),
                rating_1_count=star_count(stars, "1"),
                rating_2_count=star_count(stars, "2"),
                rating_3_count=star_count(stars, "3"),
                rating_4_count=star_count(stars, "4"),
                rating_5_count=star_count(stars, "5"),
                event_time=raw.fetched_at,
                crawler_version=self.crawler_version,
                raw_hash=raw_hash,
            )
            price_event = self._price_event(
                product, raw, raw_hash,
                source="detail",
                sku_id=seller.get("product_id"),
                seller_id=seller.get("id"),
                seller_name=seller.get("name"),
            )
        except ValidationError as exc:
            logger.warning("Skip detail %s: %s", raw.ref.url, exc.errors()[0]["msg"])
            return []
        return [product_event, price_event]

    def _price_event(
        self,
        item: dict,
        raw: RawResponse,
        raw_hash: str,
        *,
        source: str,
        sku_id: Any,
        seller_id: Any,
        seller_name: Any,
    ) -> PriceEvent:
        """Dựng PriceEvent từ một sản phẩm. Trang danh sách và trang chi tiết của Tiki
        dùng chung tên trường (price, rating_average, quantity_sold...) nên dùng chung hàm."""
        sold = as_dict(item.get("quantity_sold"))
        stock = item.get("inventory_status")
        return PriceEvent(
            platform=self.name,
            product_id=to_str(item.get("id")),
            sku_id=to_str(sku_id),
            url=product_url(item.get("url_path"), item.get("id")),
            name_raw=item.get("name"),
            source=source,
            price_current=to_int(item.get("price")),
            price_current_raw=to_str(item.get("price")),
            price_original=to_int(item.get("original_price")),
            price_original_raw=to_str(item.get("original_price")),
            in_stock=STOCK_MAP.get(stock),
            stock_status_raw=to_str(stock),
            seller_id=to_str(seller_id),
            seller_name=to_str(seller_name),
            rating_avg=to_rating(item.get("rating_average")),
            rating_count=to_int(item.get("review_count")),
            review_count=to_int(item.get("review_count")),
            sold_count=to_int(sold.get("value")),
            sold_count_raw=to_str(sold.get("text")),
            event_time=raw.fetched_at,
            crawler_version=self.crawler_version,
            raw_hash=raw_hash,
        )
