"""Crawler cho FPT Shop, đọc HTML (đã kiểm chứng ngày 2026-11-01).

FPT Shop là ứng dụng Next.js: dữ liệu trang được nhúng trong HTML dưới dạng các đoạn
self.__next_f.push([1, "<chuỗi>"]). Nối các chuỗi lại sẽ có các object JSON của sản phẩm.

- Trang danh sách (ví dụ https://fptshop.com.vn/dien-thoai): danh sách chính ~24 sản phẩm,
  mỗi sản phẩm có code (mã SKU), name, slug, currentPrice, originalPrice, averageScore,
  sold.realQuantity (lượt bán), totalInventory. KHÔNG phân trang thật phía server
  (?page=2 trùng 19/24 sản phẩm), muốn nhiều SKU hơn thì thêm trang con / bộ lọc giá vào watchlist.
- Trang chi tiết: JSON-LD Product có giá, tồn kho, thương hiệu, sku, điểm sao, số đánh giá, thông số.

Quy ước:
- product_id = slug trong URL ("iphone-duo"); sku_id = code (danh sách) hoặc sku (JSON-LD).
  Danh sách và chi tiết có thể là hai biến thể khác nhau của cùng sản phẩm (giá khác nhau),
  sku_id giúp phân biệt.
- sold_count = sold.realQuantity; sold_count_raw giữ nguyên cả object sold dạng JSON
  (có realQuantity, virtualQuantity, totalQuantity) để Silver tự chọn.
- in_stock: totalInventory > 0 -> True, = 0 -> False, không có -> None.
- FPT Shop tự bán hàng: seller_id, seller_name = None.
"""

import json
import logging
from typing import Any

from bs4 import BeautifulSoup
from pydantic import ValidationError

from common.ids import make_raw_hash
from common.schema import Event, PriceEvent, ProductEvent, ProductRef, RawResponse
from platforms.base import Platform
from platforms.parsing import (
    as_dict,
    availability_to_bool,
    first_image,
    first_offer,
    json_ld,
    longest_object_list,
    next_flight_text,
    slug_from_url,
    to_int,
    to_rating,
    to_str,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://fptshop.com.vn"

# Đọc dữ liệu Next.js: hàm chung ở platforms/parsing.py
flight_text = next_flight_text


def main_product_list(flight: str) -> list[dict]:
    """Danh sách sản phẩm chính: mảng "items" dài nhất chứa object có currentPrice
    (khối phụ như "Bán chạy nhất" chỉ 1-2 sản phẩm)."""
    return longest_object_list(flight, "items", "currentPrice")


def stock_from_inventory(value: Any) -> bool | None:
    count = to_int(value)
    if count is None:
        return None
    return count > 0


class FptshopPlatform(Platform):
    name = "fptshop"

    async def discover(self, category: str):
        """FPT Shop không phân trang phía server: mỗi danh mục chỉ có một trang."""
        yield ProductRef(platform=self.name, kind="listing", url=f"{BASE_URL}/{category}", category=category)

    async def fetch(self, ref: ProductRef) -> RawResponse:
        resp = await self.http.get(ref.url)
        return self.make_raw(ref, resp.status_code, resp.text, resp.headers.get("content-type"))

    def parse(self, raw: RawResponse) -> list[Event]:
        if raw.status_code != 200:
            logger.warning("Skip %s: HTTP %s", raw.ref.url, raw.status_code)
            return []
        raw_hash = make_raw_hash(raw.body)
        if raw.kind == "listing":
            return self._parse_listing(raw, raw_hash)
        return self._parse_detail(BeautifulSoup(raw.body, "html.parser"), raw, raw_hash)

    def _parse_listing(self, raw: RawResponse, raw_hash: str) -> list[Event]:
        items = main_product_list(flight_text(raw.body))
        if not items:
            logger.warning("No product data in %s (page layout changed?)", raw.ref.url)
        events: list[Event] = []
        for item in items:
            slug = to_str(item.get("slug"))
            url = f"{BASE_URL}/{slug.lstrip('/')}" if slug else None
            sold = as_dict(item.get("sold"))
            inventory = item.get("totalInventory")
            try:
                events.append(PriceEvent(
                    platform=self.name,
                    product_id=slug_from_url(url),
                    sku_id=to_str(item.get("code")),
                    url=url,
                    name_raw=item.get("name"),
                    source="listing",
                    price_current=to_int(item.get("currentPrice")),
                    price_current_raw=to_str(item.get("currentPrice")),
                    price_original=to_int(item.get("originalPrice")),
                    price_original_raw=to_str(item.get("originalPrice")),
                    in_stock=stock_from_inventory(inventory),
                    stock_status_raw=to_str(inventory),
                    rating_avg=to_rating(item.get("averageScore")),
                    sold_count=to_int(sold.get("realQuantity")),
                    sold_count_raw=json.dumps(sold, ensure_ascii=False) if sold else None,
                    event_time=raw.fetched_at,
                    crawler_version=self.crawler_version,
                    raw_hash=raw_hash,
                ))
            except ValidationError as exc:
                logger.warning("Skip listing item %s: %s", slug, exc.errors()[0]["msg"])
        return events

    def _parse_detail(self, soup: BeautifulSoup, raw: RawResponse, raw_hash: str) -> list[Event]:
        product = json_ld(soup, "Product")
        if not product:
            logger.warning("Skip %s: no JSON-LD Product", raw.ref.url)
            return []
        offer = first_offer(product)
        rating = as_dict(product.get("aggregateRating"))
        url = to_str(offer.get("url")) or raw.ref.url
        product_id = slug_from_url(url)

        crumbs = json_ld(soup, "BreadcrumbList").get("itemListElement") or []
        names = [as_dict(c).get("name") or as_dict(as_dict(c).get("item")).get("name") for c in crumbs]
        category = " > ".join(n for n in names[1:] if n) or None    # bỏ "Trang chủ"

        specs = {
            as_dict(p).get("name"): as_dict(p).get("value")
            for p in product.get("additionalProperty") or []
            if as_dict(p).get("name")
        }
        price_raw = to_str(offer.get("price"))
        try:
            product_event = ProductEvent(
                platform=self.name,
                product_id=product_id,
                url=url,
                name_raw=product.get("name"),
                brand_raw=to_str(as_dict(product.get("brand")).get("name")),
                category_raw=category,
                specs_raw=specs,
                image_url=first_image(product.get("image")),
                rating_avg=to_rating(rating.get("ratingValue")),
                rating_count=to_int(rating.get("reviewCount")),
                event_time=raw.fetched_at,
                crawler_version=self.crawler_version,
                raw_hash=raw_hash,
            )
            price_event = PriceEvent(
                platform=self.name,
                product_id=product_id,
                sku_id=to_str(product.get("sku")),
                url=url,
                name_raw=product.get("name"),
                source="detail",
                price_current=to_int(price_raw),
                price_current_raw=price_raw,
                in_stock=availability_to_bool(offer.get("availability")),
                stock_status_raw=to_str(offer.get("availability")),
                rating_avg=to_rating(rating.get("ratingValue")),
                rating_count=to_int(rating.get("reviewCount")),
                review_count=to_int(rating.get("reviewCount")),
                event_time=raw.fetched_at,
                crawler_version=self.crawler_version,
                raw_hash=raw_hash,
            )
        except ValidationError as exc:
            logger.warning("Skip detail %s: %s", raw.ref.url, exc.errors()[0]["msg"])
            return []
        return [product_event, price_event]
