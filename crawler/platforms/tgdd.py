"""Crawler cho Thế Giới Di Động (thegioididong.com), đọc HTML (đã kiểm chứng ngày 2026-11-01).

Trang là ứng dụng Next.js: dữ liệu nhúng trong self.__next_f.push(...) (giống FPT Shop).

- Trang danh sách (ví dụ https://www.thegioididong.com/dtdd): mảng "products" ~20 sản phẩm,
  mỗi sản phẩm có id, url, name, productCode, rate (sao), totalRate (lượt đánh giá),
  totalSold ("Đã bán 60,2k"), productPrices[0] gồm price, sysPrice (giá gốc), stock, isCanBuy.
  KHÔNG phân trang phía server (?page=2 trả lại đúng trang 1); muốn nhiều SKU hơn thì
  dùng các trang lọc theo hãng / RAM / bộ nhớ có trong menu của sàn.
- Trang chi tiết: JSON-LD ProductGroup (thương hiệu, điểm sao, thông số) chứa nhiều biến thể
  (màu, dung lượng) trong hasVariant, mỗi biến thể có sku và giá riêng -> mỗi biến thể một
  PriceEvent. Trang không có biến thể thì dùng JSON-LD Product.

Quy ước:
- product_id = slug trong URL ("oppo-reno16f-5g-8gb-128gb"); sku_id = productCode / sku biến thể.
- rating_count = totalRate (số lượt chấm sao); review_count = None (sàn không tách riêng).
- sold_count ước lượng từ chữ ("60,2k" -> 60200), chữ gốc giữ ở sold_count_raw.
- in_stock = isCanBuy; stock_status_raw = số tồn kho sàn công bố (stock).
- Thế Giới Di Động tự bán hàng: seller_id, seller_name = None.
"""

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
    parse_count_text,
    slug_from_url,
    to_int,
    to_rating,
    to_str,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.thegioididong.com"


def can_buy(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


class TgddPlatform(Platform):
    name = "tgdd"

    async def discover(self, category: str):
        """Không phân trang phía server: mỗi danh mục ("dtdd-samsung") chỉ có một trang."""
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
        products = longest_object_list(next_flight_text(raw.body), "products", "productPrices")
        if not products:
            logger.warning("No product data in %s (page layout changed?)", raw.ref.url)
        events: list[Event] = []
        for item in products:
            path = to_str(item.get("url"))
            url = f"{BASE_URL}/{path.lstrip('/')}" if path else None
            prices = item.get("productPrices") or []
            price = as_dict(prices[0]) if prices else {}
            sold_raw = to_str(item.get("totalSold"))
            try:
                events.append(PriceEvent(
                    platform=self.name,
                    product_id=slug_from_url(url),
                    sku_id=to_str(item.get("productCode")),
                    url=url,
                    name_raw=item.get("name"),
                    source="listing",
                    price_current=to_int(price.get("price")),
                    price_current_raw=to_str(price.get("price")),
                    price_original=to_int(price.get("sysPrice")),
                    price_original_raw=to_str(price.get("sysPrice")),
                    in_stock=can_buy(price.get("isCanBuy")),
                    stock_status_raw=to_str(price.get("stock")),
                    rating_avg=to_rating(item.get("rate")),
                    rating_count=to_int(item.get("totalRate")),
                    sold_count=parse_count_text(sold_raw),
                    sold_count_raw=sold_raw,
                    event_time=raw.fetched_at,
                    crawler_version=self.crawler_version,
                    raw_hash=raw_hash,
                ))
            except ValidationError as exc:
                logger.warning("Skip listing item %s: %s", path, exc.errors()[0]["msg"])
        return events

    def _parse_detail(self, soup: BeautifulSoup, raw: RawResponse, raw_hash: str) -> list[Event]:
        group = json_ld(soup, "ProductGroup")
        if group:
            variants = [as_dict(v) for v in group.get("hasVariant") or []]
            # Bỏ phần tử rỗng (không sku, không giá) mà sàn đôi khi chèn vào hasVariant
            variants = [v for v in variants if v.get("sku") or first_offer(v).get("price") is not None]
        else:
            group = json_ld(soup, "Product")
            variants = [group] if group else []
        if not group:
            logger.warning("Skip %s: no JSON-LD ProductGroup/Product", raw.ref.url)
            return []

        url = raw.ref.url.split("?")[0]
        product_id = slug_from_url(url)
        rating = as_dict(group.get("aggregateRating"))
        crumbs = json_ld(soup, "BreadcrumbList").get("itemListElement") or []
        names = [as_dict(c).get("name") or as_dict(as_dict(c).get("item")).get("name") for c in crumbs]
        category = " > ".join(n for n in names[1:-1] if n) or to_str(group.get("category"))
        specs = {
            as_dict(p).get("name"): as_dict(p).get("value")
            for p in group.get("additionalProperty") or []
            if as_dict(p).get("name")
        }
        image = first_image(group.get("image")) or (first_image(variants[0].get("image")) if variants else None)

        events: list[Event] = []
        try:
            events.append(ProductEvent(
                platform=self.name,
                product_id=product_id,
                url=url,
                name_raw=group.get("name"),
                brand_raw=to_str(as_dict(group.get("brand")).get("name")),
                category_raw=category,
                specs_raw=specs,
                image_url=image,
                rating_avg=to_rating(rating.get("ratingValue")),
                rating_count=to_int(rating.get("ratingCount") or rating.get("reviewCount")),
                event_time=raw.fetched_at,
                crawler_version=self.crawler_version,
                raw_hash=raw_hash,
            ))
        except ValidationError as exc:
            logger.warning("Skip detail %s: %s", raw.ref.url, exc.errors()[0]["msg"])
            return []

        for variant in variants:
            offer = first_offer(variant)
            price_raw = to_str(offer.get("price"))
            try:
                events.append(PriceEvent(
                    platform=self.name,
                    product_id=product_id,
                    sku_id=to_str(variant.get("sku")),
                    url=to_str(offer.get("url")) or url,
                    name_raw=variant.get("name") or group.get("name"),
                    source="detail",
                    price_current=to_int(price_raw),
                    price_current_raw=price_raw,
                    in_stock=availability_to_bool(offer.get("availability")),
                    stock_status_raw=to_str(offer.get("availability")),
                    rating_avg=to_rating(rating.get("ratingValue")),
                    rating_count=to_int(rating.get("ratingCount") or rating.get("reviewCount")),
                    event_time=raw.fetched_at,
                    crawler_version=self.crawler_version,
                    raw_hash=raw_hash,
                ))
            except ValidationError as exc:
                logger.warning("Skip variant %s of %s: %s", variant.get("sku"), raw.ref.url, exc.errors()[0]["msg"])
        return events
