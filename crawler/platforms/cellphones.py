"""Crawler cho CellphoneS, đọc HTML (đã kiểm chứng ngày 2026-11-01).

- Trang danh sách (ví dụ https://cellphones.com.vn/mobile.html): HTML có sẵn ~20 thẻ
  sản phẩm (div.product-item) gồm tên, link, giá bán, giá gốc, điểm sao. KHÔNG phân trang
  phía server (?page=2 trả lại đúng trang 1), phần còn lại nạp bằng JavaScript nên
  không lấy. Muốn nhiều SKU hơn thì thêm trang con theo hãng vào watchlist.
- Trang chi tiết: JSON-LD Product có giá, tồn kho, thương hiệu, sku, điểm sao,
  số đánh giá, bảng thông số (additionalProperty).

Quy ước (đã thống nhất):
- product_id = slug trong URL ("iphone-18-pro"), cũng chính là "sku" CellphoneS khai
  trong JSON-LD. Trang không có id dạng số. sku_id = None.
- CellphoneS tự bán hàng, không phải marketplace: seller_id, seller_name = None.
"""

import logging

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
    parse_vnd,
    slug_from_url,
    text_of,
    to_int,
    to_rating,
    to_str,
)

logger = logging.getLogger(__name__)


class CellphonesPlatform(Platform):
    name = "cellphones"

    def discover(self, category: str):
        raise NotImplementedError("CellphoneS discover is implemented in step 9")

    async def fetch(self, ref: ProductRef) -> RawResponse:
        # Cả trang danh sách lẫn chi tiết đều là một trang HTML: ref.url là link trang
        resp = await self.http.get(ref.url)
        return self.make_raw(ref, resp.status_code, resp.text, resp.headers.get("content-type"))

    def parse(self, raw: RawResponse) -> list[Event]:
        if raw.status_code != 200:
            logger.warning("Skip %s: HTTP %s", raw.ref.url, raw.status_code)
            return []
        soup = BeautifulSoup(raw.body, "html.parser")
        raw_hash = make_raw_hash(raw.body)
        if raw.kind == "listing":
            return self._parse_listing(soup, raw, raw_hash)
        return self._parse_detail(soup, raw, raw_hash)

    def _parse_listing(self, soup: BeautifulSoup, raw: RawResponse, raw_hash: str) -> list[Event]:
        events: list[Event] = []
        # Lưới sản phẩm chính có class product-item; các ô "nổi bật" phía trên (swiper)
        # chỉ là bản lặp lại nên không lấy
        cards = soup.select("div.product-item")
        if not cards:
            logger.warning("No product cards in %s (page layout changed?)", raw.ref.url)
        for card in cards:
            link = card.select_one("a.product__link")
            url = link.get("href") if link else None
            price_raw = text_of(card.select_one(".product__price--show"))
            original_raw = text_of(card.select_one(".product__price--through"))
            try:
                events.append(PriceEvent(
                    platform=self.name,
                    product_id=slug_from_url(url),
                    url=url,
                    name_raw=text_of(card.select_one(".product__name")),
                    source="listing",
                    price_current=parse_vnd(price_raw),
                    price_current_raw=price_raw,
                    price_original=parse_vnd(original_raw),
                    price_original_raw=original_raw,
                    rating_avg=to_rating(text_of(card.select_one(".product__box-rating"))),
                    event_time=raw.fetched_at,
                    crawler_version=self.crawler_version,
                    raw_hash=raw_hash,
                ))
            except ValidationError as exc:
                logger.warning("Skip listing card %s: %s", url, exc.errors()[0]["msg"])
        return events

    def _parse_detail(self, soup: BeautifulSoup, raw: RawResponse, raw_hash: str) -> list[Event]:
        product = json_ld(soup, "Product")
        if not product:
            logger.warning("Skip %s: no JSON-LD Product", raw.ref.url)
            return []
        offer = first_offer(product)
        rating = as_dict(product.get("aggregateRating"))
        url = to_str(offer.get("url")) or raw.ref.url
        product_id = to_str(product.get("sku")) or slug_from_url(url)

        # Breadcrumb: bỏ phần tử đầu (trang chủ) và cuối (tên sản phẩm)
        crumbs = json_ld(soup, "BreadcrumbList").get("itemListElement") or []
        names = [as_dict(as_dict(c).get("item")).get("name") or as_dict(c).get("name") for c in crumbs]
        category = " > ".join(n for n in names[1:-1] if n) or None

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
                url=url,
                name_raw=product.get("name"),
                source="detail",
                price_current=parse_vnd(price_raw),
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
