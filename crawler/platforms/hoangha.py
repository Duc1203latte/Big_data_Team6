"""Crawler cho Hoàng Hà Mobile, đọc HTML.

Sàn đổi giao diện đêm 2026-11-01 -> 02. Parser đọc giao diện mới, giữ selector của
giao diện cũ làm dự phòng (fixture cũ vẫn được test).

Giao diện mới (kiểm chứng 2026-11-02):
- Trang danh sách (https://hoanghamobile.com/dien-thoai-di-dong?p=2): phân trang phía server,
  20 thẻ/trang article[data-product-id], bên trong dùng thuộc tính data-* rõ nghĩa:
  a[data-product-detail-link], [data-product-price], [data-product-last-price] (giá gốc),
  [data-product-availability-label] ("Sẵn hàng", "Liên hệ"...), button[data-sku].
  Nút button[data-category-more]: "Xem thêm N sản phẩm" (N = số còn lại).
- Trang chi tiết: JSON-LD Product (giá, tồn kho, thương hiệu, sku, điểm sao) và
  BreadcrumbList; thông số trong bảng <tr><th scope="row">Tên</th><td>Giá trị</td></tr>.

Giao diện cũ (2026-11-01): thẻ div.pj16-item, giá .price strong, giá gốc .price-last strike,
trạng thái .list-conhang, nút #page-pager a; chi tiết: ul.specs-content, breadcrumb microdata.

Quy ước (đã thống nhất):
- product_id = slug trong URL ("iphone-18-pro-max"); sku_id = mã SKU của Hoàng Hà ("MJXN4X").
- Giá "Hoàng Hà Member" không lưu (schema không có trường), chỉ lấy giá công khai.
- Hoàng Hà tự bán hàng: seller_id, seller_name = None.
"""

import logging
import math
import re
from urllib.parse import urljoin

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

BASE_URL = "https://hoanghamobile.com"

# Chữ trạng thái hàng trên thẻ sản phẩm -> in_stock. Chữ khác ("Liên hệ"...) -> None.
STOCK_TEXT_MAP = {"sẵn hàng": True, "còn hàng": True, "hết hàng": False}


def listing_url(category: str, page: int) -> str:
    """Trang 1 không có tham số, trang N là ?p=N (đúng như link "Xem thêm" của sàn)."""
    return f"{BASE_URL}/{category}" + (f"?p={page}" if page > 1 else "")


def product_cards(soup: BeautifulSoup) -> list:
    """Thẻ sản phẩm của trang danh sách: giao diện mới, không có thì thử giao diện cũ."""
    return soup.select("article[data-product-id]") or soup.select("div.pj16-item")


def page_count(soup: BeautifulSoup) -> int:
    """Số trang của danh mục, tính từ trang 1: số thẻ sản phẩm và nút "Xem thêm N sản phẩm"."""
    cards = len(product_cards(soup))
    if cards == 0:
        return 0
    more_button = soup.select_one("button[data-category-more]") or soup.select_one("#page-pager a")
    more = text_of(more_button) or ""
    match = re.search(r"(\d[\d.,]*)\s*sản phẩm", more)
    if not match:
        return 1
    remaining = int(re.sub(r"\D", "", match.group(1)))
    return 1 + math.ceil(remaining / cards)


class HoanghaPlatform(Platform):
    name = "hoangha"

    async def discover(self, category: str):
        """Gọi trang 1 để biết số trang, rồi trả về ref của từng trang (category: "dien-thoai-di-dong")."""
        first = ProductRef(platform=self.name, kind="listing", url=listing_url(category, 1),
                           category=category, page=1)
        raw = await self.fetch(first)
        if raw.status_code != 200:
            logger.warning("Cannot discover %s: HTTP %s", category, raw.status_code)
            return
        pages = page_count(BeautifulSoup(raw.body, "html.parser"))
        logger.info("Discovered %s: %d pages", category, pages)
        for page in range(1, pages + 1):
            yield ProductRef(platform=self.name, kind="listing", url=listing_url(category, page),
                             category=category, page=page)

    async def fetch(self, ref: ProductRef) -> RawResponse:
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
        cards = product_cards(soup)
        if not cards:
            logger.warning("No product cards in %s (page layout changed?)", raw.ref.url)
        for card in cards:
            if card.name == "article":
                # Giao diện mới. Có 2 link tới trang chi tiết: link ảnh (không chữ) và link tên.
                links = card.select("a[data-product-detail-link]")
                link = next((a for a in links if text_of(a)), links[0] if links else None)
                title_tag = card.select_one("[data-product-title]")
                name = text_of(link) or (title_tag.get("data-product-title") if title_tag else None)
                price_raw = text_of(card.select_one("[data-product-price]"))
                original_raw = text_of(card.select_one("[data-product-last-price]"))   # rỗng nếu không giảm
                stock_raw = text_of(card.select_one("[data-product-availability-label]"))
            else:
                # Giao diện cũ
                link = card.select_one("h3 a")
                name = text_of(link)
                price_raw = text_of(card.select_one(".price strong"))
                original_raw = text_of(card.select_one(".price-last strike"))
                stock_raw = text_of(card.select_one(".list-conhang"))
            url = urljoin(BASE_URL, link["href"]) if link and link.get("href") else None
            sku_tag = card.select_one("[data-sku]")
            try:
                events.append(PriceEvent(
                    platform=self.name,
                    product_id=slug_from_url(url),
                    sku_id=to_str(sku_tag.get("data-sku")) if sku_tag else None,
                    url=url,
                    name_raw=name,
                    source="listing",
                    price_current=parse_vnd(price_raw),
                    price_current_raw=price_raw,
                    price_original=parse_vnd(original_raw),
                    price_original_raw=original_raw,
                    in_stock=STOCK_TEXT_MAP.get((stock_raw or "").lower()),
                    stock_status_raw=stock_raw,
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
        url = to_str(product.get("url")) or to_str(offer.get("url")) or raw.ref.url
        product_id = slug_from_url(url)
        sku_id = to_str(product.get("sku"))

        # Breadcrumb: JSON-LD (giao diện mới) hoặc microdata (giao diện cũ).
        # Bỏ "Trang chủ" ở đầu và tên sản phẩm ở cuối (nếu có).
        crumbs = json_ld(soup, "BreadcrumbList").get("itemListElement") or []
        names = [as_dict(c).get("name") or as_dict(as_dict(c).get("item")).get("name") for c in crumbs]
        if not names:
            names = [text_of(t) for t in soup.select('[itemtype*="BreadcrumbList"] [itemprop="name"]')]
        names = [n for n in names[1:] if n]
        if names and names[-1] == product.get("name"):
            names = names[:-1]
        category = " > ".join(names) or None

        # Thông số, giữ nguyên chữ:
        #   mới: <tr><th scope="row">Tên</th><td>Giá trị</td></tr>
        #   cũ:  <li><strong>Tên</strong><span>Giá trị</span></li> trong ul.specs-content
        specs = {}
        for row in soup.select('tr'):
            key_tag, value_tag = row.find("th", scope="row"), row.find("td")
            if key_tag and value_tag and text_of(key_tag):
                specs[text_of(key_tag)] = text_of(value_tag)
        for item in soup.select("ul.specs-content li"):
            key = text_of(item.find("strong"))
            if key:
                specs[key] = text_of(item.find("span"))

        # Giao diện cũ: Offer có "price". Giao diện mới: AggregateOffer (nhiều biến thể) chỉ có
        # lowPrice/highPrice -> price_current = lowPrice (khớp giá hiện ở trang danh sách),
        # chữ gốc ghi cả hai đầu để Silver biết đây là khoảng giá.
        if offer.get("price") is not None:
            price_raw = to_str(offer.get("price"))
            price_value = offer.get("price")
        elif offer.get("lowPrice") is not None:
            price_raw = f"lowPrice={offer.get('lowPrice')};highPrice={offer.get('highPrice')}"
            price_value = offer.get("lowPrice")
        else:
            price_raw = price_value = None
        price_current = to_int(price_value) if isinstance(price_value, (int, float)) else parse_vnd(price_value)
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
                sku_id=sku_id,
                url=url,
                name_raw=product.get("name"),
                source="detail",
                price_current=price_current,
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
