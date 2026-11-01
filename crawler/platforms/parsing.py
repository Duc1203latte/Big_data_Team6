"""Hàm dùng chung để đọc trang HTML và JSON-LD cho các sàn không có API JSON
(CellphoneS, Hoàng Hà, FPT Shop...).

Giống tiki.py: chỉ ép kiểu, đọc không được thì trả None, không bao giờ ném lỗi.
"""

import json
import re
from typing import Any
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Tag

# Dùng lại các hàm ép kiểu đã có của Tiki để mọi sàn ép kiểu giống nhau
from platforms.tiki import as_dict, to_int, to_rating, to_str  # noqa: F401  (re-export)

# Giá dạng "30.990.000đ", "41,490,000 ₫", "38690000". Không khớp ("Liên hệ", "1.2tr") -> None,
# giữ nguyên chữ gốc ở trường *_raw để Silver xử lý.
PRICE_RE = re.compile(r"^\s*(\d{1,3}(?:[.,\s]\d{3})+|\d+)\s*(?:đ|₫|vnđ|vnd)?\s*$", re.IGNORECASE)

# schema.org availability -> in_stock. Giá trị khác (PreOrder...) -> None, không đoán.
AVAILABILITY_MAP = {"instock": True, "outofstock": False, "soldout": False, "discontinued": False}


def parse_vnd(text: Any) -> int | None:
    """Chuỗi giá -> số nguyên VND."""
    if text is None:
        return None
    match = PRICE_RE.match(str(text))
    if not match:
        return None
    return int(re.sub(r"\D", "", match.group(1)))


def text_of(element: Tag | None) -> str | None:
    """Chữ bên trong một thẻ HTML, gộp khoảng trắng; không có thẻ hoặc rỗng -> None."""
    if element is None:
        return None
    return element.get_text(" ", strip=True) or None


def slug_from_url(url: str | None) -> str | None:
    """"https://cellphones.com.vn/iphone-18-pro.html" -> "iphone-18-pro"."""
    if not url:
        return None
    last = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]
    return last.removesuffix(".html") or None


def json_ld(soup: BeautifulSoup, type_name: str) -> dict:
    """Khối JSON-LD đầu tiên có @type = type_name (ví dụ "Product"), không có thì {}."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except ValueError:
            continue
        items = data if isinstance(data, list) else as_dict(data).get("@graph") or [data]
        for item in items:
            if isinstance(item, dict) and item.get("@type") == type_name:
                return item
    return {}


def availability_to_bool(value: Any) -> bool | None:
    """"https://schema.org/InStock" -> True, ".../OutOfStock" -> False, khác -> None."""
    if not value:
        return None
    return AVAILABILITY_MAP.get(str(value).rsplit("/", 1)[-1].lower())


def first_offer(product: dict) -> dict:
    """offers có thể là một object hoặc một list; lấy offer đầu tiên."""
    offers = product.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    return as_dict(offers)


def first_image(image: Any) -> str | None:
    """image có thể là chuỗi hoặc list chuỗi."""
    if isinstance(image, list):
        image = image[0] if image else None
    return to_str(image)
