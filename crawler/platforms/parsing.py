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
    """image có thể là chuỗi, list chuỗi, hoặc ImageObject ({"url": ...})."""
    if isinstance(image, list):
        image = image[0] if image else None
    if isinstance(image, dict):
        image = image.get("url") or image.get("contentUrl")
    return to_str(image)


# ---------------------------------------------------------------------------
# Trang Next.js (FPT Shop, Thế Giới Di Động): dữ liệu nằm trong self.__next_f.push(...)
# ---------------------------------------------------------------------------

NEXT_FLIGHT_RE = re.compile(r"self\.__next_f\.push\((\[.*?\])\)</script>", re.DOTALL)
_json_decoder = json.JSONDecoder()


def next_flight_text(html: str) -> str:
    """Nối các chuỗi trong self.__next_f.push([1, "..."]) thành một văn bản."""
    parts = []
    for match in NEXT_FLIGHT_RE.finditer(html):
        try:
            chunk = json.loads(match.group(1))
        except ValueError:
            continue
        if len(chunk) > 1 and isinstance(chunk[1], str):
            parts.append(chunk[1])
    return "".join(parts)


def longest_object_list(text: str, key: str, required_field: str) -> list[dict]:
    """Trong mọi mảng "key":[...] của văn bản, lấy mảng dài nhất gồm các object có required_field.
    Dùng để lấy danh sách sản phẩm chính (các khối phụ như "Bán chạy" thường chỉ vài sản phẩm)."""
    best: list[dict] = []
    for match in re.finditer(r'"' + re.escape(key) + r'":\[', text):
        try:
            items, _ = _json_decoder.raw_decode(text, match.end() - 1)
        except ValueError:
            continue
        objects = [x for x in items if isinstance(x, dict) and required_field in x]
        if len(objects) > len(best):
            best = objects
    return best


# Số lượng dạng chữ: "Đã bán 60,2k", "Đã bán 726", "1,2tr", "5k+"
COUNT_RE = re.compile(r"(\d[\d.,]*)\s*(k|tr|triệu)?", re.IGNORECASE)
COUNT_MULTIPLIER = {None: 1, "k": 1_000, "tr": 1_000_000, "triệu": 1_000_000}


def parse_count_text(text: Any) -> int | None:
    """Chữ số lượng -> số nguyên (ước lượng khi có k/tr). Không đọc được -> None.

    Có hậu tố k/tr thì dấu "," hoặc "." là dấu thập phân ("60,2k" -> 60200);
    không có hậu tố thì đó là dấu phân cách hàng nghìn ("1.234" -> 1234).
    """
    if text is None:
        return None
    match = COUNT_RE.search(str(text))
    if not match:
        return None
    number, suffix = match.group(1), (match.group(2) or "").lower() or None
    if suffix is None:
        return int(re.sub(r"\D", "", number))
    try:
        value = float(number.replace(",", "."))
    except ValueError:
        return None
    return round(value * COUNT_MULTIPLIER[suffix])
