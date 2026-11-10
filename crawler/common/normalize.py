"""Chuẩn hoá và kiểm tra chất lượng một event, dùng chung cho mọi job đọc Bronze (README-2A mục 10.1).

Hàm thuần: không đọc file, không gọi mạng, không dùng Spark. Nhờ vậy cùng một logic chạy được trong
batch (rebuild_silver, build_silver_sample) và streaming (tuần 4). KHÔNG viết bản thứ hai ở nơi khác.

Đầu vào luôn là dict (một dòng JSONL đã json.loads), không phải PriceEvent: Bronze có thể chứa bản ghi
hỏng mà pydantic sẽ từ chối, trong khi ở đây cần đọc được nó để ghi lý do vào quarantine.
"""

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

# Các sàn hợp lệ. lazada có code trong repo (máy của thành viên khác chạy) nên vẫn tính là hợp lệ.
KNOWN_PLATFORMS = frozenset({"tiki", "cellphones", "hoangha", "fptshop", "tgdd", "lazada"})

# Khoảng giá chấp nhận (VND). Ngoài khoảng này gần như chắc chắn là parse sai, không phải giá thật.
PRICE_MIN = 1_000
PRICE_MAX = 2_000_000_000

# Giờ sàn Việt Nam, dùng khi gặp thời gian không có múi giờ
VN_TZ = timezone(timedelta(hours=7))

# errors: bản ghi hỏng thật -> quarantine
MISSING_EVENT_ID = "MISSING_EVENT_ID"
MISSING_PRODUCT_ID = "MISSING_PRODUCT_ID"
BAD_TIMESTAMP = "BAD_TIMESTAMP"
UNKNOWN_PLATFORM = "UNKNOWN_PLATFORM"
PRICE_OUT_OF_RANGE = "PRICE_OUT_OF_RANGE"
# Dòng không đọc được thành JSON object (ví dụ dòng cuối bị cắt khi crawler chết giữa chừng).
# validate không trả mã này (chưa có event để kiểm tra); job đọc dòng thô tự gán rồi đưa vào quarantine.
BAD_JSON = "BAD_JSON"

# flags: bản ghi hợp lệ nhưng thiếu dữ liệu -> vẫn vào Silver, ghi vào cột dq_flags
MISSING_PRICE = "MISSING_PRICE"
ZERO_PRICE = "ZERO_PRICE"

# Các dạng chữ giá gặp trong landing/ (sau khi bỏ ký hiệu tiền tệ và khoảng trắng):
_DIGITS = re.compile(r"\d+")                          # 25990000 (fptshop, tgdd, tiki)
_DOT_GROUPS = re.compile(r"\d{1,3}(?:\.\d{3})+")      # 25.990.000 (cellphones, hoangha)
_COMMA_GROUPS = re.compile(r"\d{1,3}(?:,\d{3})+")     # 25,990,000 (hoangha, trang chi tiết)
_DECIMAL = re.compile(r"\d+\.\d{1,2}")                # 25990000.0 (JSON-LD của hoangha, tgdd)
# Ký hiệu tiền tệ cần bỏ trước khi so khớp. Chữ "vnđ" đứng trước "đ" để không bỏ sót chữ "vn".
_CURRENCY = re.compile(r"vnđ|vnd|₫|đ", re.IGNORECASE)


def parse_price_vnd(raw: str | int | float | None) -> int | None:
    """'25.990.000₫', '25,990,000 VND', '25990000.0', 25990000 -> 25990000.

    Trả None nếu không đọc được ('Liên hệ', chuỗi rỗng, 'lowPrice=...;highPrice=...' của Hoàng Hà).
    Không raise. Giá 0 vẫn trả 0: quyết định 0 nghĩa là gì là việc của validate, không phải ở đây.
    """
    # bool là lớp con của int trong Python (True == 1), phải loại trước
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw >= 0 else None
    if isinstance(raw, float):
        # Chỉ nhận số nguyên dạng float (7500000.0); 1.5 không phải giá VND hợp lệ
        return int(raw) if raw.is_integer() and raw >= 0 else None
    if not isinstance(raw, str):
        return None

    # Bỏ ký hiệu tiền tệ và mọi khoảng trắng (kể cả khoảng trắng không ngắt dòng \xa0)
    text = _CURRENCY.sub("", raw)
    text = "".join(text.split())
    if not text:
        return None

    # Thứ tự kiểm tra không quan trọng: 4 mẫu loại trừ nhau. Phần thập phân chỉ có 1-2 chữ số,
    # còn nhóm hàng nghìn luôn đúng 3 chữ số, nên "390.000" luôn là 390 nghìn, không phải 390,0.
    if _DIGITS.fullmatch(text):
        return int(text)
    if _DOT_GROUPS.fullmatch(text):
        return int(text.replace(".", ""))
    if _COMMA_GROUPS.fullmatch(text):
        return int(text.replace(",", ""))
    if _DECIMAL.fullmatch(text):
        try:
            value = Decimal(text)
        except InvalidOperation:
            return None
        # "7500000.0" -> 7500000; "1.5" -> None (VND không có số lẻ, có lẻ là đọc nhầm trường)
        return int(value) if value == value.to_integral_value() else None
    return None


def to_utc(ts: str | datetime | None) -> datetime | None:
    """Đưa thời gian về UTC có timezone. Không đọc được thì trả None, không raise.

    Chuỗi hoặc datetime không có timezone thì coi là giờ Việt Nam (UTC+7) rồi đổi sang UTC.
    Dữ liệu crawler luôn có đuôi Z, nhánh này chỉ để phòng dữ liệu từ nguồn khác.
    Không cắt microsecond: crawler đã cắt tới giây rồi, ở đây không đổi độ chính xác.
    """
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        text = ts.strip()
        # Python 3.10 (image Spark) không đọc được đuôi "Z" -> đổi thành "+00:00"; 3.11 thì đọc được cả hai
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return dt.astimezone(timezone.utc)


def compute_discount_pct(current: int | None, original: int | None) -> float | None:
    """Phần trăm giảm giá, làm tròn 2 chữ số: (original - current) / original * 100.

    None nếu thiếu một trong hai, original <= 0, hoặc current > original
    (Hoàng Hà có ~6.000 event giá gốc thấp hơn giá bán: coi là dữ liệu sàn sai, không tính).
    current == original -> 0.0 (có giá gốc nhưng không giảm).
    """
    if current is None or original is None or original <= 0 or current < 0 or current > original:
        return None
    return round((original - current) / original * 100, 2)


def _price_of(event: dict, field: str) -> int | None:
    """Giá dạng số của một trường (price_current / price_original).

    Số do crawler parse là chuẩn. Chỉ khi số rỗng mới đọc lại từ chữ gốc <field>_raw
    (ví dụ tgdd trang chi tiết: price_current null nhưng price_current_raw = "7500000.0").
    """
    value = parse_price_vnd(event.get(field))
    if value is None:
        value = parse_price_vnd(event.get(f"{field}_raw"))
    return value


def validate(event: dict) -> tuple[list[str], list[str]]:
    """Trả về (errors, flags). Cả hai rỗng nghĩa là bản ghi đầy đủ.

    errors — bản ghi hỏng thật, đi vào quarantine:
    - MISSING_EVENT_ID
    - MISSING_PRODUCT_ID
    - BAD_TIMESTAMP
    - UNKNOWN_PLATFORM     (ngoài tiki, cellphones, hoangha, fptshop, tgdd, lazada)
    - PRICE_OUT_OF_RANGE   (có giá nhưng ngoài 1.000 đến 2.000.000.000 VND)

    flags — bản ghi hợp lệ nhưng thiếu dữ liệu, VẪN vào Silver, ghi vào cột dq_flags:
    - MISSING_PRICE        (sản phẩm "Liên hệ", chưa có giá; chữ gốc vẫn ở price_current_raw)
    - ZERO_PRICE           (sàn ghi giá 0: fptshop, cellphones, tgdd; Silver đổi giá thành null)

    Kiểm tra giá chỉ áp dụng cho price event. Thứ tự trong list cố định để chạy lại ra cùng kết quả.
    """
    errors: list[str] = []
    flags: list[str] = []

    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        errors.append(MISSING_EVENT_ID)
    product_id = event.get("product_id")
    if not isinstance(product_id, str) or not product_id.strip():
        errors.append(MISSING_PRODUCT_ID)
    if to_utc(event.get("event_time")) is None:
        errors.append(BAD_TIMESTAMP)
    if event.get("platform") not in KNOWN_PLATFORMS:
        errors.append(UNKNOWN_PLATFORM)

    if event.get("event_type") == "price":
        price = _price_of(event, "price_current")
        if price is None:
            flags.append(MISSING_PRICE)
        elif price == 0:
            flags.append(ZERO_PRICE)
        elif not PRICE_MIN <= price <= PRICE_MAX:
            errors.append(PRICE_OUT_OF_RANGE)

    return errors, flags


def normalize_price(event: dict) -> tuple[dict | None, list[str]]:
    """Một price event (dict) -> (dòng silver_price, errors).

    Có errors thì trả (None, errors): job gọi hàm này tự ghi bản ghi vào quarantine kèm errors.
    Dòng trả về chưa có ingest_time và ingest_source: hai cột đó lấy từ metadata Bronze
    (hoặc null / "sample" ở build_silver_sample), không nằm trong event.
    """
    errors, flags = validate(event)
    if errors:
        return None, errors

    event_time = to_utc(event["event_time"])
    # MISSING_PRICE và ZERO_PRICE đều để giá null: 0 không phải giá thật, để 0 sẽ kéo lệch trung bình
    price_current = None if flags else _price_of(event, "price_current")
    # Giá gốc 0 (fptshop ~185 event) cũng không phải giá thật -> null
    price_original = _price_of(event, "price_original") or None

    row = {
        "event_id": event["event_id"],
        "platform": event["platform"],
        "product_id": event["product_id"],
        "product_key": f"{event['platform']}:{event['product_id']}",
        "sku_id": event.get("sku_id"),
        "url": event.get("url"),
        "name_raw": event.get("name_raw"),
        "source": event.get("source"),
        "price_current": price_current,
        "price_current_raw": event.get("price_current_raw"),
        "price_original": price_original,
        "discount_pct": compute_discount_pct(price_current, price_original),
        "currency": event.get("currency") or "VND",
        "in_stock": event.get("in_stock"),
        "seller_id": event.get("seller_id"),
        "seller_name": event.get("seller_name"),
        "rating_avg": event.get("rating_avg"),
        "rating_count": event.get("rating_count"),
        "review_count": event.get("review_count"),
        "sold_count": event.get("sold_count"),
        "event_time": event_time,
        "date": event_time.date(),           # partition theo ngày UTC, khớp landing/ và Bronze
        "crawler_version": event.get("crawler_version"),
        "dq_flags": flags,
    }
    return row, []


def normalize_product(event: dict) -> tuple[dict | None, list[str]]:
    """Một product event (dict) -> (dòng silver_product, errors). Cách dùng giống normalize_price.
    specs_raw (dict) đổi thành chuỗi JSON, sắp xếp key để cùng dữ liệu luôn ra cùng chuỗi.
    """
    # Product event không có giá nên validate không bao giờ trả flags ở đây
    errors, _ = validate(event)
    if errors:
        return None, errors

    event_time = to_utc(event["event_time"])
    specs = event.get("specs_raw")
    row = {
        "event_id": event["event_id"],
        "platform": event["platform"],
        "product_id": event["product_id"],
        "product_key": f"{event['platform']}:{event['product_id']}",
        "url": event.get("url"),
        "name_raw": event.get("name_raw"),
        "brand_raw": event.get("brand_raw"),
        "category_raw": event.get("category_raw"),
        "specs_raw": json.dumps(specs, ensure_ascii=False, sort_keys=True) if specs else None,
        "image_url": event.get("image_url"),
        "rating_avg": event.get("rating_avg"),
        "rating_count": event.get("rating_count"),
        "rating_1_count": event.get("rating_1_count"),
        "rating_2_count": event.get("rating_2_count"),
        "rating_3_count": event.get("rating_3_count"),
        "rating_4_count": event.get("rating_4_count"),
        "rating_5_count": event.get("rating_5_count"),
        "event_time": event_time,
        "crawler_version": event.get("crawler_version"),
    }
    return row, []
