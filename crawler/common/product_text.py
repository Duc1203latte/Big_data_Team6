"""Chuẩn hoá tên sản phẩm cho entity_resolution (Gold, tuần 3) - khâu B trong hướng dẫn Bước 1.

Chỉ gồm hàm thuần xử lý chuỗi: không đọc file, không gọi mạng, để test dễ.
Căn cứ dữ liệu: docs/gold_step0_findings.md.
"""

from __future__ import annotations

import re
import unicodedata


# TODO B1: cụm quảng cáo cần bỏ. Sắp cụm DÀI trước, ngắn sau.
AD_PHRASES: list[str] = [
    "chính hãng apple việt nam",
    "chính hãng vn/a",
    "chính hãng fpt",
    "hàng chính hãng",
    "chính hãng",
    "flash sale",
    "new",
    "hot",
]

# TODO B1: tiền tố loại ở đầu tên ("điện thoại", "laptop", ...)
TYPE_PREFIXES: list[str] = [
    "điện thoại",
    "máy tính bảng",
    "laptop",
    "tai nghe"
]

# TODO B1: màu (tgdd ghi màu ở cuối tên)
COLORS: set[str] = {
    "đen",
    "trắng",
    "xanh",
    "dương",
    "lá",
    "hồng",
    "tím",
    "vàng",
    "xám",
    "bạc",
    "đỏ",
    "cam",
    "titan",
    "nhạt",
    "đậm",
    "-",
}

# Biên dịch regex 1 lần khi import, không biên dịch lại cho từng tên.
_AD_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p) for p in sorted(AD_PHRASES, key=len, reverse=True)) + r")\b"
)
_PREFIX_RE = re.compile(r"^(?:" + "|".join(re.escape(p) for p in TYPE_PREFIXES) + r")\b")
_PUNCT_RE = re.compile(r"[|()\[\]_,]")
_LONE_DASH_RE = re.compile(r"\s-\s")

NO_BRAND: set[str] = {"no brand", "nobrand", "oem", "điện máy xanh", "khác", "other", ""}

# TODO B3: quy đổi brand_raw (chữ thường) -> brand chuẩn, vd "iphone (apple)" -> "apple"
BRAND_ALIASES: dict[str, str] = {
    "macbook": "apple", "iphone": "apple", "ipad": "apple",
    "hp hyperx": "hyperx", "nothing phone": "nothing", "tp-link": "tp link",
    "soundcore": "anker", "edra": "e-dra",
}

# TODO B3: brand đã biết, và từ khoá ngầm định brand ("iphone" -> "apple", "galaxy" -> "samsung")
KNOWN_BRANDS: list[str] = [
    "apple", "samsung", "xiaomi", "oppo", "vivo", "realme", "honor", "huawei", "nokia", "motorola",
    "tecno", "nubia", "masstel", "nothing", "asus", "acer", "dell", "hp", "lenovo", "msi", "razer",
    "logitech", "ugreen", "baseus", "anker", "hoco", "mipow", "zagg", "uniq", "esr", "spigen",
    "nillkin", "otterbox", "pitaka", "zeelot", "tomtoc", "pisen", "innostyle", "jbl", "sony",
    "marshall", "harman kardon", "edifier", "sennheiser", "soundmax", "soundpeats", "havit",
    "microlab", "philips", "garmin", "amazfit", "energizer", "sandisk", "rapoo", "dareu", "wiwu",
    "choetech", "remax", "tp link", "dji", "hyperx", "kingston",
]
MODEL_TO_BRAND: dict[str, str] = {
    "iphone": "apple", "ipad": "apple", "macbook": "apple", "airpods": "apple", "apple watch": "apple",
    "galaxy": "samsung", "redmi": "xiaomi", "poco": "xiaomi",
    "vivobook": "asus", "zenbook": "asus", "ideapad": "lenovo", "thinkpad": "lenovo", "legion": "lenovo",
    "victus": "hp", "pavilion": "hp", "inspiron": "dell", "vostro": "dell", "latitude": "dell",
}

_NAME_BRAND: dict[str, str] = {**{b: b for b in KNOWN_BRANDS}, **MODEL_TO_BRAND}
_BRAND_IN_NAME_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in sorted(_NAME_BRAND, key=len, reverse=True)) + r")\b"
)

_FOR_RE = re.compile(r"\b(?:dành cho|cho|for)\b")

# TODO B4: từ ở ĐẦU tên cho biết là phụ kiện ("ốp", "bao da", "cáp", ...)
ACCESSORY_PREFIXES: list[str] = [
    "ốp", "bao da", "bao", "case", "túi", "hộp đựng", "miếng dán", "miếng lót", "dán", "kính",
    "cường lực", "cáp", "dây", "củ sạc", "cốc sạc", "sạc", "pin", "adapter", "hub", "cổng chuyển",
    "đầu chuyển", "bộ chuyển", "đầu đọc", "giá đỡ", "đế", "bút", "thay", "thẻ nhớ", "chuột",
    "bàn phím", "nút", "vỏ", "bộ dán", "phụ kiện", "kệ", "gậy", "lót", "tẩu",
]

ACCESSORY_WORDS: list[str] = [
    "ốp lưng", "bao da", "miếng dán", "kính cường lực", "cường lực", "giá đỡ", "giá kẹp",
    "gậy", "dây đeo", "thay màn", "bảo vệ", "cáp", "kệ", "tẩu sạc", "lót chuột",
]

# Phân loại các thiết bị
DEVICE_PREFIXES: list[str] = ["điện thoại", "máy tính bảng", "laptop", "máy tính xách tay"]

# RULE cụ thể phân loại
CATEGORY_RULES: list[tuple[str, str]] = [
    ("điện thoại - máy tính bảng > máy tính bảng", "tablet"),       # tiki
    ("điện thoại - máy tính bảng", "phone"),                         # tiki
    ("laptop - máy vi tính", "laptop"),                              # tiki
    ("thiết bị số - phụ kiện số > thiết bị âm thanh", "audio"),      # tiki
    ("thiết bị số - phụ kiện số > thiết bị đeo thông minh và phụ kiện > phụ kiện", "accessory"),
    ("thiết bị số - phụ kiện số > thiết bị đeo thông minh", "watch"),
    ("thiết bị số", "accessory"),                                    # tiki: phần còn lại
    ("điện thoại", "phone"),           # cellphones, fptshop, hoangha, tgdd ("điện thoại, smartphone...")
    ("máy tính bảng", "tablet"),
    ("tablet", "tablet"),
    ("laptop", "laptop"),
    ("đồng hồ", "watch"),
    ("âm thanh", "audio"),
    ("tai nghe", "audio"),
    ("loa", "audio"),
    ("phụ kiện", "accessory"),
]

# Không có catagory_raw -> đoán từ tên
NAME_RULES: list[tuple[tuple[str, ...], str]] = [
    (("máy tính bảng", "ipad", "galaxy tab", "tablet"), "tablet"),
    (("laptop", "macbook", "vivobook", "zenbook", "ideapad", "thinkpad"), "laptop"),
    (("đồng hồ", "watch"), "watch"),
    (("tai nghe", "loa", "airpods", "micro", "microphone", "mic"), "audio"),
    (("điện thoại", "iphone", "galaxy", "redmi", "poco"), "phone"),
]

_ACC_PREFIX_RE = re.compile(
    r"^(?:" + "|".join(re.escape(p) for p in sorted(ACCESSORY_PREFIXES, key=len, reverse=True)) + r")\b"
)
_DEVICE_RE = re.compile(r"^(?:" + "|".join(re.escape(p) for p in DEVICE_PREFIXES) + r")\b")
# Bỏ "(...)" hoặc "[...]" ở ĐẦU tên: "(IPHONE 13) Ốp lưng..." -> "Ốp lưng..."
_LEAD_BRACKET_RE = re.compile(r"^\s*[\(\[][^\)\]]*[\)\]]\s*")

# TODO B5: hậu tố phân biệt model
VARIANT_WORDS: set[str] = {"pro", "max", "plus", "ultra", "mini", "lite", "fe", "air", "5g", "4g", "lte"}

# Tách từ theo dấu cách hoặc "/" ("8gb/128gb" -> "8gb", "128gb")
_TOKEN_SPLIT_RE = re.compile(r"[\s/]+")

# B6: thông số (số + đơn vị) hoặc năm -> không phải số đời máy
_SPEC_RE = re.compile(
    r"^(?:\d+(?:[.,]\d+)?(?:gb|tb|g|mah|w|inch|mm|hz|cpu|gpu|k|m|cm|h|mp)|(?:19|20)\d\d)$"
)

def _contains_word(text: str, words) -> bool:
    """Có từ/cụm nào trong words xuất hiện như một từ nguyên vẹn trong text không."""
    return any(re.search(r"\b" + re.escape(w) + r"\b", text) for w in words)

def clean_name(name: str) -> str:
    """B1. Đưa tên về dạng gọn để embed và so sánh.

    Thứ tự: NFC -> chữ thường -> bỏ cụm quảng cáo -> bỏ tiền tố loại -> bỏ màu ở cuối
    -> thay | ( ) [ ] _ , và "-" đứng riêng bằng dấu cách -> gộp dấu cách.
    Vd: "iPhone 16 Pro Max 512GB | Chính hãng VN/A" -> "iphone 16 pro max 512gb"
    """
    # đưa về NFC (Normalization Form C)
    s = unicodedata.normalize("NFC", name)
    # chữ thường
    s = s.lower()
    # bỏ cụm quảng cáo
    s = _AD_RE.sub(" ", s)
    # xoá các kí tự | ( ) [ ] _ , và dấu "-" đứng riêng
    s = _PUNCT_RE.sub(" ", s)
    s = _LONE_DASH_RE.sub(" ", s)
    s = " ".join(s.split())
    # bỏ tiền tố loại ở đầu tiên
    s = _PREFIX_RE.sub(" ", s)
    # bỏ dần từ chỉ màu ở cuối - chỉ giữ lại ít nhất 1 từ
    words = s.split()
    while len(words) > 0 and words[-1] in COLORS:
        words.pop()
    # ghép lại
    return " ".join(words)

    raise NotImplementedError


_CAPACITY_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(gb|tb)\b")
def extract_capacity(name: str) -> tuple[int | None, int | None]:
    """B2. Trả (ram_gb, storage_gb). TB đổi ra GB (1tb -> 1024).

    2 số: nhỏ là RAM, lớn là bộ nhớ trong. 1 số: >= 32 là bộ nhớ trong, nhỏ hơn thì ...
    (bạn chọn và ghi rõ ở đây). Không bắt 10CPU, 8GPU, 20000mAh, 65W.
    """

    s = unicodedata.normalize("NFC", name).lower()

    # gom dung lượng tìm thấy và đổi ra gb
    sizes: set[int] = set()
    for number, unit in _CAPACITY_RE.findall(s):
        value = float(number.replace(",", "."))
        if unit == "tb":
            value *= 1024
        sizes.add(int(value))

    # không có dung lượng
    if not sizes:
        return (None, None)
    # có cả Ram và bộ nhớ trong
    if len(sizes) >= 2:
        return (min(sizes), max(sizes))
    # chỉ có 1
    only = sizes.pop()
    if only >= 32:
        return (None, only)
    return (only, None)


def normalize_brand(brand_raw: str | None, name: str) -> str | None:
    """B3. Brand chuẩn (chữ thường) hoặc None.

    Có brand_raw thì ƯU TIÊN brand_raw (qua BRAND_ALIASES); "no brand", "oem" -> None.
    Không có thì lấy brand xuất hiện ĐẦU TIÊN trong tên ("Ốp lưng Mipow cho iPhone" -> mipow).
    """
    # brand raw do sàn ghi
    if brand_raw:
        b = unicodedata.normalize("NFC", brand_raw).lower().strip()
        b = b.strip("[]'\" ")
        m = re.search(r"\((.+?)\)", b)
        if m:
            b = m.group(1).strip()
        b = BRAND_ALIASES.get(b, b)
        if b not in NO_BRAND:
            return b

    #tìm trong tên, chỉ ở phần Truước chữ "cho"/"dành cho"/"for"
    s = unicodedata.normalize("NFC", name).lower()
    s = _FOR_RE.split(s, maxsplit=1)[0]
    m = _BRAND_IN_NAME_RE.search(s)
    if m:
        return _NAME_BRAND[m.group(1)]
    return None


def coarse_category(category_raw: str | None, name: str) -> str:
    """B4. Một trong CATEGORIES.

    Dựa vào category_raw (cấp 1; tiki cấp 2; tgdd so "bắt đầu bằng"), sau đó tên được đè:
    tên bắt đầu bằng từ phụ kiện -> "accessory". Không có category_raw thì đoán từ tên.
    """
    n = unicodedata.normalize("NFC", name).lower().strip()
    n = _LEAD_BRACKET_RE.sub("", n)
    head = _FOR_RE.split(n, maxsplit=1)[0]

    # tên bắt đầu bằng từ phụ kiện
    if _ACC_PREFIX_RE.match(n):
        return "accessory"

    # có từ phụ kiện mạnh ở giữa tên, trừ khi tên mở đầu bằng "điện thoại/laptop/..."
    if not _DEVICE_RE.match(n) and _contains_word(head, ACCESSORY_WORDS):
        return "accessory"

    # dựa vào danh mục của sàn: luật đầu tiên khớp thì dùng
    if category_raw:
        c = unicodedata.normalize("NFC", category_raw).lower().strip()
        for prefix, cat in CATEGORY_RULES:
            if c.startswith(prefix):
                return cat

    # không có danh mục (hoặc không khớp luật nào): đoàn từ tên
    for words, cat in NAME_RULES:
        if _contains_word(head, words):
            return cat

    return "other"


def variant_tokens(clean: str) -> frozenset[str]:
    """B5. Tập hậu tố model từ tên ĐÃ clean_name: pro, max, plus ("+" -> plus), ultra, mini,
    lite, fe, 5g, lte, và hậu tố dính số như "16e".

    Vd: "iphone 16 pro max 512gb" -> {"pro", "max"}; "redmi note 15 pro+ 5g" -> {"pro", "plus", "5g"}
    """
    s = clean.replace("+", " plus ").replace("promax", "pro max")
    return frozenset(t for t in _TOKEN_SPLIT_RE.split(s) if t in VARIANT_WORDS)

def model_numbers(clean: str) -> frozenset[str]:
    """B6. Các từ có chữ số chỉ đời máy, từ tên ĐÃ clean_name. Bỏ thông số (128gb, 5g, 65w, 10cpu...) và năm.

    Vd: "iphone 16 pro max 512gb" -> {"16"}; "macbook air 13 m5 2026 10cpu/8gpu/16gb/512gb" -> {"13", "m5"}.
    Luật ghép (khâu E): tập này của sản phẩm A phải chứa, hoặc nằm trong, tập của sản phẩm B.
    """
    s = clean.replace("+", " ")
    return frozenset(
        t for t in _TOKEN_SPLIT_RE.split(s)
        if any(ch.isdigit() for ch in t) and not _SPEC_RE.match(t)
    )