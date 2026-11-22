"""Test cho common/product_text.py. Chạy: pytest tests/test_product_text.py -q

Các ca dưới đây lấy từ hướng dẫn Bước 1 (tên thật trong Silver). Gặp tên xử lý sai thì THÊM CA VÀO ĐÂY
trước, rồi mới sửa code. Mục tiêu: ít nhất ~20 ca.
"""

import pytest

from common.product_text import (
    chip_config,
    chip_tier,
    clean_name,
    coarse_category,
    extract_capacity,
    model_numbers,
    normalize_brand,
    variant_tokens,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("iPhone 16 Pro Max 512GB | Chính hãng VN/A", "iphone 16 pro max 512gb"),
        ("Điện Thoại Samsung Galaxy A17 5G 8GB/128GB - Hàng Chính Hãng", "samsung galaxy a17 5g 8gb/128gb"),
        ("iPhone 15 128GB Xanh dương nhạt", "iphone 15 128gb"),
        ("Kính dán màn hình ZAGG Plus Edge cho iPhone 15 - Chính hãng",
         "kính dán màn hình zagg plus edge cho iphone 15"),
        # TODO: thêm ca của bạn
    ],
)
def test_clean_name(raw, expected):
    assert clean_name(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("iPhone 15 128GB", (None, 128)),
        ("Samsung Galaxy A17 5G 8GB/128GB", (8, 128)),
        ("MacBook Air M5 16GB/1TB", (16, 1024)),
        ("Loa JBL Charge 6", (None, None)),
        ("MacBook Air 13 M5 2026 10CPU/8GPU/16GB/512GB", (16, 512)),
        ("Pin sạc dự phòng MoveSpeed PB07 20000mAh 22.5W", (None, None)),
        ("Cáp HDMI 2.0 4K 18Gbps", (None, None)),
        ("Nubia A56 4GB", (4, None)),
        ("iPhone 16 Pro Max 1TB | Chính hãng VN/A", (None, 1024)),
        ("TECNO SPARK Go 3 4GB+4GB/64GB", (4, 64)),
    ],
)
def test_extract_capacity(raw, expected):
    assert extract_capacity(raw) == expected


@pytest.mark.parametrize(
    "brand_raw, name, expected",
    [
        ("iPhone (Apple)", "iPhone 15 128GB", "apple"),
        (None, "Ốp lưng Mipow cho iPhone 15", "mipow"),
        ("['xiaomi']", "Xiaomi Redmi Note 15", "xiaomi"),
        ("Macbook", "MacBook Air 13 M5", "apple"),
        ("No Brand", "Cáp sạc cho iPhone", None),
        ("OEM", "Ốp lưng trong suốt dành cho iPhone 16", None),
        (None, "Ốp Lưng Chống Sốc OtterBox Defender Series cho iPhone 15", "otterbox"),
        (None, "Pin sạc dự phòng Xiaomi 20000mAh 22.5W", "xiaomi"),
        (None, "Laptop Asus Vivobook 15", "asus"),
        (None, "Cáp HDMI 1M 2.0 4K", None),
        ("Hp Hyperx", "Tai nghe HyperX Cloud", "hyperx"),
    ],
)
def test_normalize_brand(brand_raw, name, expected):
    assert normalize_brand(brand_raw, name) == expected


@pytest.mark.parametrize(
    "category_raw, name, expected",
    [
        (None, "Ốp Lưng Trong Suốt MagSafe cho iPhone 16", "accessory"),
        ("Điện thoại", "iPhone 16 Plus 128GB", "phone"),
        ("Phụ kiện > Pin + màn hình iPhone", "Thay màn hình GX OLED iPhone 16 Plus", "accessory"),
        ("Điện thoại, smartphone chính hãng, giá rẻ, trả góp 0% lãi suất > Điện thoại Samsung",
         "Samsung Galaxy A17 5G", "phone"),
        ("Điện Thoại - Máy Tính Bảng > Điện thoại Smartphone", "Điện thoại Samsung Galaxy A06", "phone"),
        ("Đồng hồ > Dây Đồng Hồ", "Dây đeo Apple Watch 45mm", "accessory"),
        ("Tablet > iPad", "iPad Air M4 11 inch", "tablet"),
        (None, "Samsung Galaxy Tab S10 FE", "tablet"),
        (None, "Microphone Dành Cho Iphone Mini RMK-K01", "audio"),
        (None, "Thiết bị chuyển đổi USB 2.0 ra LAN", "other"),
        (None, "(IPHONE 13) Ốp lưng Likgus lưng trong viền màu", "accessory"),
        (None, "Ugreen 60548 6.8inch Giá đỡ điện thoại", "accessory"),
        ("Điện Thoại - Máy Tính Bảng > Điện thoại Smartphone",
         "Điện Thoại Samsung Galaxy A17 5G, Camera 50MP, Kính Cường Lực Gorilla Victus", "phone"),
    ],
)
def test_coarse_category(category_raw, name, expected):
    assert coarse_category(category_raw, name) == expected


@pytest.mark.parametrize(
    "clean, expected",
    [
        ("iphone 16 pro max 512gb", {"pro", "max"}),
        ("iphone 16 128gb", set()),
        ("iphone 16e 128gb", set()),  # "16e" là số đời máy, xem test_model_numbers
        ("xiaomi redmi note 15 pro+ 5g 12gb/256gb", {"pro", "plus", "5g"}),
        ("samsung galaxy s24 fe 5g", {"fe", "5g"}),
        ("iphone 15 promax", {"pro", "max"}),
        ("macbook air 13 m5 2026 10cpu/8gpu/16gb/512gb", {"air"}),
    ],
)
def test_variant_tokens(clean, expected):
    assert variant_tokens(clean) == frozenset(expected)

@pytest.mark.parametrize(
    "clean, expected",
    [
        ("iphone 16 pro max 512gb", {"16"}),
        ("iphone 16e 128gb", {"16e"}),
        ("samsung galaxy s24 fe 5g", {"s24"}),
        ("samsung galaxy a07s 4gb/64gb", {"a07s"}),
        ("macbook air 13 m5 2026 10cpu/8gpu/16gb/512gb/70w", {"13", "m5"}),
        ("pin sạc dự phòng 20000mah 22.5w", set()),
        ("hp omnibook 5 16-ag1069au bz7t1pa", {"5", "16-ag1069au", "bz7t1pa"}),
    ],
)
def test_model_numbers(clean, expected):
    assert model_numbers(clean) == frozenset(expected)

@pytest.mark.parametrize(
    "clean, expected",
    [
        ("macbook air 13 m5 2026 10cpu/8gpu/16gb/512gb", {"10cpu", "8gpu"}),
        ("macbook pro 14 inch m5 10cpu 10gpu 24gb/1tb", {"10cpu", "10gpu"}),
        ("iphone 16 pro max 512gb", set()),
    ],
)
def test_chip_config(clean, expected):
    assert chip_config(clean) == frozenset(expected)


@pytest.mark.parametrize(
    "clean, expected",
    [
        ("macbook pro 14 m5 pro 2026 15cpu/16gpu/24gb/1tb", "m5pro"),
        ("macbook pro 14 inch m5 24gb/1tb", "m5"),
        ("macbook pro 14 inch m5 max 36gb/2tb", "m5max"),
        ("iphone 16 pro max 512gb", None),
    ],
)
def test_chip_tier(clean, expected):
    assert chip_tier(clean) == expected
