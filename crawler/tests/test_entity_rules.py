"""Test luật cứng và gom cụm (common/entity_rules.py). Các ca lấy từ cặp ứng viên thật ở khâu D."""

from common.entity_rules import Match, check_pair, cluster, cluster_attrs, master_id


def prod(brand="apple", storage=None, ram=None, variants=(), models=("15",), price=21_990_000):
    return {"brand": brand, "storage_gb": storage, "ram_gb": ram, "variants": set(variants),
            "model_nums": set(models), "price_median": price}


def test_same_phone_matches():
    r = check_pair(prod(storage=128), prod(storage=128, price=21_490_000), 1.0, 0.85)
    assert r["matched"]


def test_fptshop_without_storage_matches_by_price():
    # fptshop "iPhone 15" (giá bản 128GB) ~ tgdd "iPhone 15 128GB"
    assert check_pair(prod(storage=None, price=21_490_000), prod(storage=128), 1.0, 0.85)["matched"]
    # bản 512GB đắt hơn nhiều thì luật dung lượng loại
    assert not check_pair(prod(storage=None, price=21_490_000), prod(storage=512, price=31_990_000),
                          1.0, 0.85)["ok_storage"]


def test_different_storage_rejected():
    assert not check_pair(prod(storage=512), prod(storage=256), 1.0, 0.85)["matched"]


def test_different_generation_rejected():
    r = check_pair(prod(models=("15",)), prod(models=("16",)), 0.955, 0.85)
    assert r["ok_score"] and not r["ok_model"] and not r["matched"]


def test_model_subset_matches():
    a = prod(brand="hp", models=("5", "16-ag1069au", "bz7t1pa"), price=20_000_000)
    b = prod(brand="hp", models=("5", "16-ag1069au"), price=20_500_000)
    assert check_pair(a, b, 0.97, 0.85)["matched"]


def test_variant_and_ram_rules():
    assert not check_pair(prod(variants=()), prod(variants=("pro",)), 0.97, 0.85)["matched"]
    a = prod(brand="samsung", ram=4, models=("a07",), price=4_190_000)
    b = prod(brand="samsung", ram=6, models=("a07",), price=4_590_000)
    assert not check_pair(a, b, 1.0, 0.85)["ok_ram"]


def test_brand_must_be_known_and_equal():
    assert not check_pair(prod(brand=None), prod(), 1.0, 0.85)["ok_brand"]
    assert not check_pair(prod(brand="hoco"), prod(brand="ugreen"), 0.9, 0.85)["ok_brand"]


def test_price_safety_net():
    assert not check_pair(prod(price=590_000), prod(price=21_990_000), 0.9, 0.85)["ok_price"]


def test_cluster_one_product_per_platform():
    platform_of = {"fpt:x6c": "fptshop", "tgdd:x6c-128": "tgdd", "tgdd:x6c-256": "tgdd", "hh:x6c-128": "hoangha"}
    matches = [Match("fpt:x6c", "tgdd:x6c-128", 0.99), Match("fpt:x6c", "tgdd:x6c-256", 0.98),
               Match("hh:x6c-128", "tgdd:x6c-128", 1.0)]
    out = cluster(platform_of, matches)
    # cụm lớn có 3 sàn, bản 256GB của tgdd đứng riêng vì cụm đã có 1 sản phẩm tgdd
    assert out["fpt:x6c"]["master_id"] == out["tgdd:x6c-128"]["master_id"] == out["hh:x6c-128"]["master_id"]
    assert out["fpt:x6c"]["cluster_size"] == 3
    assert out["tgdd:x6c-256"]["cluster_size"] == 1 and out["tgdd:x6c-256"]["match_score"] is None


def test_cluster_is_deterministic_and_master_id_stable():
    platform_of = {"a:1": "a", "b:1": "b", "c:1": "c"}
    matches = [Match("a:1", "b:1", 0.9), Match("b:1", "c:1", 0.9)]
    first = cluster(platform_of, matches)
    second = cluster(platform_of, list(reversed(matches)))
    assert first == second
    assert first["c:1"]["master_id"] == master_id(["a:1", "b:1", "c:1"])


def test_tie_prefers_closer_price():
    # fptshop "iPhone 15" (21,49tr) điểm 1.0 với cả tgdd 128GB (21,99tr) và tgdd 256GB (24,49tr): chọn 128GB
    platform_of = {"fpt:ip15": "fptshop", "tgdd:ip15-128": "tgdd", "tgdd:ip15-256": "tgdd"}
    matches = [Match("fpt:ip15", "tgdd:ip15-256", 1.0, 24.49 / 21.49), Match("fpt:ip15", "tgdd:ip15-128", 1.0, 21.99 / 21.49)]
    out = cluster(platform_of, matches)
    assert out["fpt:ip15"]["master_id"] == out["tgdd:ip15-128"]["master_id"]
    assert out["tgdd:ip15-256"]["cluster_size"] == 1


def test_chip_rule():
    a = prod(brand="apple", models=("14", "m5"), price=69_990_000) | {"chip": {"18cpu", "20gpu"}}
    b = prod(brand="apple", models=("14", "m5"), price=58_990_000) | {"chip": {"10cpu", "10gpu"}}
    c = prod(brand="apple", models=("14", "m5"), price=67_190_000)                 # tên không ghi chip
    assert not check_pair(a, b, 0.95, 0.85)["ok_chip"]
    assert check_pair(a, c, 0.95, 0.85)["ok_chip"]


def test_cluster_blocks_transitive_storage_conflict():
    # fptshop "Xiaomi 17T 12GB" (không ghi dung lượng) khớp cả cellphones 512GB và tgdd 256GB
    platform_of = {"fpt:17t": "fptshop", "cps:17t-512": "cellphones", "tgdd:17t-256": "tgdd"}
    attrs = {"fpt:17t": cluster_attrs({"ram_gb": 12}),
             "cps:17t-512": cluster_attrs({"ram_gb": 12, "storage_gb": 512}),
             "tgdd:17t-256": cluster_attrs({"ram_gb": 12, "storage_gb": 256})}
    matches = [Match("cps:17t-512", "fpt:17t", 1.0, 1.008), Match("fpt:17t", "tgdd:17t-256", 1.0, 1.028)]
    out = cluster(platform_of, matches, attrs)
    assert out["cps:17t-512"]["master_id"] != out["tgdd:17t-256"]["master_id"]
    assert out["fpt:17t"]["master_id"] == out["cps:17t-512"]["master_id"]       # giá gần hơn được gộp trước


def test_chip_tier_rule():
    a = prod(brand="apple", models=("14", "m5"), price=64_990_000) | {"chip_tier": "m5pro"}
    b = prod(brand="apple", models=("14", "m5"), price=57_190_000) | {"chip_tier": "m5"}
    assert not check_pair(a, b, 0.97, 0.85)["ok_chip"]
