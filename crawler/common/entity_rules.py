"""Luật cứng và gom cụm cho entity_resolution (khâu E). Hàm thuần, không phụ thuộc Spark/Qdrant, để test.

Một cặp ứng viên (A, B) do Qdrant tìm ra (cùng category, khác sàn) chỉ được ghép khi qua MỌI luật:

    ok_score    cosine >= ngưỡng
    ok_brand    cả hai đều có brand và brand bằng nhau (kế hoạch: "trùng thương hiệu")
    ok_storage  cả hai có storage_gb -> bằng nhau; thiếu một bên -> giá lệch <= 15% (fptshop không ghi GB);
                thiếu cả hai -> qua
    ok_ram      cả hai có ram_gb -> bằng nhau, không thì qua
    ok_variant  tập hậu tố (pro, max, plus, 5g...) bằng nhau
    ok_model    tập số đời máy của A chứa, hoặc nằm trong, tập của B (iphone 15 ≠ 16; tên dài/ngắn của cùng máy)
    ok_chip     cả hai tên ghi số nhân CPU/GPU -> bằng nhau (MacBook M5 10CPU/8GPU ≠ 10CPU/10GPU), và cả hai
                có dòng chip Apple M -> bằng nhau (M5 ≠ M5 Pro)
    ok_price    cả hai có giá -> max/min <= 1.5 (lưới an toàn: máy 24tr không thể là ốp 590k)

Gom cụm: duyệt các cặp đã qua luật theo điểm giảm dần, chỉ gộp hai cụm khi
  - tập sàn của chúng không giao nhau (mỗi cụm tối đa 1 sản phẩm mỗi sàn), và
  - các thuộc tính đã biết (dung lượng, RAM, chip) của cả hai cụm vẫn thống nhất.
Điều kiện thứ hai chặn lỗi bắc cầu: fptshop "Xiaomi 17T 12GB" (không ghi dung lượng) khớp được cả bản 256GB
lẫn 512GB của hai sàn khác, nhưng hai bản đó không được vào chung một cụm.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

STORAGE_PRICE_TOLERANCE = 0.15   # thiếu dung lượng một bên: giá chênh tối đa 15% mới coi là cùng bản
MAX_PRICE_RATIO = 1.5            # giá hai bên chênh quá 1.5 lần thì không phải cùng sản phẩm
CHECKS = ("ok_score", "ok_brand", "ok_storage", "ok_ram", "ok_variant", "ok_model", "ok_chip", "ok_price")
CLUSTER_ATTRS = ("storage_gb", "ram_gb", "chip", "chip_tier")   # phải thống nhất trong cả cụm


def _known(v) -> bool:
    """Có giá trị thật: loại None, NaN và pd.NA (bool(pd.NA == pd.NA) báo TypeError)."""
    if v is None:
        return False
    try:
        return bool(v == v)
    except TypeError:
        return False


def price_ratio(a, b) -> float | None:
    """max/min của hai giá, None nếu thiếu hoặc không dương."""
    if not (_known(a) and _known(b)) or a <= 0 or b <= 0:
        return None
    return max(a, b) / min(a, b)


def check_pair(a: Mapping, b: Mapping, score: float, threshold: float) -> dict[str, bool]:
    """a, b: brand, storage_gb, ram_gb, variants (set), model_nums (set), price_median."""
    ratio = price_ratio(a.get("price_median"), b.get("price_median"))

    sa, sb = a.get("storage_gb"), b.get("storage_gb")
    if _known(sa) and _known(sb):
        ok_storage = int(sa) == int(sb)
    elif _known(sa) or _known(sb):
        ok_storage = ratio is not None and ratio <= 1 + STORAGE_PRICE_TOLERANCE
    else:
        ok_storage = True

    ra, rb = a.get("ram_gb"), b.get("ram_gb")
    ma, mb = frozenset(a.get("model_nums") or ()), frozenset(b.get("model_nums") or ())
    ca, cb = frozenset(a.get("chip") or ()), frozenset(b.get("chip") or ())
    ba, bb = a.get("brand"), b.get("brand")
    checks = {
        "ok_score": score >= threshold,
        "ok_brand": _known(ba) and _known(bb) and ba == bb,
        "ok_storage": ok_storage,
        "ok_ram": not (_known(ra) and _known(rb)) or int(ra) == int(rb),
        "ok_variant": frozenset(a.get("variants") or ()) == frozenset(b.get("variants") or ()),
        "ok_model": ma <= mb or mb <= ma,
        "ok_chip": (not (ca and cb) or ca == cb)
                   and (not (a.get("chip_tier") and b.get("chip_tier")) or a["chip_tier"] == b["chip_tier"]),
        "ok_price": ratio is None or ratio <= MAX_PRICE_RATIO,
    }
    checks["matched"] = all(checks[c] for c in CHECKS)
    return checks


def master_id(product_keys: Iterable[str]) -> str:
    """Id cụm: băm product_key nhỏ nhất trong cụm. Cụm không đổi thì id không đổi giữa các lần chạy."""
    return "m_" + hashlib.sha1(min(product_keys).encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class Match:
    key_a: str
    key_b: str
    score: float
    price_ratio: float | None = None    # hoà điểm thì cặp giá gần nhau hơn được gộp trước


def cluster_attrs(product: Mapping) -> dict:
    """Thuộc tính phải thống nhất trong cụm; None = chưa biết (không ràng buộc)."""
    out = {}
    for a in CLUSTER_ATTRS:
        v = product.get(a)
        if a == "chip":
            v = frozenset(v) if v else None
        elif a == "chip_tier":
            v = v or None
        elif _known(v):
            v = int(v)
        else:
            v = None
        out[a] = v
    return out


class ClusterUnionFind:
    """Union-find giữ thêm, cho mỗi cụm, tập sàn và tập giá trị đã biết của từng thuộc tính.
    Chỉ gộp khi hai cụm không có sàn chung và mọi thuộc tính vẫn có tối đa 1 giá trị đã biết."""

    def __init__(self, platform_of: Mapping[str, str], attrs_of: Mapping[str, Mapping] | None = None):
        attrs_of = attrs_of or {}
        self.parent = {k: k for k in platform_of}
        self.platforms = {k: {p} for k, p in platform_of.items()}
        self.values = {k: {a: ({v} if v is not None else set()) for a, v in attrs_of.get(k, {}).items()}
                       for k in platform_of}

    def find(self, k: str) -> str:
        root = k
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[k] != root:                 # nén đường đi
            self.parent[k], k = root, self.parent[k]
        return root

    def union(self, a: str, b: str) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb or self.platforms[ra] & self.platforms[rb]:
            return False
        va, vb = self.values[ra], self.values[rb]
        if any(len(va.get(attr, set()) | vb.get(attr, set())) > 1 for attr in set(va) | set(vb)):
            return False
        if len(self.platforms[ra]) < len(self.platforms[rb]):
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.platforms[ra] |= self.platforms.pop(rb)
        merged = self.values.pop(rb)
        for attr, vals in merged.items():
            self.values[ra].setdefault(attr, set()).update(vals)
        return True


def cluster(platform_of: Mapping[str, str], matches: Iterable[Match],
            attrs_of: Mapping[str, Mapping] | None = None) -> dict[str, dict]:
    """Trả {product_key: {master_id, cluster_size, match_score}} cho MỌI product_key.

    match_score: điểm của cặp đã đưa sản phẩm vào cụm (None nếu đứng một mình).
    Duyệt theo điểm giảm dần; hoà điểm thì cặp giá gần nhau hơn trước (fptshop "iPhone 15" không ghi GB có
    điểm 1.0 với cả bản 128GB lẫn 256GB của sàn khác, giá mới phân biệt được), rồi theo khoá để chạy lại ra
    đúng cụm cũ.
    """
    uf = ClusterUnionFind(platform_of, attrs_of)
    best: dict[str, float] = {}
    order = sorted(matches, key=lambda m: (-round(m.score, 4), m.price_ratio if m.price_ratio else 99.0,
                                           m.key_a, m.key_b))
    for m in order:
        if uf.union(m.key_a, m.key_b):
            for k in (m.key_a, m.key_b):
                best[k] = max(best.get(k, m.score), m.score)

    members: dict[str, list[str]] = {}
    for k in platform_of:
        members.setdefault(uf.find(k), []).append(k)
    out = {}
    for keys in members.values():
        mid = master_id(keys)
        for k in keys:
            out[k] = {"master_id": mid, "cluster_size": len(keys), "match_score": best.get(k)}
    return out
