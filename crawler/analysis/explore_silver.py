"""Bước 0 tuần 3: khảo sát Silver trước khi viết Gold (product_master, price_baseline, daily_stats).

Chỉ đọc, không ghi gì vào Silver. In kết quả ra màn hình và lưu vài file CSV để xem bằng Excel.

Dùng (trong thư mục crawler/, đã kích hoạt .venv):
    python -m analysis.explore_silver --silver-root ~/data/silver_20261121
    python -m analysis.explore_silver --silver-root ~/data/silver_20261121 --probe "iphone 16" --probe "galaxy a56"

Kết quả CSV nằm ở --out (mặc định datalake_local/explore/, thư mục này không commit).
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

PRICE_COLS = ["platform", "product_key", "sku_id", "name_raw", "source", "price_current", "event_time", "date"]
PRODUCT_COLS = ["platform", "product_key", "name_raw", "brand_raw", "category_raw", "specs_raw", "event_time"]

# Dung lượng trong tên: "256GB", "1 TB", "8GB/256GB"
CAPACITY_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(gb|tb)\b", re.IGNORECASE)

# Chữ quảng cáo nghi ngờ, đếm để quyết định danh sách cần bỏ khi chuẩn hoá tên. Thêm bớt tuỳ ý.
AD_PHRASES = [
    "chính hãng", "chinh hang", "freeship", "free ship", "miễn phí", "giá rẻ", "trả góp", "tra gop",
    "vn/a", "nguyên seal", "fullbox", "full box", "new", "mới", "bảo hành", "hàng nhập khẩu",
    "quốc tế", "like new", "cũ", "đã kích hoạt", "deal", "sale", "hot", "|", "(", "[",
]

DEFAULT_PROBES = ["iphone 16", "iphone 15", "galaxy s24", "galaxy a", "redmi note", "macbook air", "airpods"]

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_colwidth", 90)


def section(title: str) -> None:
    print("\n" + "=" * 90 + f"\n{title}\n" + "=" * 90)


def pct(a: float, b: float) -> str:
    return f"{100 * a / b:.1f}%" if b else "-"


def load(path: Path, cols: list[str]) -> pd.DataFrame:
    # Luôn đọc qua Delta (không đọc thẳng .parquet, sẽ ra dòng trùng). Chuỗi đổi sang category cho đỡ RAM.
    table = DeltaTable(str(path)).to_pyarrow_table(columns=cols)
    df = table.to_pandas(strings_to_categorical=True)
    for c in ("product_key", "sku_id", "name_raw"):
        if c in df.columns:
            df[c] = df[c].astype(str).replace({"None": "∅", "nan": "∅"})
    return df


# ---------------------------------------------------------------------------------------------- 1
def check_counts(root: Path, price: pd.DataFrame, product: pd.DataFrame) -> None:
    section("1. SỐ DÒNG, KHOẢNG NGÀY, NGÀY THIẾU DỮ LIỆU")
    manifest = root / "MANIFEST.txt"
    if manifest.exists():
        print("MANIFEST.txt:\n" + manifest.read_text().strip())
    print(f"\nĐếm lại: price = {len(price):,} dòng, product = {len(product):,} dòng")
    print(f"Ngày (UTC): {price['date'].min()} → {price['date'].max()}, {price['date'].nunique()} ngày")

    grid = price.groupby(["date", "platform"], observed=True).size().unstack(fill_value=0)
    print("\nSố dòng theo ngày × sàn:")
    print(grid.to_string())
    # Ngày có ít hơn một nửa trung vị của sàn đó: nghi máy tắt hoặc bị chặn
    low = grid.lt(grid.median() * 0.5)
    suspicious = [(str(d), p, int(grid.loc[d, p])) for d in grid.index for p in grid.columns if low.loc[d, p]]
    print("\nNgày × sàn ít bất thường (< 50% trung vị của sàn):", suspicious or "không có")


# ---------------------------------------------------------------------------------------------- 2
def check_granularity(price: pd.DataFrame, out: Path) -> None:
    section("2. MỘT product_key LÀ MỘT SẢN PHẨM HAY NHIỀU BIẾN THỂ?")
    rows = []
    for plat, g in price.groupby("platform", observed=True):
        sku_per_key = g.groupby("product_key", observed=True)["sku_id"].nunique()
        name_per_key = g.groupby("product_key", observed=True)["name_raw"].nunique()
        name_per_sku = g.groupby(["product_key", "sku_id"], observed=True)["name_raw"].nunique()
        # Cùng một lần crawl (cùng event_time) mà một product_key có nhiều giá: các biến thể giá khác nhau
        gp = g.dropna(subset=["price_current"])
        multi_price = gp.groupby(["product_key", "event_time"], observed=True)["price_current"].nunique()
        multi_price_keys = multi_price[multi_price > 1].index.get_level_values(0).nunique()
        n_keys = len(sku_per_key)
        rows.append({
            "platform": plat,
            "product_key": n_keys,
            "(product_key, sku_id)": len(name_per_sku),
            "key >1 sku": pct((sku_per_key > 1).sum(), n_keys),
            "key >1 tên": pct((name_per_key > 1).sum(), n_keys),
            "sku đổi tên theo thời gian": pct((name_per_sku > 1).sum(), len(name_per_sku)),
            "key có nhiều giá cùng lúc": pct(multi_price_keys, n_keys),
            "sku_id rỗng": pct((g["sku_id"] == "∅").sum(), len(g)),
        })
    print(pd.DataFrame(rows).to_string(index=False))

    # Ví dụ: các product_key có nhiều tên nhất mỗi sàn, để nhìn tận mắt
    examples = []
    for plat, g in price.groupby("platform", observed=True):
        top = g.groupby("product_key", observed=True)["name_raw"].nunique().nlargest(3).index
        for key in top:
            sub = g[g["product_key"] == key]
            for (sku, name), n in sub.groupby(["sku_id", "name_raw"], observed=True).size().items():
                med = sub[(sub["sku_id"] == sku) & (sub["name_raw"] == name)]["price_current"].median()
                examples.append({"platform": plat, "product_key": key, "sku_id": sku, "name_raw": name,
                                 "n_obs": n, "median_price": med})
    ex = pd.DataFrame(examples)
    ex.to_csv(out / "granularity_examples.csv", index=False)
    print("\nVí dụ (3 product_key nhiều tên nhất mỗi sàn, 25 dòng đầu; đủ ở granularity_examples.csv):")
    print(ex.head(25).to_string(index=False))


# ---------------------------------------------------------------------------------------------- 3
def check_names(price: pd.DataFrame, out: Path) -> pd.DataFrame:
    section("3. TÊN SẢN PHẨM: DUNG LƯỢNG VÀ CHỮ QUẢNG CÁO")
    names = (price.groupby(["platform", "product_key", "sku_id", "name_raw"], observed=True)
             .agg(n_obs=("price_current", "size"), median_price=("price_current", "median"),
                  first_seen=("event_time", "min"), last_seen=("event_time", "max"))
             .reset_index())
    names.to_csv(out / "names_by_platform.csv", index=False)
    print(f"{len(names):,} cặp (product_key, sku_id, name_raw) khác nhau → names_by_platform.csv")

    names["n_capacity"] = names["name_raw"].map(lambda s: len(CAPACITY_RE.findall(s)))
    cap = names.groupby("platform", observed=True)["n_capacity"].agg(
        co_dung_luong=lambda s: pct((s >= 1).sum(), len(s)),
        hai_so_tro_len=lambda s: pct((s >= 2).sum(), len(s)),
    )
    print("\nTên có chứa dung lượng (GB/TB); 'hai_so_tro_len' thường là RAM/ROM như 8GB/256GB:")
    print(cap.to_string())

    print("\nVí dụ tên mỗi sàn (5 tên ngẫu nhiên):")
    for plat, g in names.groupby("platform", observed=True):
        print(f"  [{plat}]")
        for n in g["name_raw"].sample(min(5, len(g)), random_state=1):
            print(f"     {n}")

    lower = names["name_raw"].str.lower()
    counts = {p: int(lower.str.contains(p, regex=False).sum()) for p in AD_PHRASES}
    print("\nSố tên chứa cụm nghi là quảng cáo (trên tổng", f"{len(names):,}):")
    for p, c in sorted(counts.items(), key=lambda x: -x[1]):
        if c:
            print(f"  {p!r:22} {c:>7,}  {pct(c, len(names))}")

    tokens = Counter(t for s in lower for t in re.findall(r"[\wÀ-ỹ/+.-]+", s))
    print("\n60 từ hay gặp nhất trong tên (tìm thêm chữ quảng cáo, tên thương hiệu):")
    print(", ".join(f"{t}({c})" for t, c in tokens.most_common(60)))
    return names


# ---------------------------------------------------------------------------------------------- 4
def check_product(price: pd.DataFrame, product: pd.DataFrame, out: Path) -> None:
    section("4. silver_product: CÓ BRAND, CATEGORY, SPECS CHO BAO NHIÊU SẢN PHẨM?")
    latest = product.sort_values("event_time").groupby("product_key", observed=True).tail(1)
    print(f"{len(product):,} lần quan sát, {len(latest):,} product_key khác nhau (lấy bản mới nhất)")

    rows = []
    for plat, g in price.groupby("platform", observed=True):
        keys = set(g["product_key"].unique())
        lp = latest[latest["platform"] == plat]
        covered = keys & set(lp["product_key"])
        rows.append({
            "platform": plat,
            "key trong price": len(keys),
            "có trong product": pct(len(covered), len(keys)),
            "brand_raw có": pct(lp["brand_raw"].notna().sum(), len(lp)),
            "category_raw có": pct(lp["category_raw"].notna().sum(), len(lp)),
            "specs_raw có": pct(lp["specs_raw"].notna().sum(), len(lp)),
        })
    print(pd.DataFrame(rows).to_string(index=False))

    for plat, lp in latest.groupby("platform", observed=True):
        print(f"\n[{plat}] brand hay gặp: "
              + ", ".join(f"{b}({c})" for b, c in lp["brand_raw"].value_counts().head(12).items()))
        print(f"[{plat}] category hay gặp: "
              + ", ".join(f"{b}({c})" for b, c in lp["category_raw"].value_counts().head(8).items()))
        keys = Counter()
        for s in lp["specs_raw"].dropna().head(300):
            try:
                obj = json.loads(s)
            except (TypeError, ValueError):
                continue
            if isinstance(obj, dict):
                keys.update(obj.keys())
        print(f"[{plat}] khoá specs hay gặp: " + ", ".join(k for k, _ in keys.most_common(15)))
    latest.drop(columns=["specs_raw"]).to_csv(out / "product_latest.csv", index=False)


# ---------------------------------------------------------------------------------------------- 5
def check_baseline(price: pd.DataFrame, out: Path) -> None:
    section("5. ĐỦ DỮ LIỆU CHO BASELINE CHƯA? (đơn vị: product_key + sku_id, chỉ giá khác null)")
    p = price.dropna(subset=["price_current"])
    per = (p.groupby(["platform", "product_key", "sku_id"], observed=True)
           .agg(n_obs=("price_current", "size"), n_days=("date", "nunique"),
                median_price=("price_current", "median"))
           .reset_index())
    per["obs_per_day"] = per["n_obs"] / per["n_days"]
    # Nhóm theo dõi sát được crawl 30-60 phút/lần (>= ~12 lần/ngày); nhóm crawl đêm chỉ ~1 lần/ngày.
    per["hot"] = per["obs_per_day"] >= 6
    per["n_days_bin"] = pd.cut(per["n_days"], [0, 6, 13, 29, 10_000], labels=["<7", "7-13", "14-29", ">=30"])
    per.to_csv(out / "baseline_readiness.csv", index=False)

    print("Số (product_key, sku_id) theo số ngày có giá:")
    print(pd.crosstab(per["platform"], per["n_days_bin"], margins=True).to_string())
    print("\nNhóm theo dõi sát (>= 6 lần quan sát/ngày):")
    hot = per[per["hot"]]
    if hot.empty:
        print("  không có sản phẩm nào")
    else:
        print(pd.crosstab(hot["platform"], hot["n_days"] >= 7, margins=True)
              .rename(columns={True: ">=7 ngày", False: "<7 ngày"}).to_string())
    print("\nPhân bố số lần quan sát mỗi ngày (cho biết sàn nào crawl dày):")
    print(per.groupby("platform", observed=True)["obs_per_day"].describe(percentiles=[.25, .5, .75]).round(1).to_string())

    # Giá trong một ngày dao động bao nhiêu (nhiễu nền của nhóm hot): (max - min) / median
    hp = p.merge(hot[["product_key", "sku_id"]], on=["product_key", "sku_id"])
    if len(hp):
        d = hp.groupby(["product_key", "sku_id", "date"], observed=True)["price_current"].agg(["min", "max", "median"])
        d = d[d["median"] > 0]
        spread = (d["max"] - d["min"]) / d["median"] * 100
        print("\nNhóm hot: dao động giá trong một ngày, % so với trung vị ngày:")
        print(spread.describe(percentiles=[.5, .9, .99]).round(2).to_string())


# ---------------------------------------------------------------------------------------------- 6
def probe(names: pd.DataFrame, probes: list[str], out: Path) -> None:
    section("6. CÙNG MỘT SẢN PHẨM, MỖI SÀN ĐẶT TÊN THẾ NÀO?")
    lower = names["name_raw"].str.lower()
    hits = []
    for q in probes:
        m = names[lower.str.contains(q.lower(), regex=False)]
        print(f"\n--- '{q}': {len(m)} tên ---")
        for plat, g in m.groupby("platform", observed=True):
            top = g.nlargest(4, "n_obs")
            for _, r in top.iterrows():
                price_txt = f"{r['median_price']:,.0f}" if pd.notna(r["median_price"]) else "-"
                print(f"  {plat:<11} {price_txt:>13}  {r['name_raw']}")
        hits.append(m.assign(probe=q))
    if hits:
        pd.concat(hits).to_csv(out / "probe_names.csv", index=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--silver-root", required=True, type=Path,
                    help="thư mục chứa price/ và product/ (vd ~/data/silver_20261121)")
    ap.add_argument("--out", type=Path, default=Path("datalake_local/explore"))
    ap.add_argument("--probe", action="append", help="từ khoá xem tên giữa các sàn, lặp lại được")
    args = ap.parse_args()

    root = args.silver_root.expanduser()
    out = args.out.expanduser()
    out.mkdir(parents=True, exist_ok=True)

    print(f"Đọc {root} ...")
    price = load(root / "price", PRICE_COLS)
    product = load(root / "product", PRODUCT_COLS)

    check_counts(root, price, product)
    check_granularity(price, out)
    names = check_names(price, out)
    check_product(price, product, out)
    check_baseline(price, out)
    probe(names, args.probe or DEFAULT_PROBES, out)
    print(f"\nXong. File CSV ở {out.resolve()}")


if __name__ == "__main__":
    main()
