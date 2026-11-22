"""Bước 2 tuần 3: gán nhãn tay ~500 cặp sản phẩm để đo precision, recall của entity_resolution và chọn ngưỡng.

Chạy trên máy cá nhân với bản sao Gold (pandas + deltalake, không cần Spark).

1) Lấy mẫu, chia đều cho nhóm:
    python -m jobs.er_labeling sample --gold-root datalake_local/gold --out labeling/
   -> labeling/labeler_1.csv ... labeler_4.csv  (mỗi người ~125 cặp + 30 cặp chung để đo độ đồng thuận)
      labeling/pairs_key.csv                     (khoá, điểm, tầng, trọng số; NGƯỜI GÁN KHÔNG XEM file này)
   Người gán điền cột label: 1 = cùng một sản phẩm (cùng model, dung lượng, RAM; khác màu vẫn là 1),
   0 = khác, ? = không chắc. Không thấy điểm hay kết quả máy để khỏi bị ảnh hưởng.

2) Gom nhãn về, đánh giá:
    python -m jobs.er_labeling evaluate --labels labeling/ --target-precision 0.9
   -> in và ghi labeling/report.md: precision, recall theo ngưỡng, ngưỡng đề xuất, độ đồng thuận (Cohen kappa).

Lấy mẫu PHÂN TẦNG theo (máy có ghép hay không, khoảng điểm): ngẫu nhiên thuần thì gần như toàn cặp "không
khớp" điểm thấp. Mỗi cặp mang trọng số = số cặp của tầng / số cặp lấy từ tầng, nên precision/recall tính ra
là ước lượng không lệch trên TOÀN BỘ cặp ứng viên. Recall ở đây là trên các cặp Qdrant tìm ra (top-k),
không phải trên mọi cặp có thể có.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from deltalake import DeltaTable

from common.entity_rules import CHECKS

SCORE_BINS = [0.70, 0.80, 0.85, 0.90, 0.95, 0.99, 1.0001]
RULES_EXCEPT_SCORE = [c for c in CHECKS if c != "ok_score"]
OVERLAP = 30           # số cặp chung cho mọi người, đo độ đồng thuận
SEED = 20261122


def load_gold(gold_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    cand = DeltaTable(str(gold_root / "er_candidates")).to_pandas()
    master = DeltaTable(str(gold_root / "product_master")).to_pandas(
        columns=["product_key", "platform", "name_rep", "price_median", "category"])
    return cand, master.set_index("product_key")


def sample(args: argparse.Namespace) -> None:
    cand, master = load_gold(args.gold_root)
    cand = cand[cand["score"] >= SCORE_BINS[0]].copy()
    cand["bin"] = pd.cut(cand["score"], SCORE_BINS, right=False).astype(str)
    cand["stratum"] = np.where(cand["matched"], "matched", "rejected") + " " + cand["bin"]

    # Phân bổ: một nửa cho cặp máy ghép (đo precision), một nửa cho cặp bị loại (đo recall),
    # trong mỗi nửa chia theo căn bậc hai cỡ tầng để tầng nhỏ vẫn có mẫu
    rng = np.random.default_rng(SEED)
    sizes = cand.groupby("stratum").size()
    alloc = {}
    for group in ("matched", "rejected"):
        g = sizes[sizes.index.str.startswith(group)]
        w = np.sqrt(g) / np.sqrt(g).sum()
        for s, share in w.items():
            alloc[s] = min(int(g[s]), max(1, round(args.n / 2 * share)))
    picked = []
    for s, n in alloc.items():
        pool = cand[cand["stratum"] == s]
        idx = rng.choice(len(pool), size=n, replace=False)
        part = pool.iloc[idx].copy()
        part["weight"] = len(pool) / n
        picked.append(part)
    pairs = pd.concat(picked).sample(frac=1, random_state=SEED).reset_index(drop=True)
    pairs.insert(0, "pair_id", [f"p{i:04d}" for i in range(len(pairs))])

    # Bảng cho người gán: tên, giá, sàn của hai bên; không có điểm và kết quả máy
    def side(keys: pd.Series, s: str) -> pd.DataFrame:
        m = master.loc[keys]
        return pd.DataFrame({f"platform_{s}": m["platform"].to_numpy(), f"name_{s}": m["name_rep"].to_numpy(),
                             f"price_{s}": m["price_median"].round(-3).to_numpy()})
    sheet = pd.concat([pairs[["pair_id"]], side(pairs["key_a"], "a"), side(pairs["key_b"], "b")], axis=1)
    sheet["label"] = ""
    sheet["note"] = ""

    args.out.mkdir(parents=True, exist_ok=True)
    overlap, rest = sheet.iloc[:OVERLAP], sheet.iloc[OVERLAP:]
    bounds = np.linspace(0, len(rest), args.labelers + 1).astype(int)
    chunks = [rest.iloc[bounds[i]:bounds[i + 1]] for i in range(args.labelers)]
    for i, chunk in enumerate(chunks, start=1):
        part = pd.concat([overlap, chunk]).sample(frac=1, random_state=SEED + i)
        part.to_csv(args.out / f"labeler_{i}.csv", index=False, encoding="utf-8-sig")   # utf-8-sig: Excel đọc đúng dấu
    key_cols = ["pair_id", "key_a", "key_b", "score", "stratum", "weight", *CHECKS, "matched"]
    pairs[key_cols].to_csv(args.out / "pairs_key.csv", index=False)
    print(f"{len(pairs)} pairs ({OVERLAP} shared by all) -> {args.out}/labeler_1..{args.labelers}.csv")
    print(pairs.groupby("stratum").agg(n=("pair_id", "size"), population=("weight", lambda w: round(w.sum())))
          .to_string())


def _labels(folder: Path) -> pd.DataFrame:
    frames = []
    for f in sorted(folder.glob("labeler_*.csv")):
        df = pd.read_csv(f, dtype={"label": str}, encoding="utf-8-sig")
        df["labeler"] = f.stem
        frames.append(df[["pair_id", "label", "labeler"]])
    if not frames:
        raise SystemExit(f"no labeler_*.csv in {folder}")
    lab = pd.concat(frames)
    lab["label"] = lab["label"].str.strip().map({"1": 1, "0": 0})       # "?" và ô trống -> NaN
    return lab


def _markdown_table(df: pd.DataFrame) -> str:
    """Bảng markdown đơn giản (không cần thư viện tabulate)."""
    def fmt(v):
        return f"{v:.3f}" if isinstance(v, float) else str(v)
    head = "| " + " | ".join(df.columns) + " |"
    sep = "| " + " | ".join("---" for _ in df.columns) + " |"
    body = ["| " + " | ".join(fmt(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([head, sep, *body])


def cohen_kappa(a: pd.Series, b: pd.Series) -> float:
    po = (a == b).mean()
    pe = a.mean() * b.mean() + (1 - a.mean()) * (1 - b.mean())
    return float("nan") if pe == 1 else (po - pe) / (1 - pe)


def evaluate(args: argparse.Namespace) -> None:
    key = pd.read_csv(args.labels / "pairs_key.csv")
    lab = _labels(args.labels)

    # Độ đồng thuận trên các cặp chung (từng đôi người gán)
    shared = lab.pivot_table(index="pair_id", columns="labeler", values="label")
    shared = shared.dropna(thresh=2)
    kappas = []
    cols = list(shared.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            both = shared[[cols[i], cols[j]]].dropna()
            if len(both) >= 5:
                kappas.append(cohen_kappa(both[cols[i]], both[cols[j]]))

    # Nhãn cuối của mỗi cặp: đa số (cặp chung), hoà hoặc toàn "?" thì bỏ
    final = lab.dropna(subset=["label"]).groupby("pair_id")["label"].mean()
    final = final[final != 0.5].round().astype(int).rename("label")
    df = key.merge(final, left_on="pair_id", right_index=True)
    rules_ok = df[RULES_EXCEPT_SCORE].all(axis=1)
    w = df["weight"]

    rows = []
    for t in np.round(np.arange(0.80, 1.0001, 0.01), 2):
        pred = rules_ok & (df["score"] >= t)
        tp = (w * (pred & (df["label"] == 1))).sum()
        fp = (w * (pred & (df["label"] == 0))).sum()
        fn = (w * (~pred & (df["label"] == 1))).sum()
        precision = tp / (tp + fp) if tp + fp else float("nan")
        recall = tp / (tp + fn) if tp + fn else float("nan")
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else float("nan")
        rows.append({"threshold": t, "precision": precision, "recall": recall, "f1": f1,
                     "pairs_predicted": int(pred.sum())})
    curve = pd.DataFrame(rows)
    ok = curve[curve["precision"] >= args.target_precision]
    best = ok.sort_values(["recall", "threshold"], ascending=[False, True]).iloc[0] if len(ok) else None

    # Luật nào loại oan cặp đúng (nhãn 1 nhưng máy loại)
    missed = df[(df["label"] == 1) & ~df["matched"]]
    blame = {c: int((~missed[c]).sum()) for c in CHECKS}

    lines = [
        "# Kết quả gán nhãn khớp sản phẩm", "",
        f"- Số cặp có nhãn dùng được: {len(df)} (bỏ cặp '?', ô trống, hoà phiếu)",
        f"- Tỉ lệ cặp đúng trong mẫu (chưa trọng số): {df['label'].mean():.1%}",
        f"- Độ đồng thuận giữa người gán (Cohen kappa, trung bình các đôi): "
        + (f"{np.mean(kappas):.2f}" if kappas else "chưa đủ cặp chung"),
        "",
        "## Precision, recall theo ngưỡng (đã trọng số theo tầng, các luật khác giữ nguyên)", "",
        _markdown_table(curve), "",
        "## Ngưỡng đề xuất", "",
        (f"Ngưỡng **{best['threshold']:.2f}**: precision {best['precision']:.1%}, recall {best['recall']:.1%} "
         f"(recall cao nhất trong các ngưỡng đạt precision >= {args.target_precision:.0%}).")
        if best is not None else f"Không ngưỡng nào đạt precision >= {args.target_precision:.0%}: xem lại luật.",
        "", "## Cặp đúng (nhãn 1) mà máy loại, theo luật gây loại", "",
        ", ".join(f"{k}: {v}" for k, v in blame.items()), "",
        "Recall tính trên các cặp ứng viên Qdrant tìm ra (top-k), không phải mọi cặp có thể có.",
    ]
    report = "\n".join(lines)
    (args.labels / "report.md").write_text(report, encoding="utf-8")
    curve.to_csv(args.labels / "threshold_curve.csv", index=False)
    print(report)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample", help="lấy mẫu cặp và xuất file cho người gán")
    s.add_argument("--gold-root", type=Path, default=Path("datalake_local/gold"))
    s.add_argument("--out", type=Path, default=Path("labeling"))
    s.add_argument("--n", type=int, default=500)
    s.add_argument("--labelers", type=int, default=4)
    e = sub.add_parser("evaluate", help="đọc nhãn, tính precision/recall, chọn ngưỡng")
    e.add_argument("--labels", type=Path, default=Path("labeling"))
    e.add_argument("--target-precision", type=float, default=0.9)
    args = ap.parse_args(argv)
    (sample if args.cmd == "sample" else evaluate)(args)


if __name__ == "__main__":
    main()
