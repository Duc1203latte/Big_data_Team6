"""Xuất dữ liệu đã crawl ra CSV và viết báo cáo thống kê (chỉ đọc, không gửi request nào).

Đọc landing/<event_type>/platform=<sàn>/date=.../hour=.../*.jsonl rồi ghi:
    exports/price_<sàn>.csv      mỗi price event một dòng, đủ mọi cột của PriceEvent
    exports/product_<sàn>.csv    mỗi product event một dòng; specs_raw ghi thành chuỗi JSON
    exports/summary.md           báo cáo thống kê (Markdown)

- landing/ chỉ được ĐỌC (append-only), không sửa hay xoá gì.
- Bỏ event trùng event_id (cùng sản phẩm, cùng giây, cùng loại) và đếm lại trong báo cáo.
- Dòng JSON hỏng (thường là dòng cuối của file đang được crawler ghi dở) thì bỏ qua và đếm.
- CSV ghi UTF-8 có BOM để Excel hiển thị đúng tiếng Việt; pandas đọc bình thường.
- Ghi ra file .tmp rồi mới đổi tên, nên đang chạy dở thì không để lại file CSV thiếu.
- Thống kê giá chỉ để mô tả dữ liệu đã thu, không ghi ngược vào landing/ hay vào event.

Chạy (trong thư mục crawler/):
    python -m jobs.export_csv                                  # toàn bộ dữ liệu, mọi sàn
    python -m jobs.export_csv --platform tiki --platform tgdd  # chỉ vài sàn
    python -m jobs.export_csv --from 2026-11-03 --to 2026-11-05  # theo ngày partition (UTC)
"""

import argparse
import csv
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import orjson

from common.config import load_settings
from common.schema import PriceEvent, ProductEvent

# Thứ tự cột của CSV = thứ tự trường trong schema, để file của mọi sàn giống hệt nhau
PRICE_COLUMNS = list(PriceEvent.model_fields)
PRODUCT_COLUMNS = list(ProductEvent.model_fields)
COLUMNS = {"price": PRICE_COLUMNS, "product": PRODUCT_COLUMNS}

# Các trường dùng để đo độ đầy đủ của dữ liệu trong báo cáo
PRICE_FILL_FIELDS = ["name_raw", "price_current", "price_original", "in_stock", "seller_id",
                     "seller_name", "rating_avg", "review_count", "sold_count"]
PRODUCT_FILL_FIELDS = ["brand_raw", "category_raw", "specs_raw", "image_url",
                       "rating_avg", "rating_5_count"]

VN_TZ = timezone(timedelta(hours=7))


# ---------------------------------------------------------------------------
# Thống kê gom trong lúc đọc
# ---------------------------------------------------------------------------

@dataclass
class SkuTrack:
    """Theo dõi giá của một SKU (sản phẩm + biến thể) qua các lần quan sát."""

    observations: int = 0
    last_time: str = ""
    last_price: int | None = None
    last_original: int | None = None
    prices: set[int] = field(default_factory=set)   # các mức giá khác nhau đã thấy


@dataclass
class PlatformSummary:
    files: int = 0
    bytes: int = 0
    bad_lines: int = 0
    events: dict[str, int] = field(default_factory=lambda: {"price": 0, "product": 0})
    duplicates: dict[str, int] = field(default_factory=lambda: {"price": 0, "product": 0})
    first_time: str | None = None
    last_time: str | None = None
    by_day: dict[str, int] = field(default_factory=lambda: defaultdict(int))   # price event theo ngày
    source: dict[str, int] = field(default_factory=lambda: defaultdict(int))   # listing / detail
    filled: dict[str, int] = field(default_factory=lambda: defaultdict(int))   # số event có giá trị
    product_ids: set[str] = field(default_factory=set)
    skus: dict[tuple[str, str | None], SkuTrack] = field(default_factory=dict)
    # product_id -> category_raw của product event mới nhất (để đếm theo danh mục)
    categories: dict[str, str] = field(default_factory=dict)


def is_filled(value) -> bool:
    """Có dữ liệu không: None, chuỗi rỗng, dict rỗng đều tính là thiếu."""
    return value is not None and value != "" and value != {}


def update_summary(summary: PlatformSummary, event_type: str, event: dict) -> None:
    """Cộng một event (đã bỏ trùng) vào thống kê của sàn."""
    summary.events[event_type] += 1
    time = event.get("event_time") or ""
    if time:
        if summary.first_time is None or time < summary.first_time:
            summary.first_time = time
        if summary.last_time is None or time > summary.last_time:
            summary.last_time = time
    product_id = event.get("product_id")
    summary.product_ids.add(product_id)

    if event_type == "product":
        for name in PRODUCT_FILL_FIELDS:
            summary.filled[f"product.{name}"] += is_filled(event.get(name))
        if event.get("category_raw"):
            summary.categories[product_id] = event["category_raw"]
        return

    summary.by_day[time[:10]] += 1
    summary.source[event.get("source") or "?"] += 1
    for name in PRICE_FILL_FIELDS:
        summary.filled[f"price.{name}"] += is_filled(event.get(name))

    track = summary.skus.setdefault((product_id, event.get("sku_id")), SkuTrack())
    track.observations += 1
    price = event.get("price_current")
    if price is not None:
        track.prices.add(price)
    # File được đọc theo thứ tự thời gian nhưng vẫn so event_time cho chắc
    if time >= track.last_time:
        track.last_time = time
        if price is not None:
            track.last_price = price
            track.last_original = event.get("price_original")


# ---------------------------------------------------------------------------
# Đọc landing/ và ghi CSV
# ---------------------------------------------------------------------------

def partition_date(path: Path) -> date | None:
    """.../date=YYYY-MM-DD/hour=HH/part-....jsonl -> ngày partition (UTC)."""
    try:
        return date.fromisoformat(path.parent.parent.name.removeprefix("date="))
    except ValueError:
        return None


def list_files(landing_dir: Path, event_type: str, platform: str,
               date_from: date | None, date_to: date | None) -> list[Path]:
    """Các file JSONL của một sàn, một loại event, sắp theo ngày/giờ (tên thư mục sắp được)."""
    files = []
    for path in sorted((landing_dir / event_type / f"platform={platform}").glob("date=*/hour=*/*.jsonl")):
        day = partition_date(path)
        if day is None:
            continue
        if (date_from and day < date_from) or (date_to and day > date_to):
            continue
        files.append(path)
    return files


def csv_value(value) -> str | int | float:
    """Giá trị một ô CSV: None -> rỗng, dict/list -> chuỗi JSON, bool -> true/false."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return orjson.dumps(value).decode()
    return value


def export_platform_type(landing_dir: Path, out_dir: Path, event_type: str, platform: str,
                         summary: PlatformSummary, date_from: date | None, date_to: date | None) -> int:
    """Xuất một loại event của một sàn ra CSV. Trả về số dòng đã ghi (0 = không tạo file)."""
    files = list_files(landing_dir, event_type, platform, date_from, date_to)
    if not files:
        return 0
    columns = COLUMNS[event_type]
    target = out_dir / f"{event_type}_{platform}.csv"
    tmp = target.with_suffix(".csv.tmp")
    seen: set[str] = set()      # event_id đã ghi, để bỏ trùng
    rows = 0
    with tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for path in files:
            summary.files += 1
            summary.bytes += path.stat().st_size
            with path.open("rb") as src:
                for line in src:
                    if not line.strip():
                        continue
                    try:
                        event = orjson.loads(line)
                    except orjson.JSONDecodeError:
                        summary.bad_lines += 1
                        continue
                    event_id = event.get("event_id")
                    if event_id in seen:
                        summary.duplicates[event_type] += 1
                        continue
                    seen.add(event_id)
                    writer.writerow([csv_value(event.get(c)) for c in columns])
                    update_summary(summary, event_type, event)
                    rows += 1
    os.replace(tmp, target)
    return rows


def discover_platforms(landing_dir: Path) -> list[str]:
    """Các sàn có thư mục trong landing/ (ở bất kỳ loại event nào)."""
    names = {p.name.removeprefix("platform=") for p in landing_dir.glob("*/platform=*") if p.is_dir()}
    return sorted(names)


# ---------------------------------------------------------------------------
# Báo cáo Markdown
# ---------------------------------------------------------------------------

def quantile(sorted_values: list[int], q: float) -> int | None:
    """Phân vị q (0..1) theo kiểu "nearest rank" trên danh sách đã sắp xếp."""
    if not sorted_values:
        return None
    index = min(len(sorted_values) - 1, max(0, round(q * (len(sorted_values) - 1))))
    return sorted_values[index]


def fmt_int(value: int | float | None) -> str:
    """Số có dấu chấm phân cách hàng nghìn kiểu Việt Nam: 1234567 -> 1.234.567."""
    if value is None:
        return "-"
    return f"{value:,.0f}".replace(",", ".")


def fmt_pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.1f}%" if whole else "-"


def fmt_time(ts: str | None) -> str:
    """Giờ UTC dạng ISO -> giờ Việt Nam cho dễ đọc."""
    if not ts:
        return "-"
    return datetime.fromisoformat(ts).astimezone(VN_TZ).strftime("%d/%m/%Y %H:%M")


def md_table(header: list[str], rows: list[list[str]], align_right_from: int = 1) -> list[str]:
    """Bảng Markdown; các cột từ align_right_from trở đi căn phải (cột số)."""
    lines = ["| " + " | ".join(header) + " |",
             "|" + "|".join("---" if i < align_right_from else "---:" for i in range(len(header))) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return lines


def read_runs(state_db: Path) -> list[tuple]:
    """(job, sàn, trạng thái, số run, tổng event) từ crawl_runs; chỉ đọc, không có DB thì trả rỗng."""
    if not state_db.exists():
        return []
    conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        return conn.execute(
            "SELECT job, platform, status, COUNT(*), COALESCE(SUM(events_count), 0) "
            "FROM crawl_runs GROUP BY job, platform, status ORDER BY job, platform, status"
        ).fetchall()
    finally:
        conn.close()


def build_report(summaries: dict[str, PlatformSummary], runs: list[tuple], generated_at: datetime,
                 csv_rows: dict[str, dict[str, int]]) -> str:
    platforms = sorted(summaries)
    total_price = sum(s.events["price"] for s in summaries.values())
    total_product = sum(s.events["product"] for s in summaries.values())
    first = min((s.first_time for s in summaries.values() if s.first_time), default=None)
    last = max((s.last_time for s in summaries.values() if s.last_time), default=None)

    out = ["# Thống kê dữ liệu đã crawl", ""]
    out.append(f"Tạo lúc {generated_at.astimezone(VN_TZ).strftime('%d/%m/%Y %H:%M')} (giờ VN) "
               f"bằng `python -m jobs.export_csv`. Dữ liệu từ {fmt_time(first)} tới {fmt_time(last)}.")
    out.append("")

    # 1. Tổng quan
    out += ["## 1. Tổng quan", ""]
    rows = []
    for p in platforms:
        s = summaries[p]
        obs = [t.observations for t in s.skus.values()]
        rows.append([
            p, fmt_int(s.events["price"]), fmt_int(s.events["product"]),
            fmt_int(len(s.product_ids)), fmt_int(len(s.skus)),
            f"{sum(obs) / len(obs):.1f}" if obs else "-",
            fmt_time(s.first_time), fmt_time(s.last_time),
        ])
    rows.append(["**Tổng**", f"**{fmt_int(total_price)}**", f"**{fmt_int(total_product)}**",
                 fmt_int(sum(len(s.product_ids) for s in summaries.values())),
                 fmt_int(sum(len(s.skus) for s in summaries.values())), "", "", ""])
    out += md_table(["Sàn", "Price event", "Product event", "Sản phẩm", "SKU",
                     "Lần quan sát / SKU", "Từ", "Tới"], rows)
    out += ["", "*SKU = sản phẩm + biến thể (sku_id). \"Lần quan sát / SKU\": trung bình mỗi SKU "
            "được ghi giá bao nhiêu lần trong kỳ.*", ""]

    # 2. Theo ngày
    days = sorted({d for s in summaries.values() for d in s.by_day})
    out += ["## 2. Price event theo ngày (UTC)", ""]
    rows = [[d] + [fmt_int(summaries[p].by_day.get(d, 0)) for p in platforms]
            + [fmt_int(sum(summaries[p].by_day.get(d, 0) for p in platforms))] for d in days]
    out += md_table(["Ngày"] + platforms + ["Tổng"], rows)
    out += ["", "*Ngày đầu và ngày cuối thường thiếu giờ nên ít hơn; ngày ít bất thường "
            "là lúc máy tắt hoặc sàn bị tạm dừng.*", ""]

    # 3. Độ đầy đủ
    out += ["## 3. Độ đầy đủ dữ liệu", "", "Tỉ lệ event có giá trị ở từng trường.", "",
            "**Price event**", ""]
    rows = [[f"`{name}`"] + [fmt_pct(summaries[p].filled.get(f"price.{name}", 0),
                                     summaries[p].events["price"]) for p in platforms]
            for name in PRICE_FILL_FIELDS]
    out += md_table(["Trường"] + platforms, rows)
    out += ["", "**Product event** (trang chi tiết)", ""]
    rows = [[f"`{name}`"] + [fmt_pct(summaries[p].filled.get(f"product.{name}", 0),
                                     summaries[p].events["product"]) for p in platforms]
            for name in PRODUCT_FILL_FIELDS]
    out += md_table(["Trường"] + platforms, rows)
    out += ["", "*Thiếu `price_current` thường là sản phẩm \"Liên hệ\" / chưa có giá, chữ gốc vẫn "
            "nằm ở `price_current_raw`. \"-\" = sàn không có loại event này.*", "",
            "*`seller_id` / `seller_name` trống ở CellphoneS, FPT Shop, Hoàng Hà, TGDĐ là bình thường: "
            "đây là cửa hàng tự bán, không có shop bên thứ ba. Tiki là sàn nhiều shop: trang danh sách "
            "có `seller_id` nhưng không có tên shop, tên shop chỉ có ở product event (trang chi tiết).*", ""]

    # 4. Giá
    out += ["## 4. Giá (giá lần cuối thấy của mỗi SKU, VND)", ""]
    rows = []
    for p in platforms:
        s = summaries[p]
        last_prices = sorted(t.last_price for t in s.skus.values() if t.last_price is not None)
        changed = sum(len(t.prices) > 1 for t in s.skus.values())
        on_sale = sum(1 for t in s.skus.values()
                      if t.last_price is not None and t.last_original and t.last_original > t.last_price)
        rows.append([p, fmt_int(len(last_prices)), fmt_int(quantile(last_prices, 0)),
                     fmt_int(quantile(last_prices, 0.25)), fmt_int(quantile(last_prices, 0.5)),
                     fmt_int(quantile(last_prices, 0.75)), fmt_int(quantile(last_prices, 1)),
                     f"{fmt_int(changed)} ({fmt_pct(changed, len(s.skus))})",
                     fmt_pct(on_sale, len(last_prices))])
    out += md_table(["Sàn", "SKU có giá", "Thấp nhất", "P25", "Trung vị", "P75", "Cao nhất",
                     "SKU đổi giá ≥1 lần", "Đang có giá gốc > giá bán"], rows)
    out += ["", "*\"SKU đổi giá\": có ít nhất 2 mức `price_current` khác nhau trong kỳ — đây là "
            "nguyên liệu chính cho phát hiện bất thường giá.*", ""]

    # 5. Danh mục
    out += ["## 5. Danh mục nhiều sản phẩm nhất (theo `category_raw` của product event)", ""]
    for p in platforms:
        counts: dict[str, int] = defaultdict(int)
        for category in summaries[p].categories.values():
            counts[category] += 1
        if not counts:
            continue
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        out += [f"**{p}** ({fmt_int(len(summaries[p].categories))} sản phẩm có danh mục)", ""]
        out += md_table(["Danh mục", "Sản phẩm"], [[c.replace("|", "/"), fmt_int(n)] for c, n in top])
        out.append("")

    # 6. Chất lượng file
    out += ["## 6. File và chất lượng", ""]
    rows = [[p, fmt_int(summaries[p].files), f"{summaries[p].bytes / 1024 ** 2:,.0f} MB".replace(",", "."),
             fmt_int(summaries[p].duplicates["price"] + summaries[p].duplicates["product"]),
             fmt_int(summaries[p].bad_lines),
             " / ".join(f"{k} {fmt_int(v)}" for k, v in sorted(summaries[p].source.items())) or "-",
             ", ".join(f"{t} {fmt_int(n)}" for t, n in sorted(csv_rows.get(p, {}).items()))]
            for p in platforms]
    out += md_table(["Sàn", "File JSONL", "Dung lượng", "Event trùng (đã bỏ)", "Dòng hỏng (đã bỏ)",
                     "Nguồn price event", "Dòng CSV"], rows)
    out += ["", "*Dòng hỏng thường là dòng cuối của file crawler đang ghi dở lúc xuất.*", ""]

    # 7. Crawl runs
    if runs:
        out += ["## 7. Lượt crawl (`state/crawler.db`)", ""]
        out += md_table(["Job", "Sàn", "Trạng thái", "Số lượt", "Event"],
                        [[job, p, status, fmt_int(n), fmt_int(ev)] for job, p, status, n, ev in runs],
                        align_right_from=3)
        out += ["", "*`killed` = bị dừng giữa chừng (Ctrl+C, tắt máy); `events` của lượt killed "
                "có thể là 0 dù dữ liệu đã ghi vào landing/.*", ""]
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Chạy
# ---------------------------------------------------------------------------

def export_all(landing_dir: Path, out_dir: Path, state_db: Path | None, platforms: list[str] | None,
               date_from: date | None, date_to: date | None,
               now: datetime | None = None) -> tuple[dict[str, PlatformSummary], dict[str, dict[str, int]]]:
    """Xuất CSV cho từng sàn và viết summary.md. Trả về (thống kê, số dòng CSV theo sàn/loại)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    names = platforms or discover_platforms(landing_dir)
    summaries: dict[str, PlatformSummary] = {}
    csv_rows: dict[str, dict[str, int]] = {}
    for platform in names:
        summary = PlatformSummary()
        counts = {}
        for event_type in ("price", "product"):
            rows = export_platform_type(landing_dir, out_dir, event_type, platform, summary, date_from, date_to)
            if rows:
                counts[event_type] = rows
            print(f"{platform} {event_type}: {rows} rows")
        if counts:
            summaries[platform] = summary
            csv_rows[platform] = counts
    runs = read_runs(state_db) if state_db else []
    if platforms:
        runs = [r for r in runs if r[1] in platforms]
    report = build_report(summaries, runs, now or datetime.now(timezone.utc), csv_rows)
    (out_dir / "summary.md").write_text(report, encoding="utf-8")
    return summaries, csv_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export landing/ JSONL to CSV and write a summary report")
    parser.add_argument("--platform", action="append", help="only these platforms (repeatable); default: all")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, default=None,
                        help="first partition date YYYY-MM-DD (UTC), inclusive")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, default=None,
                        help="last partition date YYYY-MM-DD (UTC), inclusive")
    parser.add_argument("--out", type=Path, default=Path("exports"), help="output folder (default exports/)")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    settings = load_settings()
    export_all(settings.landing_dir, args.out, settings.state_db, args.platform, args.date_from, args.date_to)
    print(f"Done: CSV + summary.md in {args.out}/")
