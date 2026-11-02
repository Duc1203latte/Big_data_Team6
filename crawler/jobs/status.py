"""Xem nhanh tình trạng crawler (chỉ đọc, không gửi request nào).

In ra:
1. Số event theo sàn trong N giờ gần nhất (đếm dòng trong landing/, theo partition giờ UTC),
   kèm tỉ lệ thiếu giá và thiếu tên của price event - tăng đột ngột thường là sàn đổi giao diện.
2. Các run gần nhất của mỗi job/sàn, và mọi run failed/killed trong khoảng thời gian đó.
3. Khoảng trống dữ liệu của crawl_hot: hai vòng liên tiếp cách nhau quá 2 lần chu kỳ
   (máy tắt, sleep, crawler bị dừng...).
4. Số sản phẩm trong catalog và số đã có trang chi tiết.

Chạy (trong thư mục crawler/):
    python -m jobs.status              # 24 giờ gần nhất
    python -m jobs.status --hours 72
"""

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
from rich.console import Console
from rich.table import Table

from common.config import load_platform_limits, load_settings
from common.state import StateStore

console = Console()


@dataclass
class PlatformStats:
    events: int = 0
    price_events: int = 0
    price_missing: int = 0
    name_missing: int = 0


def partition_time(path: Path) -> datetime | None:
    """landing/<type>/platform=x/date=YYYY-MM-DD/hour=HH/part-....jsonl -> thời điểm đầu giờ (UTC)."""
    try:
        date = path.parent.parent.name.removeprefix("date=")
        hour = path.parent.name.removeprefix("hour=")
        return datetime.fromisoformat(f"{date}T{hour}:00:00+00:00")
    except ValueError:
        return None


def collect_event_stats(landing_dir: Path, since: datetime) -> tuple[dict[str, PlatformStats], dict[str, dict[datetime, int]]]:
    """Đọc các file JSONL có partition giờ >= since.
    Trả về (thống kê theo sàn, số event theo sàn theo giờ)."""
    stats: dict[str, PlatformStats] = defaultdict(PlatformStats)
    by_hour: dict[str, dict[datetime, int]] = defaultdict(lambda: defaultdict(int))
    since_hour = since.replace(minute=0, second=0, microsecond=0)
    for path in landing_dir.glob("*/platform=*/date=*/hour=*/*.jsonl"):
        hour = partition_time(path)
        if hour is None or hour < since_hour:
            continue
        platform = path.parent.parent.parent.name.removeprefix("platform=")
        is_price = path.parts[-5] == "price"
        with path.open("rb") as f:
            for line in f:
                if not line.strip():
                    continue
                stats[platform].events += 1
                by_hour[platform][hour] += 1
                if is_price:
                    try:
                        event = orjson.loads(line)
                    except orjson.JSONDecodeError:
                        continue
                    stats[platform].price_events += 1
                    stats[platform].price_missing += event.get("price_current") is None
                    stats[platform].name_missing += not event.get("name_raw")
    return stats, by_hour


def find_gaps(runs: list[dict], cycle_minutes: dict[str, int], since: datetime,
              now: datetime) -> list[tuple[str, datetime, datetime]]:
    """Khoảng trống của crawl_hot: hai lần bắt đầu liên tiếp (hoặc lần cuối tới bây giờ)
    cách nhau quá 2 lần chu kỳ của sàn."""
    starts: dict[str, list[datetime]] = defaultdict(list)
    for run in runs:
        if run["job"] == "crawl_hot":
            starts[run["platform"]].append(datetime.fromisoformat(run["started_at"]))
    gaps = []
    for platform, times in starts.items():
        limit = timedelta(minutes=2 * cycle_minutes.get(platform, 30))
        times = sorted(t for t in times if t >= since - limit)
        for a, b in zip(times, times[1:] + [now]):
            if b - a > limit:
                gaps.append((platform, a, b))
    return sorted(gaps, key=lambda g: g[1])


def fmt_time(ts: str | datetime | None) -> str:
    """Giờ UTC -> giờ Việt Nam cho dễ đọc."""
    if ts is None:
        return "-"
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return ts.astimezone(timezone(timedelta(hours=7))).strftime("%d/%m %H:%M")


def pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.1f}%" if whole else "-"


def show_status(hours: float) -> None:
    settings = load_settings()
    limits = load_platform_limits()
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    console.print(f"[bold]Tình trạng crawler[/bold] — {hours:g} giờ gần nhất "
                  f"(từ {fmt_time(since)} tới {fmt_time(now)}, giờ VN)\n")

    # 1. Event theo sàn
    stats, by_hour = collect_event_stats(settings.landing_dir, since)
    table = Table(title="Event đã ghi (landing/)")
    for col in ("Sàn", "Event", "Price event", "Thiếu giá", "Thiếu tên", "Giờ gần nhất có dữ liệu"):
        table.add_column(col, justify="right" if col not in ("Sàn",) else "left")
    for platform in sorted(stats):
        s = stats[platform]
        last_hour = max(by_hour[platform]) if by_hour[platform] else None
        table.add_row(platform, f"{s.events:,}", f"{s.price_events:,}", pct(s.price_missing, s.price_events),
                      pct(s.name_missing, s.price_events), fmt_time(last_hour))
    total = sum(s.events for s in stats.values())
    table.add_row("[bold]Tổng", f"[bold]{total:,}", "", "", "", f"~{total / hours / 3600:.2f} event/giây")
    console.print(table)

    with StateStore(settings.state_db) as state:
        runs = [r for r in state.list_runs() if r["started_at"] >= since.isoformat()]
        all_runs = state.list_runs()

        # 2. Run gần nhất của mỗi job/sàn
        latest: dict[tuple[str, str], dict] = {}
        for run in all_runs:
            latest[(run["job"], run["platform"])] = run
        table = Table(title="Run gần nhất")
        for col in ("Job", "Sàn", "Trạng thái", "Event", "Bắt đầu", "Kết thúc", "Lỗi"):
            table.add_column(col)
        colors = {"ok": "green", "running": "cyan", "failed": "red", "killed": "yellow"}
        for (job, platform), run in sorted(latest.items()):
            c = colors.get(run["status"], "white")
            table.add_row(job, platform, f"[{c}]{run['status']}", f"{run['events_count']:,}",
                          fmt_time(run["started_at"]), fmt_time(run["ended_at"]), (run["error"] or "")[:60])
        console.print(table)

        bad = [r for r in runs if r["status"] in ("failed", "killed")]
        if bad:
            table = Table(title=f"Run failed / killed trong {hours:g} giờ")
            for col in ("Run", "Trạng thái", "Event", "Bắt đầu", "Kết thúc", "Lỗi"):
                table.add_column(col)
            for r in bad:
                table.add_row(r["run_id"], r["status"], f"{r['events_count']:,}",
                              fmt_time(r["started_at"]), fmt_time(r["ended_at"]), (r["error"] or "")[:60])
            console.print(table)
        else:
            console.print(f"[green]Không có run failed/killed trong {hours:g} giờ.[/green]\n")

        # 3. Khoảng trống
        cycles = {name: lim.hot_cycle_minutes for name, lim in limits.items()}
        gaps = find_gaps(all_runs, cycles, since, now)
        if gaps:
            table = Table(title="Khoảng trống crawl_hot (> 2 lần chu kỳ không có vòng nào)")
            for col in ("Sàn", "Từ", "Tới", "Độ dài"):
                table.add_column(col)
            for platform, a, b in gaps:
                table.add_row(platform, fmt_time(a), fmt_time(b), f"{(b - a).total_seconds() / 3600:.1f} giờ")
            console.print(table)
        else:
            console.print("[green]Không có khoảng trống crawl_hot.[/green]\n")

        # 4. Catalog
        table = Table(title="Catalog")
        for col in ("Sàn", "Sản phẩm", "Nhóm hot", "Đã có trang chi tiết"):
            table.add_column(col, justify="right" if col != "Sàn" else "left")
        rows = state.conn.execute(
            "SELECT platform, COUNT(*), SUM(is_hot), SUM(detail_at IS NOT NULL) FROM catalog GROUP BY platform"
        ).fetchall()
        for platform, count, hot, detailed in rows:
            table.add_row(platform, f"{count:,}", f"{hot or 0:,}", f"{detailed or 0:,}")
        console.print(table)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show crawler status (read-only)")
    parser.add_argument("--hours", type=float, default=24, help="look back this many hours (default 24)")
    return parser.parse_args(argv)


if __name__ == "__main__":
    show_status(parse_args().hours)
