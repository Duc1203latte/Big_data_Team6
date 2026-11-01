"""Crawl toàn danh mục, mỗi ngày một lần (README bước 9).

Cho từng sàn (các sàn chạy song song, tốc độ thấp hơn crawl_hot: xem for_full_crawl):

1. Phần danh sách: với mỗi danh mục trong config/full_categories.csv, gọi
   platform.discover() để liệt kê mọi trang, crawl từng trang -> PriceEvent,
   cập nhật catalog (first_seen / last_seen; không đổi is_hot).
2. Phần chi tiết: lấy trang chi tiết của tối đa max_details_per_run sản phẩm trong catalog
   chưa lấy bao giờ hoặc lấy đã quá --detail-stale-days ngày -> ProductEvent (thông số,
   thương hiệu, phân bố sao) + PriceEvent(source="detail"). Đánh dấu catalog.detail_at.

Mỗi lần chạy của một sàn là một dòng crawl_runs (job = crawl_full). Xử lý lỗi giống
crawl_hot: trang lỗi thì bỏ qua, sàn chặn thì dừng sàn đó, Ctrl+C thì run = killed.

Chạy (trong thư mục crawler/):
    python -m jobs.crawl_full                       # chạy một lần rồi thoát
    python -m jobs.crawl_full --daily-at 17:00      # chạy mãi, mỗi ngày lúc 17:00 UTC (00:00 VN)
    python -m jobs.crawl_full --platform hoangha --max-details 20
"""

import argparse
import asyncio
import csv
import logging
import signal
from contextlib import AsyncExitStack
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path

import httpx

from common.config import load_platform_limits, load_settings
from common.http import HttpClient, PlatformBlocked, RobotsDisallowed
from common.logging_setup import setup_logging
from common.schema import ProductRef
from common.sink import FileSink
from common.state import StateStore, utc_now
from jobs.crawl_hot import crawl_page
from platforms import PLATFORM_CLASSES
from platforms.base import Platform

logger = logging.getLogger("crawl_full")

JOB_NAME = "crawl_full"


def load_full_categories(path: str | Path) -> dict[str, list[str]]:
    """full_categories.csv -> {sàn: [danh mục, ...]}, chỉ dòng enabled = 1. Dòng # là chú thích."""
    result: dict[str, list[str]] = {}
    with open(path, encoding="utf-8", newline="") as f:
        rows = csv.DictReader(line for line in f if line.strip() and not line.startswith("#"))
        for row in rows:
            if row.get("enabled", "1").strip() == "1":
                result.setdefault(row["platform"].strip(), []).append(row["category"].strip())
    return result


async def run_full(platform: Platform, categories: list[str], state: StateStore, landing_dir: Path,
                   max_details: int, detail_stale_days: float) -> int:
    """Một lần crawl toàn danh mục + trang chi tiết của một sàn. Trả về số event đã ghi."""
    run_id = state.start_run(JOB_NAME, platform.name)
    sink = FileSink(landing_dir, run_id)
    events_total = listing_pages = details_done = 0
    status, error = "ok", None
    try:
        # 1. Phần danh sách
        for category in categories:
            try:
                async for ref in platform.discover(category):
                    events_total += await crawl_page(platform, ref, sink, state, is_hot=None)
                    listing_pages += 1
            except (RobotsDisallowed, httpx.HTTPError) as exc:
                logger.warning("Skip category %s of %s: %s", category, platform.name, type(exc).__name__)

        # 2. Phần chi tiết
        stale_before = utc_now() - timedelta(days=detail_stale_days)
        for row in state.products_needing_detail(platform.name, max_details, stale_before):
            ref = ProductRef(platform=platform.name, kind="detail", url=row["url"], product_id=row["product_id"])
            count = await crawl_page(platform, ref, sink, state, is_hot=None)
            if count:
                state.mark_detail_done(platform.name, row["product_id"], utc_now())
                details_done += 1
            events_total += count

        if events_total == 0:
            status, error = "failed", "no events"
    except PlatformBlocked as exc:
        status, error = "failed", str(exc)
        logger.critical("%s is blocking us, full crawl stopped: %s", platform.name, exc)
    except asyncio.CancelledError:
        status, error = "killed", "stopped by signal"
        raise
    except Exception as exc:
        status, error = "failed", repr(exc)
        logger.exception("Full crawl of %s crashed", platform.name)
    finally:
        sink.close()
        state.finish_run(run_id, status, events_total, error)
    logger.info("%s full crawl: %d events, %d listing pages, %d details",
                platform.name, events_total, listing_pages, details_done)
    return events_total


def seconds_until(daily_at: dtime, now: datetime) -> float:
    """Số giây từ now tới lần kế tiếp đồng hồ (UTC) chỉ daily_at."""
    target = now.replace(hour=daily_at.hour, minute=daily_at.minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def crawl_full(platform_names: list[str] | None, categories_path: Path, max_details: int | None,
                     detail_stale_days: float, daily_at: dtime | None) -> None:
    settings = load_settings()
    setup_logging(level=settings.log_level)
    limits = load_platform_limits()
    categories = load_full_categories(categories_path)

    names = platform_names or sorted(categories)
    for name in names:
        if name not in PLATFORM_CLASSES or name not in limits:
            raise SystemExit(f"Unknown platform or missing config: {name}")
        if not categories.get(name):
            raise SystemExit(f"No enabled categories for {name} in {categories_path}")

    main_task = asyncio.current_task()

    def on_signal(sig: signal.Signals) -> None:
        logger.warning("Received %s, stopping...", sig.name)
        main_task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal, sig)

    try:
        while True:
            if daily_at is not None:
                wait = seconds_until(daily_at, datetime.now(timezone.utc))
                logger.info("Next full crawl in %.1f h (at %s UTC)", wait / 3600, daily_at.strftime("%H:%M"))
                await asyncio.sleep(wait)
            with StateStore(settings.state_db) as state:
                async with AsyncExitStack() as stack:
                    tasks = []
                    for name in names:
                        full_limits = limits[name].for_full_crawl()
                        http = await stack.enter_async_context(HttpClient(name, full_limits, settings.user_agent))
                        platform = PLATFORM_CLASSES[name](http=http, crawler_version=settings.crawler_version)
                        logger.info("Full crawl %s: %d categories at %.2f req/s",
                                    name, len(categories[name]), full_limits.requests_per_second)
                        tasks.append(run_full(platform, categories[name], state, settings.landing_dir,
                                              max_details or limits[name].max_details_per_run,
                                              detail_stale_days))
                    await asyncio.gather(*tasks)
            if daily_at is None:
                break
    except asyncio.CancelledError:
        logger.info("crawl_full stopped")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily full-catalog crawl")
    parser.add_argument("--platform", action="append", choices=sorted(PLATFORM_CLASSES))
    parser.add_argument("--categories", type=Path, default=Path("config/full_categories.csv"))
    parser.add_argument("--max-details", type=int, default=None,
                        help="detail pages per platform per run (default: max_details_per_run in platforms.yaml)")
    parser.add_argument("--detail-stale-days", type=float, default=7,
                        help="re-fetch detail pages older than this many days")
    parser.add_argument("--daily-at", type=lambda s: dtime.fromisoformat(s), default=None,
                        help="run forever, every day at HH:MM UTC")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(crawl_full(args.platform, args.categories, args.max_details,
                           args.detail_stale_days, args.daily_at))
