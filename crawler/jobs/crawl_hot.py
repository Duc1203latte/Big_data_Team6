"""Crawl nhóm theo dõi sát (README bước 7).

Đọc config/watchlist.csv (các trang danh sách cần theo dõi), mỗi sàn chạy một vòng
lặp riêng song song:

    mỗi chu kỳ (mặc định 30 phút, CellphoneS 60 phút - xem platforms.yaml):
        start_run -> crawl lần lượt các trang của sàn -> ghi event qua sink (SINK trong .env,
        mặc định file = JSONL trong landing/) -> cập nhật catalog -> finish_run -> ngủ tới chu kỳ sau

Mỗi chu kỳ là một dòng trong crawl_runs (đã thống nhất), nên khoảng trống dữ liệu
khi tắt máy được ghi lại chính xác tới từng vòng.

Xử lý lỗi:
- Một trang lỗi (mạng, robots.txt cấm, parse không ra gì): log cảnh báo, sang trang sau.
- Sàn bị chặn liên tục (PlatformBlocked): đánh run là failed, DỪNG sàn đó, các sàn khác vẫn chạy.
- Lỗi bất ngờ khác: đánh run là failed, chu kỳ sau thử lại.
- Ctrl+C / SIGTERM: dừng ngay, run đang chạy được đánh killed kèm giờ dừng.

Chạy (trong thư mục crawler/):
    python -m jobs.crawl_hot                    # mọi sàn có trong watchlist, chạy mãi
    python -m jobs.crawl_hot --platform hoangha # chỉ một sàn
    python -m jobs.crawl_hot --once             # mỗi sàn chạy đúng một vòng rồi thoát
"""

import argparse
import asyncio
import csv
import logging
import signal
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path

import httpx

from common.config import PlatformLimits, load_platform_limits, load_settings
from common.http import HttpClient, PlatformBlocked, RobotsDisallowed
from common.logging_setup import setup_logging
from common.schema import ProductRef
from common.sink import EventSink, SinkConfig, make_sink
from common.state import StateStore
from platforms import PLATFORM_CLASSES
from platforms.base import Platform

logger = logging.getLogger("crawl_hot")

JOB_NAME = "crawl_hot"


@dataclass(frozen=True)
class WatchItem:
    """Một dòng của watchlist.csv: một trang danh sách cần crawl."""

    platform: str
    category: str
    page: int | None
    url: str


def load_watchlist(path: str | Path) -> dict[str, list[WatchItem]]:
    """watchlist.csv -> {sàn: [WatchItem, ...]}, chỉ lấy dòng enabled = 1, giữ thứ tự trong file.

    Cột: platform,category,page,url,enabled. Dòng bắt đầu bằng # là chú thích.
    """
    items: dict[str, list[WatchItem]] = {}
    with open(path, encoding="utf-8", newline="") as f:
        rows = csv.DictReader(line for line in f if line.strip() and not line.startswith("#"))
        for row in rows:
            if row.get("enabled", "1").strip() != "1":
                continue
            page = row.get("page", "").strip()
            item = WatchItem(
                platform=row["platform"].strip(),
                category=row["category"].strip(),
                page=int(page) if page else None,
                url=row["url"].strip(),
            )
            items.setdefault(item.platform, []).append(item)
    return items


async def crawl_page(platform: Platform, ref: ProductRef, sink: EventSink, state: StateStore,
                     is_hot: bool | None) -> int:
    """Crawl một trang: fetch -> parse -> bỏ event trùng -> ghi JSONL -> cập nhật catalog.

    Trả về số event đã ghi (0 nếu trang lỗi hoặc không có dữ liệu).
    PlatformBlocked không bắt ở đây: để vòng ngoài dừng cả sàn. Dùng chung cho crawl_full.
    """
    try:
        raw = await platform.fetch(ref)
    except RobotsDisallowed as exc:
        logger.warning("Skip %s: %s", ref.url, exc)
        return 0
    except httpx.HTTPError as exc:
        logger.warning("Skip %s: %s", ref.url, type(exc).__name__)
        return 0

    # Cùng một sản phẩm có thể xuất hiện 2 lần trên một trang (trang tổng hợp, ô quảng cáo):
    # cùng giây, cùng sản phẩm -> cùng event_id, giữ một bản để không ghi trùng
    events = list({e.event_id: e for e in platform.parse(raw)}.values())
    if not events:
        logger.warning("No events from %s (HTTP %s)", ref.url, raw.status_code)
        return 0
    sink.write(events)
    state.upsert_products(
        platform.name,
        [(e.product_id, e.url) for e in events if e.event_type == "price"],
        seen_at=raw.fetched_at,
        is_hot=is_hot,
    )
    return len(events)


async def run_cycle(platform: Platform, items: list[WatchItem], state: StateStore, landing_dir: Path,
                    sink_kind: str | SinkConfig = "file") -> int:
    """Một vòng crawl của một sàn. Trả về số event đã ghi.

    Luôn finish_run trong finally, kể cả khi bị chặn hoặc bị Ctrl+C,
    để crawl_runs không còn dòng "running" treo.
    """
    run_id = state.start_run(JOB_NAME, platform.name)
    sink = make_sink(sink_kind, run_id, landing_dir=landing_dir)
    events_total = 0
    pages_empty = 0
    status, error = "ok", None
    try:
        for item in items:
            ref = ProductRef(platform=platform.name, kind="listing", url=item.url,
                             category=item.category, page=item.page)
            count = await crawl_page(platform, ref, sink, state, is_hot=True)
            events_total += count
            pages_empty += count == 0
        if pages_empty == len(items):
            # Không trang nào ra dữ liệu: coi là thất bại để dễ thấy khi xem crawl_runs
            status, error = "failed", "no events from any page"
    except PlatformBlocked as exc:
        status, error = "failed", str(exc)
        raise
    except asyncio.CancelledError:
        status, error = "killed", "stopped by signal"
        raise
    except Exception as exc:      # lỗi bất ngờ: ghi lại, chu kỳ sau thử lại
        status, error = "failed", repr(exc)
        logger.exception("Cycle of %s crashed", platform.name)
    finally:
        sink.close()
        state.finish_run(run_id, status, events_total, error)
    logger.info("%s: %d events from %d pages (%d empty)", platform.name, events_total, len(items), pages_empty)
    return events_total


async def platform_loop(platform: Platform, items: list[WatchItem], state: StateStore,
                        landing_dir: Path, cycle_seconds: float, stop: asyncio.Event,
                        once: bool = False, sink_kind: str | SinkConfig = "file") -> None:
    """Chạy run_cycle lặp lại mỗi cycle_seconds cho tới khi có tín hiệu dừng."""
    while not stop.is_set():
        started = time.monotonic()
        try:
            await run_cycle(platform, items, state, landing_dir, sink_kind)
        except PlatformBlocked as exc:
            logger.critical("%s is blocking us, platform stopped: %s", platform.name, exc)
            return
        if once:
            return
        wait = cycle_seconds - (time.monotonic() - started)
        if wait <= 0:
            logger.warning("%s cycle overran by %.0fs, starting next cycle now", platform.name, -wait)
            continue
        logger.info("%s: next cycle in %.0f min", platform.name, wait / 60)
        try:
            # Ngủ tới chu kỳ sau, nhưng tỉnh dậy ngay khi có tín hiệu dừng
            await asyncio.wait_for(stop.wait(), timeout=wait)
        except asyncio.TimeoutError:
            pass


async def crawl_hot(platform_names: list[str] | None, watchlist_path: Path, once: bool) -> None:
    settings = load_settings()
    setup_logging(level=settings.log_level)
    limits: dict[str, PlatformLimits] = load_platform_limits()
    watchlist = load_watchlist(watchlist_path)
    logger.info("Sink: %s%s", settings.sink,
                f" (Kafka {settings.kafka_bootstrap_servers})" if "kafka" in settings.sink else "")

    names = platform_names or sorted(watchlist)
    for name in names:
        if name not in PLATFORM_CLASSES or name not in limits:
            raise SystemExit(f"Unknown platform or missing config: {name}")
        if not watchlist.get(name):
            raise SystemExit(f"No enabled watchlist rows for {name}")

    stop = asyncio.Event()
    tasks: list[asyncio.Task] = []

    def on_signal(sig: signal.Signals) -> None:
        logger.warning("Received %s, stopping...", sig.name)
        stop.set()
        for task in tasks:
            task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal, sig)

    with StateStore(settings.state_db) as state:
        async with AsyncExitStack() as stack:
            for name in names:
                http = await stack.enter_async_context(HttpClient(name, limits[name], settings.user_agent))
                platform = PLATFORM_CLASSES[name](http=http, crawler_version=settings.crawler_version)
                cycle_seconds = limits[name].hot_cycle_minutes * 60
                logger.info("Starting %s: %d pages every %d min",
                            name, len(watchlist[name]), limits[name].hot_cycle_minutes)
                tasks.append(asyncio.create_task(
                    platform_loop(platform, watchlist[name], state, settings.landing_dir,
                                  cycle_seconds, stop, once, settings.sink_config),
                    name=name,
                ))
            await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("crawl_hot stopped")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl the hot watchlist in a loop")
    parser.add_argument("--platform", action="append", choices=sorted(PLATFORM_CLASSES),
                        help="only these platforms (repeatable); default: all in watchlist")
    parser.add_argument("--watchlist", type=Path, default=Path("config/watchlist.csv"))
    parser.add_argument("--once", action="store_true", help="run one cycle per platform then exit")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(crawl_hot(args.platform, args.watchlist, args.once))
