"""Test bước 7: crawl_hot chạy với HTTP giả (trả fixture Hoàng Hà), không gọi mạng."""

import asyncio
import json
from pathlib import Path

import httpx
import pandas as pd
import pytest

from common.config import load_platform_limits
from common.http import PlatformBlocked
from common.state import StateStore
from jobs.crawl_hot import WatchItem, load_watchlist, platform_loop, run_cycle
from platforms import PLATFORM_CLASSES
from platforms.hoangha import HoanghaPlatform

FIXTURE = Path(__file__).parent / "fixtures" / "hoangha" / "listing_dien_thoai_p1.json"
LISTING_HTML = json.loads(FIXTURE.read_text(encoding="utf-8"))["body"]

ITEMS = [
    WatchItem("hoangha", "dien-thoai-di-dong", 1, "https://hoanghamobile.com/dien-thoai-di-dong"),
    WatchItem("hoangha", "dien-thoai-di-dong", 2, "https://hoanghamobile.com/dien-thoai-di-dong?p=2"),
]


class FakeHttp:
    """Trả trang danh sách thật (fixture) cho mọi URL; có thể cài lỗi cho từng URL."""

    def __init__(self, errors: dict[str, Exception] | None = None, delay: float = 0.0):
        self.errors = errors or {}
        self.delay = delay
        self.calls: list[str] = []

    async def get(self, url, *, params=None):
        self.calls.append(url)
        if self.delay:
            await asyncio.sleep(self.delay)
        if url in self.errors:
            raise self.errors[url]
        return httpx.Response(200, html=LISTING_HTML)


@pytest.fixture
def state(tmp_path):
    with StateStore(tmp_path / "state.db") as s:
        yield s


def hoangha(http) -> HoanghaPlatform:
    return HoanghaPlatform(http=http, crawler_version="0.1.0")


def read_landing(landing: Path) -> pd.DataFrame:
    files = sorted(landing.rglob("*.jsonl"))
    return pd.concat([pd.read_json(f, lines=True, dtype=False) for f in files], ignore_index=True)


# ------------------------------ watchlist ------------------------------

def test_load_watchlist_skips_comments_and_disabled(tmp_path):
    path = tmp_path / "watchlist.csv"
    path.write_text(
        "# chú thích\n"
        "platform,category,page,url,enabled\n"
        "hoangha,dien-thoai-di-dong,1,https://a/1,1\n"
        "tiki,1795,1,https://b/1,0\n"
        "cellphones,mobile,,https://c/mobile.html,1\n"
        "hoangha,dien-thoai-di-dong,2,https://a/2,1\n",
        encoding="utf-8",
    )
    items = load_watchlist(path)
    assert set(items) == {"hoangha", "cellphones"}
    assert [i.page for i in items["hoangha"]] == [1, 2]
    assert items["cellphones"][0].page is None


def test_repo_watchlist_is_consistent():
    # Mọi sàn đang bật trong watchlist phải có class Platform và cấu hình trong platforms.yaml
    items = load_watchlist("config/watchlist.csv")
    limits = load_platform_limits("config/platforms.yaml")
    assert items, "watchlist rỗng"
    for name, rows in items.items():
        assert name in PLATFORM_CLASSES and name in limits
        assert len({r.url for r in rows}) == len(rows), f"URL trùng trong watchlist của {name}"


# ------------------------------ run_cycle ------------------------------

@pytest.mark.asyncio
async def test_cycle_writes_jsonl_run_and_catalog(tmp_path, state):
    landing = tmp_path / "landing"
    count = await run_cycle(hoangha(FakeHttp()), ITEMS, state, landing)
    assert count == 36                                         # 2 trang x 18 sản phẩm

    df = read_landing(landing)
    assert len(df) == 36
    assert set(df["platform"]) == {"hoangha"}
    assert (landing / "price" / "platform=hoangha").is_dir()

    run = state.list_runs("crawl_hot")[0]
    assert run["status"] == "ok" and run["events_count"] == 36 and run["ended_at"]
    assert state.count_products("hoangha") == 18               # 2 trang giống nhau -> 18 sản phẩm
    assert state.get_product("hoangha", "iphone-18-pro-max")["is_hot"] == 1


@pytest.mark.asyncio
async def test_failed_page_is_skipped(tmp_path, state):
    http = FakeHttp(errors={ITEMS[0].url: httpx.ConnectError("down")})
    count = await run_cycle(hoangha(http), ITEMS, state, tmp_path / "landing")
    assert count == 18
    assert state.list_runs()[0]["status"] == "ok"


@pytest.mark.asyncio
async def test_all_pages_empty_marks_run_failed(tmp_path, state):
    class EmptyHttp(FakeHttp):
        async def get(self, url, *, params=None):
            return httpx.Response(200, html="<html><body>maintenance</body></html>")

    count = await run_cycle(hoangha(EmptyHttp()), ITEMS, state, tmp_path / "landing")
    run = state.list_runs()[0]
    assert count == 0
    assert run["status"] == "failed" and run["error"] == "no events from any page"


@pytest.mark.asyncio
async def test_blocked_platform_marks_failed_and_stops_loop(tmp_path, state):
    http = FakeHttp(errors={ITEMS[1].url: PlatformBlocked("hoangha: 11 consecutive blocked responses")})
    stop = asyncio.Event()
    await platform_loop(hoangha(http), ITEMS, state, tmp_path / "landing", cycle_seconds=0.01, stop=stop)
    runs = state.list_runs()
    assert len(runs) == 1                                      # không chạy thêm chu kỳ nào
    assert runs[0]["status"] == "failed" and "blocked" in runs[0]["error"]
    assert runs[0]["events_count"] == 18                       # trang 1 đã ghi trước khi bị chặn


@pytest.mark.asyncio
async def test_cancel_marks_run_killed_with_end_time(tmp_path, state):
    task = asyncio.create_task(run_cycle(hoangha(FakeHttp(delay=10)), ITEMS, state, tmp_path / "landing"))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    run = state.list_runs()[0]
    assert run["status"] == "killed" and run["ended_at"] is not None
    assert run["error"] == "stopped by signal"


# ------------------------------ platform_loop ------------------------------

@pytest.mark.asyncio
async def test_loop_once_runs_single_cycle(tmp_path, state):
    stop = asyncio.Event()
    await platform_loop(hoangha(FakeHttp()), ITEMS, state, tmp_path / "landing",
                        cycle_seconds=0.01, stop=stop, once=True)
    assert len(state.list_runs()) == 1


@pytest.mark.asyncio
async def test_loop_repeats_until_stopped_without_duplicate_event_ids(tmp_path, state):
    stop = asyncio.Event()
    landing = tmp_path / "landing"
    # Một trang mỗi chu kỳ: FakeHttp trả cùng nội dung cho mọi URL, nếu 2 trang trong cùng
    # một giây thì cùng sản phẩm sẽ (đúng thiết kế) ra cùng event_id. Ngoài thực tế token
    # bucket giãn các request cách nhau >= 5 giây nên không xảy ra.
    task = asyncio.create_task(platform_loop(hoangha(FakeHttp()), ITEMS[:1], state, landing,
                                             cycle_seconds=1.1, stop=stop))
    await asyncio.sleep(1.3)                                   # đủ cho 2 chu kỳ (cách nhau 1,1 giây)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    runs = state.list_runs()
    assert len(runs) == 2 and all(r["status"] == "ok" for r in runs)
    df = read_landing(landing)
    # Hai chu kỳ lệch nhau >= 1 giây nên event_time khác nhau -> event_id không trùng
    assert df["event_id"].is_unique
    assert len(df) == 36
