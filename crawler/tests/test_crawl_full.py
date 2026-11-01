"""Test bước 9: crawl_full và phần catalog.detail_at của StateStore (HTTP giả, offline)."""

import json
import sqlite3
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
import pytest

from common.http import PlatformBlocked
from common.state import StateStore
from jobs.crawl_full import load_full_categories, run_full, seconds_until
from platforms.hoangha import HoanghaPlatform

FIXTURES = Path(__file__).parent / "fixtures" / "hoangha"
LISTING = json.loads((FIXTURES / "listing_dien_thoai_p1.json").read_text(encoding="utf-8"))["body"]
DETAIL = json.loads((FIXTURES / "detail_normal.json").read_text(encoding="utf-8"))["body"]
T0 = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)


class FakeHoangha:
    """Trang danh sách -> fixture listing; trang chi tiết -> fixture detail. Có thể cài lỗi."""

    def __init__(self, block_after: int | None = None):
        self.calls: list[str] = []
        self.block_after = block_after

    async def get(self, url, *, params=None):
        self.calls.append(url)
        if self.block_after is not None and len(self.calls) > self.block_after:
            raise PlatformBlocked("hoangha: 11 consecutive blocked responses")
        if "/dien-thoai/" in url:
            return httpx.Response(200, html=DETAIL)
        return httpx.Response(200, html=LISTING)


@pytest.fixture
def state(tmp_path):
    with StateStore(tmp_path / "state.db") as s:
        yield s


def read_landing(landing: Path) -> pd.DataFrame:
    return pd.concat([pd.read_json(f, lines=True, dtype=False) for f in sorted(landing.rglob("*.jsonl"))],
                     ignore_index=True)


# ------------------------------ state ------------------------------

def test_products_needing_detail_priority(state):
    state.upsert_products("hoangha", [("old", "u/old"), ("hot", "u/hot"), ("cold", "u/cold")], seen_at=T0)
    state.upsert_products("hoangha", [("hot", "u/hot")], seen_at=T0, is_hot=True)
    state.mark_detail_done("hoangha", "old", T0 - timedelta(days=10))
    rows = state.products_needing_detail("hoangha", limit=10, stale_before=T0 - timedelta(days=7))
    assert [r["product_id"] for r in rows] == ["hot", "cold", "old"]    # chưa lấy (hot trước) -> cũ nhất


def test_recent_detail_not_needed(state):
    state.upsert_products("hoangha", [("p", "u/p")], seen_at=T0)
    state.mark_detail_done("hoangha", "p", T0)
    assert state.products_needing_detail("hoangha", 10, stale_before=T0 - timedelta(days=7)) == []


def test_migration_adds_detail_at_to_old_db(tmp_path):
    # DB tạo bởi phiên bản trước bước 9: bảng catalog chưa có cột detail_at
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE catalog (product_key TEXT PRIMARY KEY, platform TEXT NOT NULL, "
                 "product_id TEXT NOT NULL, url TEXT, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, "
                 "is_hot INTEGER NOT NULL DEFAULT 0)")
    conn.execute("INSERT INTO catalog VALUES ('hoangha:p', 'hoangha', 'p', 'u/p', 'x', 'x', 1)")
    conn.commit()
    conn.close()
    with StateStore(path) as s:
        assert [r["product_id"] for r in s.products_needing_detail("hoangha", 10, T0)] == ["p"]


# ------------------------------ crawl_full ------------------------------

def test_load_full_categories(tmp_path):
    path = tmp_path / "c.csv"
    path.write_text("# x\nplatform,category,enabled\nhoangha,laptop,1\ntiki,1795,0\nhoangha,tablet,1\n")
    assert load_full_categories(path) == {"hoangha": ["laptop", "tablet"]}


def test_repo_full_categories_valid():
    from common.config import load_platform_limits
    from platforms import PLATFORM_CLASSES
    cats = load_full_categories("config/full_categories.csv")
    limits = load_platform_limits("config/platforms.yaml")
    assert cats
    for name, rows in cats.items():
        assert name in PLATFORM_CLASSES and name in limits
        assert len(set(rows)) == len(rows)


@pytest.mark.asyncio
async def test_run_full_listing_then_details(tmp_path, state):
    http = FakeHoangha()
    platform = HoanghaPlatform(http=http, crawler_version="0.1.0")
    landing = tmp_path / "landing"
    count = await run_full(platform, ["dien-thoai-di-dong"], state, landing, max_details=3, detail_stale_days=7)

    # discover: 1 lần gọi trang 1 để đếm + 11 trang; rồi 3 trang chi tiết
    assert len([u for u in http.calls if "/dien-thoai-di-dong" in u]) == 12
    assert len([u for u in http.calls if "/dien-thoai/" in u]) == 3
    df = read_landing(landing)
    assert set(df["event_type"]) == {"price", "product"}
    assert (df["event_type"] == "product").sum() == 3
    assert count == len(df)

    run = state.list_runs("crawl_full")[0]
    assert run["status"] == "ok" and run["events_count"] == count
    # 3 sản phẩm đã có detail_at, catalog không bị đánh is_hot
    pending = state.products_needing_detail("hoangha", 100, stale_before=datetime.now(timezone.utc) - timedelta(days=7))
    assert len(pending) == state.count_products("hoangha") - 3
    assert state.get_product("hoangha", "iphone-18-pro-max")["is_hot"] == 0


@pytest.mark.asyncio
async def test_run_full_blocked_marks_failed(tmp_path, state):
    http = FakeHoangha(block_after=3)
    platform = HoanghaPlatform(http=http, crawler_version="0.1.0")
    await run_full(platform, ["dien-thoai-di-dong"], state, tmp_path / "landing", max_details=3, detail_stale_days=7)
    run = state.list_runs("crawl_full")[0]
    assert run["status"] == "failed" and "blocked" in run["error"]
    assert run["events_count"] > 0                       # trang đã crawl trước khi bị chặn vẫn được tính


def test_seconds_until_daily_time():
    now = datetime(2026, 11, 1, 16, 0, tzinfo=timezone.utc)
    assert seconds_until(time(17, 0), now) == 3600
    assert seconds_until(time(15, 0), now) == 23 * 3600         # đã qua giờ hôm nay -> ngày mai
