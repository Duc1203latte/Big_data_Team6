"""Test bước 3: SQLite state (crawl_runs, catalog) và cấu hình log. Chạy offline."""

import logging
from datetime import datetime, timedelta, timezone

import pytest

from common.logging_setup import setup_logging
from common.state import StateStore, make_run_id

T0 = datetime(2026, 11, 1, 8, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def state(tmp_path):
    with StateStore(tmp_path / "crawler.db") as s:
        yield s


# ---------------------------- crawl_runs ----------------------------

def test_start_and_finish_run(state):
    run_id = state.start_run("crawl_hot", "tiki", started_at=T0)
    assert run_id == "crawl_hot-tiki-20261101T080000Z"
    assert state.get_run(run_id)["status"] == "running"
    assert state.get_run(run_id)["ended_at"] is None

    state.finish_run(run_id, "ok", events_count=120, ended_at=T0 + timedelta(minutes=5))
    run = state.get_run(run_id)
    assert run["status"] == "ok"
    assert run["events_count"] == 120
    assert run["ended_at"] == "2026-11-01T08:05:00+00:00"


def test_failed_run_keeps_error(state):
    run_id = state.start_run("crawl_hot", "tiki", started_at=T0)
    state.finish_run(run_id, "failed", events_count=3, error="ConnectError")
    assert state.get_run(run_id)["error"] == "ConnectError"


def test_stale_running_marked_killed_on_next_start(tmp_path):
    # Lần 1: bật job rồi "tắt máy" (không gọi finish_run)
    with StateStore(tmp_path / "crawler.db") as s:
        old = s.start_run("crawl_hot", "tiki", started_at=T0)
    # Lần 2: mở lại DB, bật job lại -> dòng cũ phải thành killed
    with StateStore(tmp_path / "crawler.db") as s:
        new = s.start_run("crawl_hot", "tiki", started_at=T0 + timedelta(hours=3))
        assert s.get_run(old)["status"] == "killed"
        assert s.get_run(new)["status"] == "running"


def test_other_job_not_marked_killed(state):
    hot = state.start_run("crawl_hot", "tiki", started_at=T0)
    state.start_run("crawl_full", "tiki", started_at=T0 + timedelta(minutes=1))
    assert state.get_run(hot)["status"] == "running"


def test_finish_unknown_run_raises(state):
    with pytest.raises(KeyError):
        state.finish_run("does-not-exist", "ok", events_count=0)


def test_invalid_status_rejected(state):
    run_id = state.start_run("crawl_hot", "tiki", started_at=T0)
    with pytest.raises(ValueError):
        state.finish_run(run_id, "done", events_count=0)


def test_list_runs_ordered(state):
    state.start_run("crawl_hot", "tiki", started_at=T0 + timedelta(hours=1))
    state.start_run("crawl_hot", "cellphones", started_at=T0)
    runs = state.list_runs("crawl_hot")
    assert [r["platform"] for r in runs] == ["cellphones", "tiki"]


def test_run_id_uses_utc():
    vn = datetime(2026, 11, 1, 15, 0, 0, tzinfo=timezone(timedelta(hours=7)))
    assert make_run_id("crawl_hot", "tiki", vn) == "crawl_hot-tiki-20261101T080000Z"


# ------------------------------ catalog ------------------------------

def test_catalog_insert_then_update(state):
    state.upsert_products("tiki", [("1", "https://tiki.vn/p/1")], seen_at=T0, is_hot=True)
    state.upsert_products("tiki", [("1", "https://tiki.vn/p/1-new")], seen_at=T0 + timedelta(days=1))

    row = state.get_product("tiki", "1")
    assert row["product_key"] == "tiki:1"
    assert row["first_seen"] == "2026-11-01T08:00:00+00:00"   # giữ lần đầu thấy
    assert row["last_seen"] == "2026-11-02T08:00:00+00:00"    # cập nhật lần cuối thấy
    assert row["url"] == "https://tiki.vn/p/1-new"
    assert row["is_hot"] == 1                                 # is_hot=None -> giữ nguyên


def test_catalog_is_hot_can_be_turned_off(state):
    state.upsert_products("tiki", [("1", None)], seen_at=T0, is_hot=True)
    state.upsert_products("tiki", [("1", None)], seen_at=T0, is_hot=False)
    assert state.get_product("tiki", "1")["is_hot"] == 0


def test_catalog_last_seen_never_goes_back(state):
    # Ghi dữ liệu cũ hơn sau (ví dụ chạy lại) không được kéo last_seen lùi lại
    state.upsert_products("tiki", [("1", None)], seen_at=T0 + timedelta(days=1))
    state.upsert_products("tiki", [("1", None)], seen_at=T0)
    assert state.get_product("tiki", "1")["last_seen"] == "2026-11-02T08:00:00+00:00"


def test_catalog_count_and_persist(tmp_path):
    with StateStore(tmp_path / "crawler.db") as s:
        s.upsert_products("tiki", [(str(i), None) for i in range(50)], seen_at=T0)
        s.upsert_products("cellphones", [(str(i), None) for i in range(20)], seen_at=T0)
    # Đóng rồi mở lại: dữ liệu vẫn còn
    with StateStore(tmp_path / "crawler.db") as s:
        assert s.count_products() == 70
        assert s.count_products("tiki") == 50


# ------------------------------ logging ------------------------------

def test_logging_writes_daily_file(tmp_path):
    setup_logging(tmp_path, level="INFO")
    logging.getLogger("test").info("hello from test")
    for h in logging.getLogger().handlers:
        h.flush()

    files = list(tmp_path.glob("crawler-*.log"))
    assert len(files) == 1
    assert "hello from test" in files[0].read_text(encoding="utf-8")


def test_logging_setup_twice_does_not_duplicate(tmp_path):
    setup_logging(tmp_path, level="INFO")
    setup_logging(tmp_path, level="INFO")
    logging.getLogger("test").info("only once")
    for h in logging.getLogger().handlers:
        h.flush()

    text = next(tmp_path.glob("crawler-*.log")).read_text(encoding="utf-8")
    assert text.count("only once") == 1
