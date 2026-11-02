"""Test jobs/status.py: đếm event theo partition và tìm khoảng trống (offline)."""

from datetime import datetime, timedelta, timezone

import orjson

from jobs.status import collect_event_stats, find_gaps, partition_time

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def write(landing, event_type, platform, hour, events):
    path = landing / event_type / f"platform={platform}" / "date=2026-11-02" / f"hour={hour:02d}" / "part-x.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(orjson.dumps(e) + b"\n" for e in events))


def test_partition_time(tmp_path):
    p = tmp_path / "price/platform=tiki/date=2026-11-02/hour=08/part-x.jsonl"
    assert partition_time(p) == datetime(2026, 11, 2, 8, tzinfo=timezone.utc)


def test_collect_event_stats(tmp_path):
    write(tmp_path, "price", "tiki", 10, [{"price_current": 1, "name_raw": "a"}, {"price_current": None, "name_raw": "b"}])
    write(tmp_path, "price", "tiki", 3, [{"price_current": 1, "name_raw": "old"}])          # trước since: bỏ qua
    write(tmp_path, "product", "tiki", 11, [{"name_raw": "p"}])
    stats, by_hour = collect_event_stats(tmp_path, since=NOW - timedelta(hours=6))
    s = stats["tiki"]
    assert (s.events, s.price_events, s.price_missing, s.name_missing) == (3, 2, 1, 0)
    assert sorted(h.hour for h in by_hour["tiki"]) == [10, 11]


def run(platform, start, job="crawl_hot"):
    return {"job": job, "platform": platform, "started_at": start.isoformat()}


def test_find_gaps():
    base = NOW - timedelta(hours=5)
    runs = [run("tiki", base + timedelta(minutes=30 * i)) for i in range(4)]       # đều 30 phút tới base+1h30
    runs.append(run("tiki", base + timedelta(hours=4)))                            # máy tắt 2,5 giờ
    runs.append(run("cellphones", NOW - timedelta(minutes=50)))                    # chu kỳ 60: chưa trễ
    runs.append(run("hoangha", NOW - timedelta(hours=3), job="crawl_full"))        # crawl_full không tính
    gaps = find_gaps(runs, {"tiki": 30, "cellphones": 60}, since=NOW - timedelta(hours=6), now=NOW)
    assert [(g[0], g[1], g[2]) for g in gaps] == [("tiki", base + timedelta(hours=1, minutes=30), base + timedelta(hours=4))]


def test_no_gap_when_running_on_schedule():
    runs = [run("tiki", NOW - timedelta(minutes=30 * i)) for i in range(5)]
    assert find_gaps(runs, {"tiki": 30}, since=NOW - timedelta(hours=3), now=NOW) == []
