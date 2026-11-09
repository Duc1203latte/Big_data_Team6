"""Test jobs/replay_producer.py offline: landing/ giả trong thư mục tạm + producer giả."""

import argparse
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import pytest

from common.state import StateStore
from jobs.replay_producer import (
    DLQ_TOPIC,
    Progress,
    Stats,
    TokenBucket,
    failed_files,
    landing_files,
    parse_args,
    parse_until,
    replay,
    running_run_ids,
)


class FakeProducer:
    def __init__(self, fail: bool = False) -> None:
        self.messages: list[dict] = []
        self.callbacks: list = []
        self.fail = fail

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        self.messages.append({"topic": topic, "value": value, "key": key, "headers": dict(headers or [])})
        self.callbacks.append(on_delivery)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        callbacks, self.callbacks = self.callbacks, []
        for cb in callbacks:
            cb("KafkaError{MSG_TIMED_OUT}" if self.fail else None, None)
        return 0


def event(i: int, event_time: str, platform: str = "tiki", event_type: str = "price") -> dict:
    return {"schema_version": 1, "event_id": f"id-{i}", "platform": platform, "product_id": str(100 + i),
            "event_type": event_type, "event_time": event_time, "crawler_version": "0.1.0", "price_current": 1000 + i}


def write(landing: Path, ev: dict, run_id: str = "crawl_hot-tiki-20261105T100000Z") -> Path:
    t = datetime.fromisoformat(ev["event_time"])
    path = (landing / ev["event_type"] / f"platform={ev['platform']}" / f"date={t:%Y-%m-%d}" / f"hour={t:%H}"
            / f"part-{run_id}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        f.write(orjson.dumps(ev) + b"\n")
    return path


def files_of(landing: Path, until=None, running=frozenset(), stats=None, event_types=("price", "product")):
    return landing_files(landing, list(event_types), None, None, None, until, set(running), stats or Stats())


def run(files, producer=None, progress=None, **kw):
    opts = dict(rate=0, ingest_source="replay", until=None, dry_run=producer is None, stats=Stats())
    opts.update(kw)
    return replay(files, producer, progress, **opts)


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    root = tmp_path / "landing"
    # Ghi lộn thứ tự thời gian để kiểm tra replay sắp xếp lại theo date/hour
    write(root, event(3, "2026-11-06T01:10:00Z"))
    write(root, event(1, "2026-11-05T10:05:00Z"))
    write(root, event(2, "2026-11-05T23:59:59Z", platform="tgdd"), run_id="crawl_hot-tgdd-20261105T230000Z")
    write(root, event(4, "2026-11-05T10:30:00Z", platform="hoangha", event_type="product"),
          run_id="crawl_full-hoangha-20261105T100000Z")
    return root


def test_sends_original_lines_in_time_order(landing):
    producer = FakeProducer()
    stats = run(files_of(landing), producer)
    assert stats.sent == 4 and stats.files_done == 4
    ids = [orjson.loads(m["value"])["event_id"] for m in producer.messages]
    assert ids[:2] in (["id-1", "id-4"], ["id-4", "id-1"])          # cùng giờ 10, khác loại
    assert ids[2:] == ["id-2", "id-3"]
    # Value là nguyên dòng trong file, không serialize lại
    all_lines = {line for p in landing.rglob("*.jsonl") for line in p.read_bytes().splitlines()}
    assert {m["value"] for m in producer.messages} == all_lines


def test_topic_key_and_headers(landing):
    producer = FakeProducer()
    run(files_of(landing), producer)
    by_id = {orjson.loads(m["value"])["event_id"]: m for m in producer.messages}
    assert by_id["id-1"]["topic"] == "raw.price" and by_id["id-1"]["key"] == b"tiki:101"
    assert by_id["id-4"]["topic"] == "raw.product" and by_id["id-4"]["key"] == b"hoangha:104"
    assert by_id["id-1"]["headers"] == {"event_type": b"price", "schema_version": b"1",
                                        "crawler_version": b"0.1.0", "ingest_source": b"replay"}


def test_until_skips_partitions_and_lines(landing):
    until = parse_until("2026-11-05T10:20Z")
    stats = Stats()
    files = files_of(landing, until=until, stats=stats)
    # Giờ 23 ngày 05 và ngày 06 nằm sau mốc -> bỏ cả file
    assert stats.files_skipped["after --until"] == 2
    producer = FakeProducer()
    stats = run(files, producer, until=until, stats=stats)
    # Giờ 10: id-1 (10:05) gửi, id-4 (10:30) bỏ vì sau mốc
    assert [orjson.loads(m["value"])["event_id"] for m in producer.messages] == ["id-1"]
    assert stats.skipped_until == 1


def test_files_of_running_runs_are_skipped(landing, tmp_path):
    db = tmp_path / "crawler.db"
    with StateStore(db) as state:
        running = state.start_run("crawl_hot", "tgdd")
        done = state.start_run("crawl_hot", "tiki")
        state.finish_run(done, "ok", 1, None)
    write(landing, event(9, "2026-11-05T11:00:00Z", platform="tgdd"), run_id=running)
    stats = Stats()
    files = files_of(landing, running=running_run_ids(db), stats=stats)
    assert running_run_ids(db) == {running}
    assert stats.files_skipped["run still running"] == 1
    assert all(running not in str(f.path) for f in files)


def test_checkpoint_skips_done_files_and_rejects_changed(landing, tmp_path):
    progress_path = tmp_path / "replay_progress.json"
    run(files_of(landing), FakeProducer(), Progress(progress_path))

    producer = FakeProducer()
    stats = run(files_of(landing), producer, Progress(progress_path))
    assert producer.messages == [] and stats.files_skipped["already replayed"] == 4

    # File đã nạp mà dài thêm -> báo lỗi, không tự gửi lại
    write(landing, event(5, "2026-11-05T10:50:00Z"))
    producer = FakeProducer()
    stats = run(files_of(landing), producer, Progress(progress_path))
    assert producer.messages == []
    assert any("size changed" in e for e in stats.errors)


def test_bad_lines_go_to_dlq_with_null_key(tmp_path):
    root = tmp_path / "landing"
    path = write(root, event(1, "2026-11-05T10:05:00Z"))
    with path.open("ab") as f:
        f.write(b'{"event_id": "cut\n' + orjson.dumps({"event_type": "price"}) + b"\n")
    producer = FakeProducer()
    stats = run(files_of(root), producer)
    dlq = [m for m in producer.messages if m["topic"] == DLQ_TOPIC]
    assert stats.dlq == 2 and len(dlq) == 2
    assert all(m["key"] is None for m in dlq)
    assert {m["headers"]["reason"] for m in dlq} == {b"JSONDecodeError", b"KeyError"}
    # raw.price không bao giờ nhận key null
    assert all(m["key"] for m in producer.messages if m["topic"] == "raw.price")


def test_delivery_failure_does_not_mark_file_done(landing, tmp_path):
    progress = Progress(tmp_path / "p.json")
    stats = run(files_of(landing), FakeProducer(fail=True), progress)
    assert stats.files_done == 0 and stats.failed_delivery == 4
    assert progress.done == {}
    assert len(stats.errors) == 4


def test_dry_run_counts_without_sending(landing, tmp_path):
    stats = run(files_of(landing), None, None)
    assert stats.sent == 4 and stats.files_done == 0


def test_failed_mode_uses_topic_from_file_name(tmp_path):
    failed = tmp_path / "kafka_failed"
    failed.mkdir()
    (failed / "raw.price-crawl_hot-tiki-20261109T100000Z.jsonl").write_bytes(
        orjson.dumps(event(1, "2026-11-09T10:00:00Z")) + b"\n")
    (failed / "notes.jsonl").write_text("ignored\n")
    files = failed_files(failed)
    assert [f.topic for f in files] == ["raw.price"]
    producer = FakeProducer()
    run(files, producer, ingest_source="live")
    assert producer.messages[0]["topic"] == "raw.price"
    assert producer.messages[0]["headers"]["ingest_source"] == b"live"


def test_token_bucket_limits_rate():
    now = [0.0]
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    bucket = TokenBucket(100, clock=lambda: now[0], sleep=sleep)
    for _ in range(300):
        bucket.take()
    # 100 token có sẵn (burst 1 giây), 200 cái sau phải chờ ~2 giây
    assert now[0] == pytest.approx(2.0, abs=0.05)
    TokenBucket(0).take()                       # rate 0 = không giới hạn, không chờ


def test_parse_until_requires_timezone():
    assert parse_until("2026-11-10T03:00Z") == datetime(2026, 11, 10, 3, tzinfo=timezone.utc)
    assert parse_until("2026-11-10T10:00+07:00") == datetime(2026, 11, 10, 3, tzinfo=timezone.utc)
    with pytest.raises(argparse.ArgumentTypeError):
        parse_until("2026-11-10T03:00")


def test_parse_args_defaults():
    args = parse_args(["--platform", "all", "--date-from", "2026-11-01", "--date-to", "2026-11-08"])
    assert args.platform is None and args.rate == 500 and args.ingest_source == "replay"
    assert args.date_from == date(2026, 11, 1)
    with pytest.raises(SystemExit):
        parse_args(["--date-from", "2026-11-08", "--date-to", "2026-11-01"])


def test_landing_is_not_modified(landing, tmp_path):
    before = {p: p.read_bytes() for p in landing.rglob("*") if p.is_file()}
    run(files_of(landing), FakeProducer(), Progress(tmp_path / "p.json"))
    assert before == {p: p.read_bytes() for p in landing.rglob("*") if p.is_file()}
