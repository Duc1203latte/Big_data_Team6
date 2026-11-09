"""Nạp dữ liệu cũ trong landing/ (hoặc state/kafka_failed/) vào Kafka (README-2A mục 8).

Cũng là công cụ kiểm thử tải ở tuần 5 (--rate).

- Gửi NGUYÊN dòng JSONL làm value (nguyên tắc 6): không sinh lại event_id, không đổi event_time.
  Chỉ parse dòng để lấy platform, product_id, event_type, event_time làm key, header và lọc --until.
- Đọc file theo thứ tự date, hour tăng dần để giữ trình tự thời gian.
- Chỉ nạp phần đã ghi xong:
    * bỏ dữ liệu có event_time >= --until (từ mốc đó crawler đã tự gửi vào Kafka qua MultiSink),
    * bỏ file của run còn "running" trong crawl_runs (crawler còn đang ghi tiếp vào file đó).
- Checkpoint ở state/replay_progress.json: file đã nạp xong (kích thước + số dòng) thì lần sau bỏ qua.
  File đã xong mà kích thước đổi -> báo lỗi, KHÔNG tự nạp lại.
- Dòng không đọc được (JSON hỏng, thiếu trường) -> gửi vào topic dlq.invalid với key null.
- File có message giao thất bại thì không đánh dấu xong; chạy lại sẽ gửi lại cả file
  (có thể tạo bản trùng trong Bronze, Silver bỏ trùng theo event_id).

Chạy (trong thư mục crawler/):
    python -m jobs.replay_producer --date-from 2026-11-01 --date-to 2026-11-08 \\
        --until 2026-11-10T03:00Z --rate 500 --dry-run
    python -m jobs.replay_producer --failed          # gửi lại state/kafka_failed/*.jsonl
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import orjson

from common.config import load_env_file
from common.ids import make_product_key
from common.sink import PRODUCER_CONFIG, TOPICS

logger = logging.getLogger("replay_producer")

DLQ_TOPIC = "dlq.invalid"
PROGRESS_EVERY = 10.0          # giây giữa hai dòng log tiến độ
FLUSH_TIMEOUT = 60.0           # chờ broker xác nhận ở cuối mỗi file


@dataclass(frozen=True)
class ReplayFile:
    path: Path
    topic: str | None          # None = lấy topic theo event_type của từng dòng (landing/)
    partition_start: datetime | None = None   # đầu giờ của partition hour=, None với kafka_failed


@dataclass
class Stats:
    sent: int = 0
    skipped_until: int = 0
    dlq: int = 0
    failed_delivery: int = 0
    files_done: int = 0
    files_skipped: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)


class TokenBucket:
    """Giới hạn tốc độ gửi: tối đa rate event/giây, cho phép dồn tối đa 1 giây (burst = rate)."""

    def __init__(self, rate: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.rate = rate
        self.clock, self.sleep = clock, sleep
        self.tokens = rate
        self.last = clock()

    def take(self) -> None:
        if self.rate <= 0:         # 0 = nhanh nhất có thể
            return
        while True:
            now = self.clock()
            self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
            self.last = now
            # So với 1 - 1e-9 chứ không phải 1: sau khi chờ, tokens có thể ra 0.9999999999 do sai số
            # số thực; khi đó khoảng chờ thêm quá nhỏ, đồng hồ không nhích, vòng lặp sẽ không bao giờ dừng
            if self.tokens >= 1 - 1e-9:
                self.tokens = max(self.tokens - 1, 0.0)
                return
            self.sleep((1 - self.tokens) / self.rate)


# ---------------------------------------------------------------------------
# Chọn file
# ---------------------------------------------------------------------------

def partition_value(path: Path, name: str) -> str | None:
    for part in path.parts:
        if part.startswith(f"{name}="):
            return part.split("=", 1)[1]
    return None


def run_id_of(path: Path) -> str:
    """part-<run_id>.jsonl -> run_id"""
    return path.stem.removeprefix("part-")


def running_run_ids(state_db: Path) -> set[str]:
    """run_id của các run crawler còn đang chạy (đọc SQLite ở chế độ chỉ đọc)."""
    if not state_db.exists():
        return set()
    con = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        return {row[0] for row in con.execute("SELECT run_id FROM crawl_runs WHERE status = 'running'")}
    finally:
        con.close()


def landing_files(landing_dir: Path, event_types: list[str], platforms: list[str] | None,
                  date_from: date | None, date_to: date | None, until: datetime | None,
                  running: set[str], stats: Stats) -> list[ReplayFile]:
    """Các file landing/ cần nạp, sắp theo (date, hour) tăng dần."""
    files = []
    for event_type in event_types:
        for path in landing_dir.glob(f"{event_type}/platform=*/date=*/hour=*/*.jsonl"):
            platform = partition_value(path, "platform")
            if platforms and platform not in platforms:
                continue
            try:
                day = date.fromisoformat(partition_value(path, "date"))
                start = datetime(day.year, day.month, day.day, int(partition_value(path, "hour")),
                                 tzinfo=timezone.utc)
            except (TypeError, ValueError):
                stats.errors.append(f"bad partition path: {path}")
                continue
            if (date_from and day < date_from) or (date_to and day > date_to):
                continue
            if until and start >= until:
                stats.files_skipped["after --until"] += 1
                continue
            if run_id_of(path) in running:
                stats.files_skipped["run still running"] += 1
                continue
            files.append(ReplayFile(path, None, start))
    files.sort(key=lambda f: (f.partition_start, str(f.path)))
    return files


def failed_files(failed_dir: Path) -> list[ReplayFile]:
    """state/kafka_failed/<topic>-<run_id>.jsonl, topic lấy từ tên file."""
    files = []
    for path in sorted(failed_dir.glob("*.jsonl")):
        topic = next((t for t in TOPICS.values() if path.name.startswith(f"{t}-")), None)
        if topic:
            files.append(ReplayFile(path, topic))
    return files


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

class Progress:
    """state/replay_progress.json: {đường dẫn: {"size", "lines", "done_at"}}."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.done: dict[str, dict] = json.loads(path.read_text()) if path.exists() else {}

    def status(self, file: Path) -> str:
        """'new' | 'done' | 'changed'"""
        entry = self.done.get(str(file))
        if entry is None:
            return "new"
        return "done" if entry["size"] == file.stat().st_size else "changed"

    def mark_done(self, file: Path, size: int, lines: int) -> None:
        self.done[str(file)] = {"size": size, "lines": lines,
                                "done_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        # Ghi ra file tạm rồi đổi tên: máy tắt giữa chừng không làm hỏng checkpoint
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.done, indent=1, sort_keys=True))
        os.replace(tmp, self.path)


# ---------------------------------------------------------------------------
# Gửi
# ---------------------------------------------------------------------------

def read_lines(path: Path, size: int) -> Iterator[bytes]:
    """Các dòng của file, chỉ trong size byte đầu (phần đã có lúc bắt đầu đọc)."""
    with path.open("rb") as f:
        data = f.read(size)
    for line in data.split(b"\n"):
        if line.strip():
            yield line.rstrip(b"\r")


def message_for(line: bytes, ingest_source: str) -> tuple[str | None, bytes | None, list, str | None, str]:
    """Một dòng -> (topic, key, headers, event_time, lỗi). Topic None + lỗi nếu dòng hỏng."""
    try:
        event = orjson.loads(line)
        event_type = event["event_type"]
        topic = TOPICS[event_type]
        key = make_product_key(event["platform"], event["product_id"]).encode()
        headers = [
            ("event_type", event_type.encode()),
            ("schema_version", str(event.get("schema_version", "")).encode()),
            ("crawler_version", str(event.get("crawler_version", "")).encode()),
            ("ingest_source", ingest_source.encode()),
        ]
        return topic, key, headers, event.get("event_time"), ""
    except (ValueError, KeyError, TypeError) as exc:
        return None, None, [], None, type(exc).__name__


def make_producer(bootstrap_servers: str) -> Any:
    from confluent_kafka import Producer
    return Producer({**PRODUCER_CONFIG, "bootstrap.servers": bootstrap_servers, "client.id": "replay-producer",
                     "logger": logging.getLogger("librdkafka")})


def replay(files: list[ReplayFile], producer: Any, progress: Progress | None, *, rate: float,
           ingest_source: str, until: datetime | None, dry_run: bool, stats: Stats,
           bucket: TokenBucket | None = None, clock: Callable[[], float] = time.monotonic) -> Stats:
    """Gửi lần lượt từng file. producer=None khi dry_run."""
    bucket = bucket or TokenBucket(rate)
    until_text = until.strftime("%Y-%m-%dT%H:%M:%SZ") if until else None
    total_lines = 0
    todo: list[tuple[ReplayFile, int]] = []
    for f in files:
        state = progress.status(f.path) if progress else "new"
        if state == "done":
            stats.files_skipped["already replayed"] += 1
            continue
        if state == "changed":
            stats.errors.append(f"{f.path}: size changed since it was replayed, not sending again")
            continue
        size = f.path.stat().st_size
        todo.append((f, size))
        total_lines += sum(1 for _ in read_lines(f.path, size))

    logger.info("%d file(s), %d line(s) to %s; skipped files: %s", len(todo), total_lines,
                "count (dry run)" if dry_run else "send", dict(stats.files_skipped) or 0)
    started = last_log = clock()
    done_lines = 0

    for f, size in todo:
        file_errors = Counter()

        def on_delivery(err, msg, file_errors=file_errors):
            if err is not None:
                file_errors[str(err)] += 1

        lines = 0
        for line in read_lines(f.path, size):
            lines += 1
            done_lines += 1
            topic, key, headers, event_time, problem = message_for(line, ingest_source)
            if topic is not None and f.topic is not None:
                topic = f.topic
            # event_time dạng "2026-11-06T10:45:54Z" nên so chuỗi là đúng thứ tự thời gian
            if until_text and isinstance(event_time, str) and event_time >= until_text:
                stats.skipped_until += 1
                continue
            if topic is None:
                stats.dlq += 1
                topic, key, headers = DLQ_TOPIC, None, [("reason", problem.encode()),
                                                         ("source_file", str(f.path).encode())]
            if dry_run:
                stats.sent += 1
                continue
            bucket.take()
            while True:
                try:
                    producer.produce(topic, value=line, key=key, headers=headers, on_delivery=on_delivery)
                    break
                except BufferError:          # hàng đợi nội bộ đầy: đợi broker nhận bớt
                    producer.poll(0.5)
            producer.poll(0)
            stats.sent += 1

            now = clock()
            if now - last_log >= PROGRESS_EVERY:
                elapsed = now - started
                logger.info("Progress: %d/%d lines (%.1f%%), %.0f events/s, %d left", done_lines, total_lines,
                            100 * done_lines / max(total_lines, 1), stats.sent / max(elapsed, 1e-9),
                            total_lines - done_lines)
                last_log = now

        if dry_run:
            continue
        remaining = producer.flush(FLUSH_TIMEOUT)
        if remaining or file_errors:
            stats.failed_delivery += remaining + sum(file_errors.values())
            stats.errors.append(f"{f.path}: {remaining} unconfirmed, delivery errors {dict(file_errors)}")
            continue                         # không đánh dấu xong: chạy lại sẽ gửi lại file này
        if progress:
            progress.mark_done(f.path, size, lines)
        stats.files_done += 1

    elapsed = clock() - started
    logger.info("Done: %d sent (%.0f events/s), %d after --until, %d to %s, %d files done, %d failed delivery",
                stats.sent, stats.sent / max(elapsed, 1e-9), stats.skipped_until, stats.dlq, DLQ_TOPIC,
                stats.files_done, stats.failed_delivery)
    for error in stats.errors:
        logger.error("%s", error)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_until(value: str) -> datetime:
    """'2026-11-10T03:00Z' -> datetime UTC. Bắt buộc ghi rõ múi giờ để không nhầm giờ VN."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise argparse.ArgumentTypeError("--until needs a timezone, e.g. 2026-11-10T03:00Z")
    return dt.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay landing/ JSONL (or state/kafka_failed/) into Kafka")
    parser.add_argument("--event-type", choices=["price", "product", "all"], default="all")
    parser.add_argument("--platform", action="append", help="repeatable; default: all platforms")
    parser.add_argument("--date-from", type=date.fromisoformat)
    parser.add_argument("--date-to", type=date.fromisoformat)
    parser.add_argument("--until", type=parse_until, help="skip events at/after this UTC time")
    parser.add_argument("--rate", type=float, default=500, help="events per second, 0 = unlimited")
    parser.add_argument("--ingest-source", default="replay", choices=["replay", "live"])
    parser.add_argument("--dry-run", action="store_true", help="only count, do not send")
    parser.add_argument("--failed", action="store_true", help="resend state/kafka_failed/*.jsonl instead")
    parser.add_argument("--landing", type=Path, default=None)
    parser.add_argument("--state-db", type=Path, default=None)
    parser.add_argument("--progress-file", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.platform and "all" in args.platform:
        args.platform = None
    if args.date_from and args.date_to and args.date_to < args.date_from:
        parser.error("--date-to is before --date-from")
    return args


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    load_env_file(".env")          # chỉ lấy đường dẫn và địa chỉ Kafka, không cần CONTACT_EMAIL
    landing_dir = args.landing or Path(os.getenv("LANDING_DIR", "./landing"))
    state_db = args.state_db or Path(os.getenv("STATE_DB", "./state/crawler.db"))
    progress_file = args.progress_file or state_db.parent / "replay_progress.json"
    stats = Stats()

    if args.failed:
        files = failed_files(state_db.parent / "kafka_failed")
    else:
        event_types = ["price", "product"] if args.event_type == "all" else [args.event_type]
        files = landing_files(landing_dir, event_types, args.platform, args.date_from, args.date_to,
                              args.until, running_run_ids(state_db), stats)
        if args.until is None:
            logger.warning("No --until given: events crawled after the switch to SINK=file,kafka will be duplicated")

    producer = None if args.dry_run else make_producer(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    progress = None if args.dry_run else Progress(progress_file)
    replay(files, producer, progress, rate=args.rate, ingest_source=args.ingest_source, until=args.until,
           dry_run=args.dry_run, stats=stats)
    return 1 if stats.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
