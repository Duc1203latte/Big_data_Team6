'''Nơi ghi event ra ngoài (sink): FileSink (giai đoạn 1), KafkaSink và MultiSink (giai đoạn 2A)'''

import logging
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import orjson

from common.ids import make_product_key
from common.schema import Event

logger = logging.getLogger(__name__)


def serialize_event(event: Event) -> bytes:
    '''Một event -> một dòng JSON (bytes, KHÔNG có "\\n" cuối).

    FileSink và KafkaSink dùng chung hàm này nên dòng trong landing/ và message Kafka
    giống nhau từng byte (README-2A nguyên tắc 6): replay gửi lại dòng cũ sẽ ra đúng message.
    '''
    # mode="json": datetime -> chuỗi ISO 8601, sau đó orjson encode ra bytes
    return orjson.dumps(event.model_dump(mode="json"))

class EventSink(ABC):
    '''Interface của mọi sinh'''
    @abstractmethod
    def write(self, events: Sequence[Event]) -> None:
        '''Ghi một lô event. Khi hàm trả về, dữ liệu phải nằm an toàn ở đích'''

    @abstractmethod
    def close(self) -> None:
        '''Giải phóng tài nguyên. Gọi nhiều lần không lỗi'''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

class FileSink(EventSink):
    """Ghi event ra JSONL theo partition (README mục 7):

        landing/<event_type>/platform=<sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-<run_id>.jsonl

    - date/hour lấy từ event_time (đã là UTC), nên file tự "rotate" theo giờ.
    - Chỉ mở file ở chế độ append ("ab"): không bao giờ ghi đè hay xoá (append-only).
    - Mỗi lần write() mở file, ghi cả lô, đóng file ngay. Crawl chỉ ~0,2 request/giây
      nên chi phí mở file không đáng kể, đổi lại dữ liệu xuống đĩa ngay sau mỗi lô
      và không phải quản lý file handle khi crawler chạy nhiều ngày.
    """

    def __init__(self, landing_dir: str | Path, run_id: str) -> None:
        #run_id nằm trong tên file nên không được chứa dấu /
        if not run_id or "/" in run_id:
            raise ValueError(f"invalid run_id: {run_id!r}")
        self.landing_dir = Path(landing_dir)
        self.run_id = run_id
        self.events_written = 0 # dùng cho crawl_runs.events_count
        self._closed = False

    def partition_path(self, event: Event) -> Path:
        '''Đường dẫn file JSONL mà event này phải được ghi vào'''
        ts = event.event_time #đã chuẩn hóa UTC trong schema
        return (
            self.landing_dir
            / event.event_type
            / f"platform={event.platform}"
            / f"date={ts:%Y-%m-%d}"
            / f"hour={ts:%H}"
            / f"part-{self.run_id}.jsonl"
        )

    def write(self, events: Sequence[Event]) -> None:
        if self._closed:
            raise RuntimeError("FileSink is cloded")
        if not events:
            return
        
        #B1: serialize toàn bộ 16 bước, gom theo file đích
        #Nếu 1 event lỗi serialize thì chưa có gì bị ghi ra đĩa
        lines_by_path: dict[Path, list[bytes]] = defaultdict(list)
        for event in events:
            line = serialize_event(event) + b"\n"
            lines_by_path[self.partition_path(event)].append(line)
        #B2: ghi từng file, 1 file 1 lần write duy nhất
        for path, lines in lines_by_path.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as f:
                f.write(b"".join(lines))
        self.events_written += len(events)

    def close(self) -> None:
        self._closed = True


# ---------------------------------------------------------------------------
# Giai đoạn 2A: Kafka (README-2A mục 6, 7)
# ---------------------------------------------------------------------------

# Topic theo event_type (README-2A mục 6.1)
TOPICS = {"price": "raw.price", "product": "raw.product"}

# Cấu hình producer bắt buộc (README-2A mục 6.3): idempotence + acks=all để không mất, không trùng
# khi librdkafka tự gửi lại.
PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "snappy",
    "linger.ms": 50,
    "batch.size": 65536,
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 120000,
}


class KafkaSink(EventSink):
    """Đẩy event vào Kafka. Topic suy ra từ event_type: price -> raw.price, product -> raw.product.

    Message (README-2A mục 6.2):
        key     = product_key ("tiki:278628866"), không bao giờ null
        value   = serialize_event(event), giống từng byte dòng JSONL của FileSink
        headers = event_type, schema_version, crawler_version, ingest_source (live|replay)

    - write(events): produce cả lô rồi flush(flush_timeout). Khi write() trả về, mọi message của lô
      hoặc đã được broker xác nhận, hoặc đã nằm trong state/kafka_failed/ chờ gửi lại.
    - Message giao thất bại (callback báo lỗi, hết flush_timeout, hàng đợi đầy): log ERROR, ghi dòng
      JSONL ra <failed_dir>/<topic>-<run_id>.jsonl để replay_producer gửi lại sau. KHÔNG raise:
      Kafka hỏng không được làm chết vòng crawl (landing/ vẫn ghi song song qua MultiSink).
    - Kafka không trả lời trong flush_timeout thì sink chuyển sang chế độ "hỏng" tới hết run:
      các lô sau ghi thẳng ra failed_dir, không chờ nữa. Nhờ vậy Kafka chết chỉ làm chậm mỗi run
      tối đa flush_timeout giây, không phải flush_timeout giây cho từng trang. Run sau thử lại từ đầu.
    - close(): flush lần cuối, đếm số message thành công / thất bại theo topic và log ra.

    Message bị coi là thất bại nhưng thật ra broker đã nhận (mất gói xác nhận) sẽ thành bản trùng khi gửi lại.
    Chấp nhận được: Bronze cho phép trùng, Silver bỏ trùng theo event_id.
    """

    def __init__(self, run_id: str, *, bootstrap_servers: str = "localhost:9092",
                 failed_dir: str | Path = "state/kafka_failed", ingest_source: str = "live",
                 flush_timeout: float = 5.0, close_timeout: float = 10.0,
                 topics: dict[str, str] | None = None, producer: Any = None) -> None:
        # run_id nằm trong tên file failed nên không được chứa dấu /
        if not run_id or "/" in run_id:
            raise ValueError(f"invalid run_id: {run_id!r}")
        self.run_id = run_id
        self.failed_dir = Path(failed_dir)
        self.ingest_source = ingest_source
        self.flush_timeout = flush_timeout
        self.close_timeout = close_timeout
        self.topics = topics or TOPICS
        if producer is None:
            # Import ở đây: máy chỉ dùng SINK=file không cần cài confluent-kafka
            from confluent_kafka import Producer
            producer = Producer({
                **PRODUCER_CONFIG,
                "bootstrap.servers": bootstrap_servers,
                "client.id": f"crawler-{run_id}",
                # Log của librdkafka (mất kết nối, broker không trả lời...) đi vào log chung của crawler
                "logger": logging.getLogger("librdkafka"),
            })
        self.producer = producer
        self.delivered: Counter[str] = Counter()     # số message broker đã xác nhận, theo topic
        self.failed: Counter[str] = Counter()        # số message đã ghi ra failed_dir, theo topic
        self.broken = False
        self._delivery_errors: Counter[str] = Counter()   # lỗi giao hàng chưa log, gom để log 1 dòng
        self._pending: dict[int, tuple[str, bytes]] = {}   # token -> (topic, value) chờ xác nhận
        self._next_token = 0
        self._closed = False

    def _headers(self, event: Event) -> list[tuple[str, bytes]]:
        return [
            ("event_type", event.event_type.encode()),
            ("schema_version", str(event.schema_version).encode()),
            ("crawler_version", event.crawler_version.encode()),
            ("ingest_source", self.ingest_source.encode()),
        ]

    def _on_delivery(self, token: int, err, msg) -> None:
        """Callback của librdkafka, chạy bên trong flush()/poll() của chính tiến trình này."""
        pending = self._pending.pop(token, None)
        if pending is None:
            # Message này đã bị coi là thất bại (hết flush_timeout) và đã nằm trong failed_dir
            return
        topic, value = pending
        if err is None:
            self.delivered[topic] += 1
        else:
            self._delivery_errors[f"{topic}: {err}"] += 1
            self._save_failed(topic, [value])

    def _log_delivery_errors(self) -> None:
        """Một dòng ERROR cho cả lô thay vì một dòng cho mỗi message lỗi."""
        if self._delivery_errors:
            logger.error("Kafka delivery failed, saved to %s: %s", self.failed_dir, dict(self._delivery_errors))
            self._delivery_errors.clear()

    def _save_failed(self, topic: str, values: list[bytes]) -> None:
        """Ghi message chưa giao được ra <failed_dir>/<topic>-<run_id>.jsonl (append) để gửi lại sau."""
        if not values:
            return
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        path = self.failed_dir / f"{topic}-{self.run_id}.jsonl"
        with path.open("ab") as f:
            f.write(b"".join(v + b"\n" for v in values))
        self.failed[topic] += len(values)

    def _abandon_pending(self) -> None:
        """Mọi message còn chờ xác nhận -> ghi ra failed_dir, không chờ nữa."""
        by_topic: dict[str, list[bytes]] = defaultdict(list)
        for topic, value in self._pending.values():
            by_topic[topic].append(value)
        self._pending.clear()
        for topic, values in by_topic.items():
            self._save_failed(topic, values)

    def _drop_producer(self) -> None:
        """Bỏ producer khi Kafka hỏng: xoá message kẹt (đã có trong failed_dir) rồi giải phóng, để
        librdkafka thôi kết nối lại và ghi log lỗi liên tục tới hết run. Run sau tạo producer mới."""
        if self.producer is None:
            return
        if self.broken and hasattr(self.producer, "purge"):
            self.producer.purge()
            self.producer.flush(0)       # chạy callback của message bị xoá (báo lỗi PURGE, đã bỏ qua)
        self.producer = None

    def write(self, events: Sequence[Event]) -> None:
        if self._closed:
            raise RuntimeError("KafkaSink is closed")
        if not events:
            return

        # Serialize trước, một event lỗi thì cả lô chưa được gửi (giống FileSink)
        messages = []
        for event in events:
            topic = self.topics[event.event_type]
            key = make_product_key(event.platform, event.product_id).encode()
            messages.append((topic, key, serialize_event(event), self._headers(event)))

        if self.broken:
            # Kafka đã không trả lời trong run này: ghi thẳng ra failed_dir, không làm chậm crawl
            for topic, _, value, _ in messages:
                self._save_failed(topic, [value])
            return

        for topic, key, value, headers in messages:
            token = self._next_token
            self._next_token += 1
            self._pending[token] = (topic, value)
            try:
                self.producer.produce(topic, value=value, key=key, headers=headers,
                                      on_delivery=partial(self._on_delivery, token))
            except Exception as exc:     # BufferError (hàng đợi đầy), KafkaException...
                self._pending.pop(token, None)
                logger.error("Kafka produce failed (%s): %r", topic, exc)
                self._save_failed(topic, [value])

        remaining = self.producer.flush(self.flush_timeout)
        self._log_delivery_errors()
        if self._pending:
            # Hết flush_timeout mà broker chưa xác nhận hết: coi như Kafka đang hỏng
            logger.error("Kafka did not confirm %d message(s) within %.0fs; writing them to %s "
                         "and skipping Kafka for the rest of run %s",
                         len(self._pending), self.flush_timeout, self.failed_dir, self.run_id)
            self._abandon_pending()
            self.broken = True
            self._drop_producer()
        elif remaining:
            logger.warning("Kafka flush returned %d queued message(s) not tracked by this sink", remaining)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pending and self.producer is not None:
            self.producer.flush(self.close_timeout)
            self._log_delivery_errors()
            self._abandon_pending()
        self._drop_producer()
        total_failed = sum(self.failed.values())
        log = logger.warning if total_failed else logger.info
        log("KafkaSink %s: delivered %s, failed %s", self.run_id, dict(self.delivered) or 0, dict(self.failed) or 0)


class MultiSink(EventSink):
    """Ghi vào nhiều sink theo thứ tự (README-2A nguyên tắc 5: SINK=file,kafka ghi song song).

    Một sink lỗi không được làm sink khác dừng: bắt lỗi từng sink, log ERROR, ghi tiếp sink sau.
    Chỉ khi MỌI sink đều lỗi mới raise, để vòng crawl biết là lô này không được ghi ở đâu cả.
    """

    def __init__(self, sinks: Sequence[EventSink]) -> None:
        if not sinks:
            raise ValueError("MultiSink needs at least one sink")
        self.sinks = list(sinks)

    def write(self, events: Sequence[Event]) -> None:
        errors = []
        for sink in self.sinks:
            try:
                sink.write(events)
            except Exception as exc:
                logger.exception("Sink %s failed to write %d event(s)", type(sink).__name__, len(events))
                errors.append(exc)
        if len(errors) == len(self.sinks):
            raise errors[-1]

    def close(self) -> None:
        for sink in self.sinks:
            try:
                sink.close()
            except Exception:
                logger.exception("Sink %s failed to close", type(sink).__name__)


# ---------------------------------------------------------------------------
# Chọn sink qua config (biến SINK trong .env)
# ---------------------------------------------------------------------------

# Các loại sink chọn được. Thêm loại mới chỉ sửa ở đây; jobs/ và platforms/ không phải sửa gì.
SINK_KINDS = ("file", "kafka")


def parse_sink_kinds(value: str) -> tuple[str, ...]:
    """'file, Kafka' -> ('file', 'kafka'). Sai tên hoặc lặp tên thì ValueError."""
    kinds = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    if not kinds:
        raise ValueError("empty sink list")
    unknown = [k for k in kinds if k not in SINK_KINDS]
    if unknown:
        raise ValueError(f"unknown sink {unknown[0]!r}, supported: {', '.join(SINK_KINDS)}")
    if len(set(kinds)) != len(kinds):
        raise ValueError(f"duplicate sink in {value!r}")
    return kinds


@dataclass(frozen=True)
class SinkConfig:
    """Cấu hình sink của một job, dựng từ Settings (common/config.py)."""

    kinds: tuple[str, ...] = ("file",)
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_failed_dir: Path = Path("state/kafka_failed")


def make_sink(config: str | SinkConfig, run_id: str, *, landing_dir: str | Path) -> EventSink:
    """Tạo sink cho một run theo cấu hình SINK (README tiêu chí 9: đổi sink chỉ cần sửa config).

    - "file":  FileSink ghi JSONL vào landing_dir (giai đoạn 1).
    - "kafka": KafkaSink đẩy vào Kafka (giai đoạn 2A).
    - nhiều loại ("file,kafka"): MultiSink ghi lần lượt theo đúng thứ tự trong SINK.
    config là chuỗi ("file", "file,kafka") hoặc SinkConfig đầy đủ (có địa chỉ Kafka).
    """
    if isinstance(config, str):
        config = SinkConfig(kinds=parse_sink_kinds(config))
    sinks: list[EventSink] = []
    for kind in config.kinds:
        if kind == "file":
            sinks.append(FileSink(landing_dir, run_id))
        elif kind == "kafka":
            sinks.append(KafkaSink(run_id, bootstrap_servers=config.kafka_bootstrap_servers,
                                   failed_dir=config.kafka_failed_dir))
        else:
            raise ValueError(f"unknown sink {kind!r}, supported: {', '.join(SINK_KINDS)}")
    return sinks[0] if len(sinks) == 1 else MultiSink(sinks)
