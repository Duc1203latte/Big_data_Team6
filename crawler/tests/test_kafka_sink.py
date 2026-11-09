"""Test KafkaSink, MultiSink và make_sink nhiều loại (README-2A mục 7).

Phần lớn chạy offline với producer giả. Test đánh dấu @pytest.mark.kafka cần broker thật ở
localhost:9092 (docker compose --profile kafka up -d), không có thì tự bỏ qua. Test với broker thật
dùng topic tạm riêng rồi xoá, KHÔNG ghi vào raw.price / raw.product.
"""

import logging
import socket
import time
import uuid
from datetime import datetime, timezone

import pytest

from common.schema import PriceEvent, ProductEvent
from common.sink import (
    PRODUCER_CONFIG,
    FileSink,
    KafkaSink,
    MultiSink,
    SinkConfig,
    make_sink,
    serialize_event,
)

BASE_TIME = datetime(2026, 11, 9, 10, 0, 5, tzinfo=timezone.utc)


def make_price(i: int, platform: str = "tiki") -> PriceEvent:
    return PriceEvent(
        platform=platform, product_id=str(1000 + i), sku_id=f"s{i}", url=f"https://example.com/p/{i}",
        name_raw=f"Sản phẩm {i}", source="listing", price_current=100_000 + i,
        price_current_raw=f"{100_000 + i}đ", event_time=BASE_TIME, crawler_version="0.1.0", raw_hash="abc",
    )


def make_product(i: int) -> ProductEvent:
    return ProductEvent(
        platform="hoangha", product_id=f"p-{i}", url=f"https://example.com/p/{i}", name_raw=f"P{i}",
        specs_raw={"RAM": "8GB"}, event_time=BASE_TIME, crawler_version="0.1.0", raw_hash="abc",
    )


class FakeError:
    """Giống KafkaError: chỉ cần in ra được."""

    def __str__(self) -> str:
        return "KafkaError{code=MSG_TIMED_OUT}"


class FakeProducer:
    """Producer giả, hành vi theo mode:
    ok    -> flush() gọi callback thành công cho mọi message
    error -> flush() gọi callback lỗi
    hang  -> flush() không gọi callback nào (broker không trả lời), trả về số message còn kẹt
    raise -> produce() raise BufferError (hàng đợi đầy)
    """

    def __init__(self, mode: str = "ok") -> None:
        self.mode = mode
        self.produced: list[dict] = []
        self.queue: list = []
        self.flush_calls: list[float] = []
        self.purged = False

    def produce(self, topic, value=None, key=None, headers=None, on_delivery=None):
        if self.mode == "raise":
            raise BufferError("Local: Queue full")
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        self.queue.append(on_delivery)

    def flush(self, timeout=None):
        self.flush_calls.append(timeout)
        if self.mode == "hang":
            return len(self.queue)
        callbacks, self.queue = self.queue, []
        for cb in callbacks:
            cb(FakeError() if self.mode == "error" else None, None)
        return 0

    def purge(self):
        self.purged = True
        self.queue = []


def kafka_sink(tmp_path, producer, **kwargs) -> KafkaSink:
    return KafkaSink("run-1", failed_dir=tmp_path / "kafka_failed", producer=producer, **kwargs)


def failed_lines(tmp_path, topic="raw.price"):
    path = tmp_path / "kafka_failed" / f"{topic}-run-1.jsonl"
    return path.read_bytes().splitlines() if path.exists() else []


# ------------------------------ message ------------------------------

def test_topic_key_headers_and_payload(tmp_path):
    producer = FakeProducer()
    events = [make_price(1), make_product(2)]
    with kafka_sink(tmp_path, producer) as sink:
        sink.write(events)

    price, product = producer.produced
    assert price["topic"] == "raw.price" and product["topic"] == "raw.product"
    assert price["key"] == b"tiki:1001"            # product_key, không bao giờ null
    assert product["key"] == b"hoangha:p-2"
    assert price["value"] == serialize_event(events[0])
    assert dict(price["headers"]) == {
        "event_type": b"price", "schema_version": b"1", "crawler_version": b"0.1.0", "ingest_source": b"live",
    }
    assert sink.delivered == {"raw.price": 1, "raw.product": 1}
    assert not sink.failed


def test_payload_is_byte_identical_to_landing_line(tmp_path):
    # Nguyên tắc 6: message Kafka = đúng dòng JSONL của FileSink (bỏ "\n")
    events = [make_price(i) for i in range(5)]
    with FileSink(tmp_path / "landing", "run-1") as fsink:
        fsink.write(events)
    producer = FakeProducer()
    with kafka_sink(tmp_path, producer) as ksink:
        ksink.write(events)
    landing_lines = next((tmp_path / "landing").rglob("*.jsonl")).read_bytes().splitlines()
    assert landing_lines == [m["value"] for m in producer.produced]


def test_replay_ingest_source_header(tmp_path):
    producer = FakeProducer()
    with kafka_sink(tmp_path, producer, ingest_source="replay") as sink:
        sink.write([make_price(1)])
    assert dict(producer.produced[0]["headers"])["ingest_source"] == b"replay"


def test_producer_config_matches_spec():
    assert PRODUCER_CONFIG["enable.idempotence"] is True
    assert PRODUCER_CONFIG["acks"] == "all"


def test_flush_after_every_write(tmp_path):
    producer = FakeProducer()
    with kafka_sink(tmp_path, producer, flush_timeout=3) as sink:
        sink.write([make_price(1)])
        sink.write([make_price(2)])
        sink.write([])                               # lô rỗng: không gửi, không flush
    assert producer.flush_calls == [3, 3]


# ------------------------------ lỗi giao hàng ------------------------------

def test_delivery_error_saved_to_failed_dir(tmp_path, caplog):
    events = [make_price(i) for i in range(3)]
    with kafka_sink(tmp_path, FakeProducer("error")) as sink:
        with caplog.at_level(logging.ERROR, logger="common.sink"):
            sink.write(events)                       # không raise
    assert failed_lines(tmp_path) == [serialize_event(e) for e in events]
    assert sink.failed == {"raw.price": 3} and not sink.delivered
    # 3 message lỗi -> 1 dòng ERROR, không phải 3
    assert len([r for r in caplog.records if "delivery failed" in r.message]) == 1


def test_produce_exception_saved_to_failed_dir(tmp_path):
    with kafka_sink(tmp_path, FakeProducer("raise")) as sink:
        sink.write([make_price(1), make_price(2)])
    assert len(failed_lines(tmp_path)) == 2
    assert sink.failed == {"raw.price": 2}


def test_broker_not_answering_marks_sink_broken(tmp_path):
    producer = FakeProducer("hang")
    sink = kafka_sink(tmp_path, producer, flush_timeout=0.1)
    sink.write([make_price(1), make_price(2)])
    assert sink.broken
    assert len(failed_lines(tmp_path)) == 2

    # Lô sau: không gửi Kafka nữa, ghi thẳng ra failed_dir, không flush (không làm chậm crawl)
    sink.write([make_price(3)])
    assert len(producer.produced) == 2
    assert producer.flush_calls == [0.1, 0]      # flush của lô 1, rồi flush(0) sau khi xoá hàng đợi
    assert len(failed_lines(tmp_path)) == 3

    # Producer bị bỏ ngay khi Kafka hỏng: thôi kết nối lại, thôi ghi log lỗi tới hết run
    assert producer.purged and sink.producer is None
    sink.close()
    assert sink.failed == {"raw.price": 3}


def test_purged_message_callback_is_ignored(tmp_path):
    # Sau khi Kafka bị coi là hỏng, callback muộn (ví dụ báo lỗi PURGE) không được ghi trùng ra failed_dir
    producer = FakeProducer("hang")
    sink = kafka_sink(tmp_path, producer, flush_timeout=0.1)
    sink.write([make_price(1)])                  # message token 0 bị coi là thất bại
    sink._on_delivery(0, FakeError(), None)      # librdkafka báo lỗi muộn cho chính message đó
    sink._on_delivery(0, None, None)
    assert len(failed_lines(tmp_path)) == 1
    assert not sink.delivered


def test_failed_file_per_topic(tmp_path):
    with kafka_sink(tmp_path, FakeProducer("error")) as sink:
        sink.write([make_price(1), make_product(2)])
    assert len(failed_lines(tmp_path, "raw.price")) == 1
    assert len(failed_lines(tmp_path, "raw.product")) == 1


def test_write_after_close_raises(tmp_path):
    sink = kafka_sink(tmp_path, FakeProducer())
    sink.close()
    sink.close()                                 # gọi 2 lần không lỗi
    with pytest.raises(RuntimeError):
        sink.write([make_price(1)])


def test_invalid_run_id(tmp_path):
    with pytest.raises(ValueError):
        KafkaSink("a/b", failed_dir=tmp_path, producer=FakeProducer())


# ------------------------------ MultiSink ------------------------------

def test_multisink_file_still_written_when_kafka_fails(tmp_path):
    fsink = FileSink(tmp_path / "landing", "run-1")
    ksink = kafka_sink(tmp_path, FakeProducer("error"))
    with MultiSink([fsink, ksink]) as sink:
        sink.write([make_price(i) for i in range(4)])
    assert fsink.events_written == 4
    assert len(failed_lines(tmp_path)) == 4


class BrokenSink(FileSink):
    def write(self, events):
        raise OSError("disk full")


def test_multisink_one_sink_raising_does_not_stop_others(tmp_path, caplog):
    producer = FakeProducer()
    broken = BrokenSink(tmp_path / "landing", "run-1")
    with MultiSink([broken, kafka_sink(tmp_path, producer)]) as sink:
        with caplog.at_level(logging.ERROR):
            sink.write([make_price(1)])          # không raise vì Kafka vẫn ghi được
    assert len(producer.produced) == 1
    assert "BrokenSink failed" in caplog.text


def test_multisink_raises_when_every_sink_fails(tmp_path):
    sink = MultiSink([BrokenSink(tmp_path / "a", "run-1"), BrokenSink(tmp_path / "b", "run-1")])
    with pytest.raises(OSError):
        sink.write([make_price(1)])


def test_multisink_closes_all_even_if_one_fails(tmp_path):
    class BadClose(FileSink):
        def close(self):
            raise RuntimeError("boom")

    good = FileSink(tmp_path, "run-1")
    MultiSink([BadClose(tmp_path, "run-1"), good]).close()
    with pytest.raises(RuntimeError):
        good.write([make_price(1)])              # good đã được đóng


# ------------------------------ make_sink ------------------------------

def test_make_sink_from_config(tmp_path):
    sink = make_sink(SinkConfig(kinds=("file", "kafka"), kafka_failed_dir=tmp_path / "f"), "run-1",
                     landing_dir=tmp_path / "landing")
    assert isinstance(sink, MultiSink)
    assert [type(s) for s in sink.sinks] == [FileSink, KafkaSink]   # đúng thứ tự trong SINK
    assert sink.sinks[1].failed_dir == tmp_path / "f"
    sink.close()


def test_make_sink_string_list(tmp_path):
    single = make_sink("kafka", "run-1", landing_dir=tmp_path)
    assert isinstance(single, KafkaSink)
    single.close()
    assert isinstance(make_sink("file", "run-1", landing_dir=tmp_path), FileSink)
    with pytest.raises(ValueError, match="duplicate"):
        make_sink("file,file", "run-1", landing_dir=tmp_path)


# ------------------------------ broker thật ------------------------------

def broker_available(host: str = "localhost", port: int = 9092) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.kafka
@pytest.mark.skipif(not broker_available(), reason="no Kafka broker on localhost:9092")
def test_real_broker_100_events_round_trip(tmp_path):
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = f"test.kafka_sink.{uuid.uuid4().hex[:8]}"    # topic tạm, không đụng raw.price
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(10)
    try:
        events = [make_price(i, platform=("tiki", "tgdd")[i % 2]) for i in range(100)]
        with KafkaSink("run-real", failed_dir=tmp_path, topics={"price": topic, "product": topic}) as sink:
            for start in range(0, 100, 40):              # lô ~40 event như một trang listing
                sink.write(events[start:start + 40])
        assert sink.delivered == {topic: 100} and not sink.failed

        consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": f"test-{uuid.uuid4().hex}",
                             "auto.offset.reset": "earliest", "enable.auto.commit": False})
        consumer.subscribe([topic])
        messages, deadline = [], time.monotonic() + 30
        while len(messages) < 100 and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                messages.append(msg)
        consumer.close()

        assert len(messages) == 100
        assert sorted(m.value() for m in messages) == sorted(serialize_event(e) for e in events)
        ids = {e.event_id for e in events}
        assert len(ids) == 100
        # Cùng key luôn vào cùng partition (giữ thứ tự giá theo sản phẩm)
        partition_of_key: dict[bytes, int] = {}
        for m in messages:
            assert m.key() is not None
            assert partition_of_key.setdefault(m.key(), m.partition()) == m.partition()
            assert dict(m.headers())["ingest_source"] == b"live"
    finally:
        admin.delete_topics([topic])[topic].result(10)
