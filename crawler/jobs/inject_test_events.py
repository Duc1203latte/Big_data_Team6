import argparse
import json
import logging
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from common.config import load_env_file
from common.ids import make_product_key
from common.sink import PRODUCER_CONFIG
from common.speed_io import ClickHouseHTTP, SimpleRedisClient

log = logging.getLogger("inject_test_events")


def make_price_event(item, tag, idx, price_current, price_original, now_iso):
    ev_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"inject:{tag}:{item['product_key']}:{now_iso}:{idx}"))
    return {
        "event_id": ev_id,
        "event_type": "price",
        "schema_version": 1,
        "crawler_version": "inject-test-1.0",
        "platform": item["platform"],
        "product_id": item["product_id"],
        "sku_id": None,
        "url": f"https://{item['platform']}.vn/test-{item['product_id']}",
        "name_raw": item["name_raw"],
        "source": "listing",
        "price_current": price_current,
        "price_current_raw": f"{price_current}đ",
        "price_original": price_original,
        "currency": "VND",
        "in_stock": True,
        "event_time": now_iso,
    }


def build_synthetic_events(ch_host, ch_port, redis_host, redis_port):
    """Lấy các sản phẩm hợp lệ từ ClickHouse để sinh bộ sự kiện test 3 luật + lỗi giá."""
    ch = ClickHouseHTTP(host=ch_host, port=ch_port)
    samples = ch.query_json_rows("""
        SELECT
            platform,
            any(product_id) AS product_id,
            product_key,
            any(canonical_id) AS canonical_id,
            any(name_raw) AS name_raw,
            any(price_current) AS sample_price
        FROM bigdata.price_realtime
        WHERE price_current >= 5000000 AND is_anomaly = 0
        GROUP BY platform, product_key
        LIMIT 30
    """)
    if not samples:
        raise RuntimeError("price_realtime has no valid sample rows (>= 5M VND); run price_stream first.")

    while len(samples) < 30:
        samples.extend(samples[: 30 - len(samples)])

    rd = SimpleRedisClient(host=redis_host, port=redis_port)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    events = []
    redis_seed_cmds = []

    # Nhóm 1: Giảm sâu 70% so với giá thường (test lệch lịch sử)
    for i, item in enumerate(samples[:10]):
        normal_price = int(item["sample_price"])
        drop_price = int(normal_price * 0.30)
        events.append(make_price_event(item, "hist", i, drop_price, normal_price, now_iso))

    # Nhóm 2: Nâng khống giá gốc lên 2.2 lần nhưng giá bán giữ nguyên (test khuyến mãi ảo)
    for i, item in enumerate(samples[10:20]):
        normal_price = int(item["sample_price"])
        inflated_orig = int(normal_price * 2.2)
        events.append(make_price_event(item, "fake", i, normal_price, inflated_orig, now_iso))

    # Nhóm 3: Giá rẻ hơn 35% so với sàn khác đang lưu trên Redis (test chéo sàn)
    for i, item in enumerate(samples[20:30]):
        normal_price = int(item["sample_price"])
        cid = item["canonical_id"] or item["product_key"]
        other_plat = "tgdd" if item["platform"] != "tgdd" else "tiki"

        redis_seed_cmds.append(("DEL", f"price:last:{cid}"))
        redis_seed_cmds.append(("HSET", f"price:last:{cid}", other_plat, str(normal_price)))

        cheaper_price = int(normal_price * 0.65)
        events.append(make_price_event(item, "cross", i, cheaper_price, normal_price, now_iso))

    # Nhóm 4: Giá lỗi vượt ngưỡng (> 2 tỷ VND) để kiểm tra luồng đẩy vào dlq.invalid
    for i, item in enumerate(samples[:6]):
        bad_price = max(int(item["sample_price"]) * 1000, 25_000_000_000)
        events.append(make_price_event(item, "badprice", i, bad_price, None, now_iso))

    if redis_seed_cmds:
        rd.pipeline(redis_seed_cmds)

    return events


def print_metrics_summary(ch_host, ch_port):
    ch = ClickHouseHTTP(host=ch_host, port=ch_port)
    lat = ch.query_json_rows("""
        SELECT
            ingest_source,
            count() AS total_events,
            round(quantile(0.50)(latency_sec), 3) AS latency_p50_sec,
            round(quantile(0.95)(latency_sec), 3) AS latency_p95_sec,
            round(max(latency_sec), 3) AS latency_max_sec
        FROM bigdata.price_realtime FINAL
        GROUP BY ingest_source
        ORDER BY ingest_source
    """)
    by_rule = ch.query_json_rows("""
        SELECT
            reason,
            count() AS alert_count
        FROM bigdata.price_alerts FINAL
        ARRAY JOIN anomaly_reason AS reason
        GROUP BY reason
        ORDER BY alert_count DESC
    """)
    by_status = ch.query_json_rows("""
        SELECT status, count() AS count
        FROM bigdata.price_alerts FINAL
        GROUP BY status
        ORDER BY status
    """)

    print("\n=== SPEED LAYER METRICS SUMMARY ===")
    print("1. Latency by source:", json.dumps(lat, ensure_ascii=False))
    print("2. Alerts by rule:", json.dumps(by_rule, ensure_ascii=False))
    print("3. Alerts by status:", json.dumps(by_status, ensure_ascii=False))


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_env_file(".env")

    parser = argparse.ArgumentParser(description="Inject test anomaly and invalid price events into Kafka")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="raw.price")
    parser.add_argument("--clickhouse-host", default="localhost")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args(argv)

    if args.summary_only:
        print_metrics_summary(args.clickhouse_host, args.clickhouse_port)
        return

    events = build_synthetic_events(args.clickhouse_host, args.clickhouse_port, args.redis_host, args.redis_port)
    producer = Producer({**PRODUCER_CONFIG, "bootstrap.servers": args.bootstrap, "client.id": "inject-test"})

    for ev in events:
        key = make_product_key(ev["platform"], ev["product_id"]).encode("utf-8")
        val = json.dumps(ev, ensure_ascii=False).encode("utf-8")
        headers = [
            ("event_type", b"price"),
            ("schema_version", b"1"),
            ("crawler_version", b"inject-test-1.0"),
            ("ingest_source", b"live"),
        ]
        producer.produce(args.topic, value=val, key=key, headers=headers)

    producer.flush(30.0)
    log.info("Injected %d test events into topic %s", len(events), args.topic)


if __name__ == "__main__":
    main()