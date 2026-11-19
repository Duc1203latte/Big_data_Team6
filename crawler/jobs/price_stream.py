import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.normalize import BAD_JSON, normalize_price
from common.speed_io import (
    ClickHouseHTTP,
    SimpleRedisClient,
    evaluate_anomaly_rules,
    fmt_dt,
    load_anomaly_config,
)
from jobs.rebuild_silver import PRICE_SCHEMA, merge_silver, to_tuple

log = logging.getLogger("price_stream")


def ensure_utc(dt):
    """Đưa datetime về UTC (timezone-aware) để trừ thời gian không bị lỗi lệch múi giờ."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def split_and_normalize_rows(rows):
    """Parse JSON và chuẩn hóa dữ liệu, tách thành 2 danh sách: hợp lệ và lỗi (DLQ)."""
    valid_dicts = []
    invalid_items = []

    for r in rows:
        raw_str = r.raw
        ingest_time = r.ingest_time
        ingest_source = r.ingest_source or "live"

        try:
            event = json.loads(raw_str)
            if not isinstance(event, dict):
                raise ValueError("Payload is not a JSON object")
            norm_row, errors = normalize_price(event)
        except (TypeError, ValueError):
            event, norm_row, errors = {}, None, [BAD_JSON]

        if errors:
            ev_id = event.get("event_id")
            if not isinstance(ev_id, str) or not ev_id.strip():
                ev_id = f"invalid:{r.kafka_partition}:{r.kafka_offset}"

            invalid_items.append({
                "event_id": ev_id,
                "platform": event.get("platform") or "_unknown",
                "dq_errors": errors,
                "raw": raw_str,
                "kafka_partition": r.kafka_partition,
                "kafka_offset": r.kafka_offset,
                "ingest_time": fmt_dt(ingest_time),
                "ingest_source": ingest_source,
            })
        else:
            norm_row["ingest_time"] = ingest_time
            norm_row["ingest_source"] = ingest_source
            valid_dicts.append(norm_row)

    return valid_dicts, invalid_items


class GoldLookupCache:
    """Cache dữ liệu product_master và price_baseline trên RAM, định kỳ làm mới."""

    def __init__(self, spark, gold_root, refresh_sec=600.0):
        self.spark = spark
        self.gold_root = gold_root
        self.refresh_sec = refresh_sec
        self.last_loaded = 0.0
        self.data = {}

    def get_map(self):
        now = time.monotonic()
        if self.last_loaded > 0 and (now - self.last_loaded) < self.refresh_sec:
            return self.data

        try:
            # Đọc riêng từng bảng rồi ghép bằng dict trên Python để tránh Spark Shuffle Join
            master_rows = (
                self.spark.read.format("delta")
                .load(f"{self.gold_root}/product_master")
                .select("product_key", "canonical_id")
                .collect()
            )
            base_rows = (
                self.spark.read.format("delta")
                .load(f"{self.gold_root}/price_baseline")
                .select("product_key", "median_price", "mad_price")
                .collect()
            )

            new_map = {}
            for r in master_rows:
                if r.product_key:
                    new_map[r.product_key] = {
                        "canonical_id": r.canonical_id or r.product_key,
                        "median_price": None,
                        "mad_price": None,
                    }
            for r in base_rows:
                if r.product_key:
                    entry = new_map.setdefault(
                        r.product_key,
                        {"canonical_id": r.product_key, "median_price": None, "mad_price": None},
                    )
                    entry["median_price"] = float(r.median_price) if r.median_price is not None else None
                    entry["mad_price"] = float(r.mad_price) if r.mad_price is not None else None

            self.data = new_map
            log.info("Loaded Gold reference cache: %d products", len(self.data))
        except Exception as exc:
            log.warning("Could not refresh Gold cache (%s), keeping existing data", exc)
        finally:
            self.last_loaded = time.monotonic()

        return self.data


def compute_and_write_windows(ch_client, valid_rows, now_utc):
    """Tính thống kê cửa sổ 1h và 24h (bước trượt 5 phút) và ghi xuống ClickHouse."""
    if not valid_rows:
        return

    win_specs = [("1h", 3600), ("24h", 86400)]
    step = 300  # 5 phút
    buckets = {}

    for item in valid_rows:
        price = item["price_current"]
        if price is None or price <= 0:
            continue

        ev_ts = int(item["event_time"].timestamp())
        aligned_end = ((ev_ts // step) + 1) * step

        for label, duration in win_specs:
            w_end = aligned_end
            w_start = w_end - duration
            key = (label, w_start, w_end, item["platform"], item["product_key"])

            if key not in buckets:
                buckets[key] = {
                    "count": 0,
                    "sum_price": 0,
                    "min_price": price,
                    "max_price": price,
                    "anomaly_count": 0,
                }
            b = buckets[key]
            b["count"] += 1
            b["sum_price"] += price
            if price < b["min_price"]:
                b["min_price"] = price
            if price > b["max_price"]:
                b["max_price"] = price
            if item["is_anomaly"]:
                b["anomaly_count"] += 1

    out_rows = []
    now_str = fmt_dt(now_utc)
    for (label, w_start, w_end, platform, product_key), b in buckets.items():
        dt_start = datetime.fromtimestamp(w_start, tz=timezone.utc)
        dt_end = datetime.fromtimestamp(w_end, tz=timezone.utc)
        out_rows.append({
            "window_size": label,
            "window_start": fmt_dt(dt_start),
            "window_end": fmt_dt(dt_end),
            "platform": platform,
            "product_key": product_key,
            "event_count": b["count"],
            "avg_price": round(b["sum_price"] / b["count"], 2),
            "min_price": b["min_price"],
            "max_price": b["max_price"],
            "anomaly_count": b["anomaly_count"],
            "updated_at": now_str,
        })

    if out_rows:
        ch_client.insert_json_rows("price_window_stats", out_rows)


def process_micro_batch(batch_df, batch_id, spark, args, gold_cache):
    raw_rows = batch_df.collect()
    if not raw_rows:
        return

    now_utc = datetime.now(timezone.utc)
    config = load_anomaly_config(args.rules_config)
    watermark_sec = config.get("watermark_minutes", 10) * 60

    valid_dicts, invalid_items = split_and_normalize_rows(raw_rows)

    realtime_rows = []
    alert_kafka_rows = []
    redis_updates = []
    window_input = []

    if valid_dicts:
        gold_map = gold_cache.get_map()

        # Sắp xếp theo thời gian sự kiện để giá mới hơn luôn được cập nhật sau cùng vào Redis
        valid_dicts.sort(key=lambda x: ensure_utc(x.get("event_time")) or now_utc)

        redis_client = SimpleRedisClient(host=args.redis_host, port=args.redis_port)
        canon_ids = sorted({
            gold_map.get(d["product_key"], {}).get("canonical_id") or d["product_key"]
            for d in valid_dicts
        })
        redis_keys = [f"price:last:{cid}" for cid in canon_ids]
        try:
            redis_prices_map = redis_client.hgetall_many(redis_keys)
        except Exception as exc:
            log.warning("Batch %d: Redis read failed: %s", batch_id, exc)
            redis_prices_map = {}

        ev_times_utc = [ensure_utc(d["event_time"]) for d in valid_dicts if d.get("event_time")]
        max_ev_time = max(ev_times_utc) if ev_times_utc else now_utc

        for d in valid_dicts:
            pkey = d["product_key"]
            plat = d["platform"]
            ref = gold_map.get(pkey, {})
            cid = ref.get("canonical_id") or pkey
            median_price = ref.get("median_price")
            mad_price = ref.get("mad_price")

            platform_map = redis_prices_map.get(f"price:last:{cid}", {})

            # Tìm giá thấp nhất của sản phẩm trên các sàn khác
            other_prices = []
            for o_plat, p_str in platform_map.items():
                if o_plat != plat:
                    try:
                        val = int(float(p_str))
                        if val > 0:
                            other_prices.append(val)
                    except ValueError:
                        pass
            other_min_price = min(other_prices) if other_prices else None

            is_anomaly, score, reasons = evaluate_anomaly_rules(
                price_current=d["price_current"],
                price_original=d["price_original"],
                discount_pct=d["discount_pct"],
                baseline_median=median_price,
                baseline_mad=mad_price,
                other_platform_min_price=other_min_price,
                config=config,
            )

            proc_time = datetime.now(timezone.utc)
            ev_time = ensure_utc(d["event_time"]) or proc_time
            ing_time = ensure_utc(d["ingest_time"]) or proc_time

            ref_time = ev_time if d["ingest_source"] == "live" else ing_time
            latency_sec = max(0.0, round((proc_time - ref_time).total_seconds(), 3))
            is_late = 1 if (max_ev_time - ev_time).total_seconds() > watermark_sec else 0

            if d["price_current"] is not None and d["price_current"] > 0:
                cur_price = int(d["price_current"])
                orig_price = int(d["price_original"]) if d["price_original"] is not None else None
                disc_pct = float(d["discount_pct"]) if d["discount_pct"] is not None else None

                realtime_rows.append({
                    "event_id": d["event_id"],
                    "platform": plat,
                    "product_id": d["product_id"],
                    "product_key": pkey,
                    "canonical_id": cid,
                    "name_raw": d["name_raw"] or "",
                    "price_current": cur_price,
                    "price_original": orig_price,
                    "discount_pct": disc_pct,
                    "in_stock": (1 if d["in_stock"] else 0) if d["in_stock"] is not None else None,
                    "event_time": fmt_dt(ev_time),
                    "ingest_time": fmt_dt(ing_time),
                    "processed_time": fmt_dt(proc_time),
                    "latency_sec": latency_sec,
                    "is_late": is_late,
                    "is_anomaly": is_anomaly,
                    "anomaly_score": score,
                    "anomaly_reason": reasons,
                    "ingest_source": d["ingest_source"] or "live",
                })

                redis_updates.append(("HSET", f"price:last:{cid}", plat, str(cur_price)))
                platform_map[plat] = str(cur_price)

                window_input.append({
                    "platform": plat,
                    "product_key": pkey,
                    "price_current": cur_price,
                    "event_time": ev_time,
                    "is_anomaly": is_anomaly,
                })

                if is_anomaly:
                    alert_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"alert:{d['event_id']}"))
                    alert_payload = json.dumps({
                        "alert_id": alert_id,
                        "event_id": d["event_id"],
                        "platform": plat,
                        "product_id": d["product_id"],
                        "product_key": pkey,
                        "canonical_id": cid,
                        "name_raw": d["name_raw"] or "",
                        "price_current": cur_price,
                        "price_original": orig_price,
                        "discount_pct": disc_pct,
                        "baseline_median": median_price,
                        "baseline_mad": mad_price,
                        "other_platform_min_price": other_min_price,
                        "anomaly_score": score,
                        "anomaly_reason": reasons,
                        "status": "new",
                        "event_time": fmt_dt(ev_time),
                        "ingest_time": fmt_dt(ing_time),
                        "alert_time": fmt_dt(proc_time),
                        "latency_sec": latency_sec,
                        "ingest_source": d["ingest_source"] or "live",
                    }, ensure_ascii=False)
                    alert_kafka_rows.append((pkey, alert_payload))

        ch_client = ClickHouseHTTP(host=args.clickhouse_host, port=args.clickhouse_port)
        if realtime_rows:
            ch_client.insert_json_rows("price_realtime", realtime_rows)
        if window_input:
            compute_and_write_windows(ch_client, window_input, now_utc)

        if redis_updates:
            try:
                redis_client.pipeline(redis_updates)
            except Exception as exc:
                log.warning("Batch %d: Redis update failed: %s", batch_id, exc)

        if args.merge_silver:
            valid_tuples = [to_tuple(d, PRICE_SCHEMA) for d in valid_dicts]
            silver_df = spark.createDataFrame(valid_tuples, PRICE_SCHEMA)
            merge_silver(spark, f"{args.silver_root}/price", silver_df, "price")

    if alert_kafka_rows:
        alert_df = spark.createDataFrame(alert_kafka_rows, ["key", "value"])
        (
            alert_df.write.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("topic", args.alerts_topic)
            .save()
        )

    if invalid_items:
        dlq_records = [
            (item["event_id"], json.dumps(item, ensure_ascii=False))
            for item in invalid_items
        ]
        dlq_df = spark.createDataFrame(dlq_records, ["key", "value"])
        (
            dlq_df.write.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("topic", args.dlq_topic)
            .save()
        )
        log.info("Batch %d: routed %d invalid events to %s", batch_id, len(dlq_records), args.dlq_topic)

    log.info(
        "Batch %d: valid=%d, anomalies=%d, invalid=%d",
        batch_id, len(realtime_rows), len(alert_kafka_rows), len(invalid_items),
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Streaming job for realtime price anomaly detection")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--input-topic", default="raw.price")
    parser.add_argument("--alerts-topic", default="alerts.price")
    parser.add_argument("--dlq-topic", default="dlq.invalid")
    parser.add_argument("--silver-root", default="/datalake/silver")
    parser.add_argument("--gold-root", default="/datalake/gold")
    parser.add_argument("--checkpoint-dir", default="/checkpoints/price_stream")
    parser.add_argument("--rules-config", default="/app/config/anomaly_rules.yaml")
    parser.add_argument("--clickhouse-host", default="clickhouse")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--redis-host", default="redis")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-offsets", type=int, default=5000)
    parser.add_argument("--merge-silver", action="store_true")
    parser.add_argument("--available-now", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    spark = SparkSession.builder.appName("price_stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    gold_cache = GoldLookupCache(spark, args.gold_root, refresh_sec=600.0)
    gold_cache.get_map()

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .option("includeHeaders", "true")
        .load()
    )

    value_str = F.col("value").cast("string")
    ev_id = F.coalesce(
        F.get_json_object(value_str, "$.event_id"),
        F.concat_ws(":", F.lit("raw"), F.col("partition"), F.col("offset")),
    )
    ev_time_str = F.get_json_object(value_str, "$.event_time")
    ev_time = F.coalesce(F.to_timestamp(ev_time_str), F.col("timestamp"))

    stream_df = (
        kafka_df.select(
            value_str.alias("raw"),
            ev_id.alias("event_id"),
            ev_time.alias("event_time"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("ingest_time"),
            F.expr("cast(filter(headers, h -> h.key = 'ingest_source')[0].value as string)").alias("ingest_source"),
        )
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    writer = (
        stream_df.writeStream
        .queryName("price_stream")
        .foreachBatch(lambda df, bid: process_micro_batch(df, bid, spark, args, gold_cache))
        .option("checkpointLocation", args.checkpoint_dir)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    query = writer.start()
    log.info("Started price_stream from topic %s", args.input_topic)
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main() 