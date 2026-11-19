import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.speed_io import ClickHouseHTTP, SimpleRedisClient, fmt_dt

log = logging.getLogger("alert_sink")


def process_alert_batch(batch_df, batch_id, args):
    rows = batch_df.collect()
    if not rows:
        return

    now_utc = datetime.now(timezone.utc)
    now_str = fmt_dt(now_utc)
    ch_rows = []
    redis_cmds = []

    for r in rows:
        try:
            data = json.loads(r.raw)
        except Exception:
            continue

        ing_str = data.get("ingest_time") or data.get("event_time") or now_str
        try:
            clean_ts = ing_str.strip().replace(" ", "T").replace("Z", "")
            ing_dt = datetime.fromisoformat(clean_ts).replace(tzinfo=timezone.utc)
            latency_sec = max(0.0, round((now_utc - ing_dt).total_seconds(), 3))
        except Exception:
            latency_sec = float(data.get("latency_sec") or 0.0)

        alert_id = data.get("alert_id") or f"alert:{data.get('event_id')}"
        ch_rows.append({
            "alert_id": alert_id,
            "event_id": data.get("event_id") or "",
            "platform": data.get("platform") or "",
            "product_id": data.get("product_id") or "",
            "product_key": data.get("product_key") or "",
            "canonical_id": data.get("canonical_id") or "",
            "name_raw": data.get("name_raw") or "",
            "price_current": int(data.get("price_current") or 0),
            "price_original": int(data["price_original"]) if data.get("price_original") is not None else None,
            "discount_pct": float(data["discount_pct"]) if data.get("discount_pct") is not None else None,
            "baseline_median": float(data["baseline_median"]) if data.get("baseline_median") is not None else None,
            "baseline_mad": float(data["baseline_mad"]) if data.get("baseline_mad") is not None else None,
            "other_platform_min_price": int(data["other_platform_min_price"]) if data.get("other_platform_min_price") is not None else None,
            "anomaly_score": float(data.get("anomaly_score") or 0.0),
            "anomaly_reason": data.get("anomaly_reason") or [],
            "status": "new",
            "event_time": data.get("event_time") or now_str,
            "alert_time": now_str,
            "latency_sec": latency_sec,
            "verified_at": None,
        })

        redis_cmds.append(("HSET", "alerts:active", alert_id, r.raw))

    if ch_rows:
        ch = ClickHouseHTTP(host=args.clickhouse_host, port=args.clickhouse_port)
        ch.insert_json_rows("price_alerts", ch_rows)

    if redis_cmds:
        try:
            rd = SimpleRedisClient(host=args.redis_host, port=args.redis_port)
            rd.pipeline(redis_cmds)
        except Exception as exc:
            log.warning("Batch %d: failed to write alerts:active to Redis: %s", batch_id, exc)

    log.info("Batch %d: saved %d alerts to ClickHouse and Redis", batch_id, len(ch_rows))


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Consume alerts.price into ClickHouse and Redis")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="alerts.price")
    parser.add_argument("--checkpoint-dir", default="/checkpoints/alert_sink")
    parser.add_argument("--clickhouse-host", default="clickhouse")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--redis-host", default="redis")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("alert_sink").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast("string").alias("raw"))
    )

    writer = (
        df.writeStream
        .queryName("alert_sink")
        .foreachBatch(lambda bdf, bid: process_alert_batch(bdf, bid, args))
        .option("checkpointLocation", args.checkpoint_dir)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()