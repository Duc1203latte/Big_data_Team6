import argparse
import json
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.speed_io import (
    ClickHouseHTTP,
    SimpleRedisClient,
    evaluate_anomaly_rules,
    fmt_dt,
    load_anomaly_config,
)

log = logging.getLogger("verify_alerts")


def load_full_reference(spark, silver_root, gold_root):
    """Đọc dữ liệu chuẩn từ Gold và lịch sử giá Silver trên HDFS để đối soát lại cảnh báo."""
    base_df = (
        spark.read.format("delta")
        .load(f"{gold_root}/price_baseline")
        .select("product_key", "median_price", "mad_price")
    )
    master_df = (
        spark.read.format("delta")
        .load(f"{gold_root}/product_master")
        .select("product_key", "canonical_id")
    )
    silver_df = (
        spark.read.format("delta")
        .load(f"{silver_root}/price")
        .where(F.col("price_current").isNotNull() & (F.col("price_current") > 0))
        .select("product_key", "platform", "price_current")
    )

    plat_prices = (
        silver_df.join(master_df, on="product_key", how="left")
        .withColumn("cid", F.coalesce(F.col("canonical_id"), F.col("product_key")))
        .groupBy("cid", "platform")
        .agg(F.min("price_current").alias("plat_min_price"))
        .collect()
    )

    cid_plat_map = {}
    for r in plat_prices:
        if r.cid and r.platform and r.plat_min_price is not None:
            cid_plat_map.setdefault(r.cid, {})[r.platform] = int(r.plat_min_price)

    ref_rows = master_df.join(base_df, on="product_key", how="full_outer").collect()
    ref_map = {}
    for r in ref_rows:
        if not r.product_key:
            continue
        ref_map[r.product_key] = {
            "canonical_id": r.canonical_id or r.product_key,
            "median_price": float(r.median_price) if r.median_price is not None else None,
            "mad_price": float(r.mad_price) if r.mad_price is not None else None,
        }

    return ref_map, cid_plat_map


def verify_pending_alerts(spark, args):
    ch = ClickHouseHTTP(host=args.clickhouse_host, port=args.clickhouse_port)
    sql = """
    SELECT
        alert_id, event_id, platform, product_id, product_key, canonical_id,
        name_raw, price_current, price_original, discount_pct,
        baseline_median, baseline_mad, other_platform_min_price,
        anomaly_score, anomaly_reason, status, event_time, alert_time, latency_sec
    FROM bigdata.price_alerts FINAL
    WHERE status = 'new'
    """
    pending = ch.query_json_rows(sql)
    if not pending:
        log.info("No pending alerts with status='new' to verify")
        return

    config = load_anomaly_config(args.rules_config)
    ref_map, cid_plat_map = load_full_reference(spark, args.silver_root, args.gold_root)

    now_str = fmt_dt(datetime.now(timezone.utc))
    updated_rows = []
    redis_cmds = []
    confirmed_count = 0
    fp_count = 0

    for row in pending:
        pkey = row["product_key"]
        plat = row["platform"]
        ref = ref_map.get(pkey, {})
        cid = ref.get("canonical_id") or row.get("canonical_id") or pkey

        median_val = ref.get("median_price")
        if median_val is None:
            median_val = row.get("baseline_median")

        mad_val = ref.get("mad_price")
        if mad_val is None:
            mad_val = row.get("baseline_mad")

        plat_map = cid_plat_map.get(cid, {})
        other_prices = [p for pl, p in plat_map.items() if pl != plat and p > 0]
        other_min = min(other_prices) if other_prices else row.get("other_platform_min_price")

        is_anom, new_score, new_reasons = evaluate_anomaly_rules(
            price_current=row.get("price_current"),
            price_original=row.get("price_original"),
            discount_pct=row.get("discount_pct"),
            baseline_median=median_val,
            baseline_mad=mad_val,
            other_platform_min_price=other_min,
            config=config,
        )

        if is_anom:
            new_status = "confirmed"
            confirmed_count += 1
        else:
            new_status = "false_positive"
            fp_count += 1

        # Bảng price_alerts dùng ReplacingMergeTree(alert_time) nên cần cập nhật alert_time = now_str
        # để bản ghi đã đối soát ghi đè bản ghi 'new' khi OPTIMIZE FINAL
        updated = {
            "alert_id": row["alert_id"],
            "event_id": row["event_id"],
            "platform": plat,
            "product_id": row["product_id"],
            "product_key": pkey,
            "canonical_id": cid,
            "name_raw": row.get("name_raw") or "",
            "price_current": int(row["price_current"]),
            "price_original": int(row["price_original"]) if row.get("price_original") is not None else None,
            "discount_pct": float(row["discount_pct"]) if row.get("discount_pct") is not None else None,
            "baseline_median": float(median_val) if median_val is not None else None,
            "baseline_mad": float(mad_val) if mad_val is not None else None,
            "other_platform_min_price": int(other_min) if other_min is not None else None,
            "anomaly_score": new_score if is_anom else float(row.get("anomaly_score") or 0.0),
            "anomaly_reason": new_reasons if is_anom else (row.get("anomaly_reason") or []),
            "status": new_status,
            "event_time": fmt_dt(row["event_time"]),
            "alert_time": now_str,
            "latency_sec": float(row.get("latency_sec") or 0.0),
            "verified_at": now_str,
        }
        updated_rows.append(updated)

        if new_status == "false_positive":
            redis_cmds.append(("HDEL", "alerts:active", row["alert_id"]))
        else:
            redis_cmds.append(("HSET", "alerts:active", row["alert_id"], json.dumps(updated, ensure_ascii=False)))

    ch.insert_json_rows("price_alerts", updated_rows)
    ch.execute("OPTIMIZE TABLE bigdata.price_alerts FINAL")

    if redis_cmds:
        try:
            rd = SimpleRedisClient(host=args.redis_host, port=args.redis_port)
            rd.pipeline(redis_cmds)
        except Exception as exc:
            log.warning("Failed to update Redis during verify_alerts: %s", exc)

    log.info(
        "Verified %d alerts: confirmed=%d, false_positive=%d",
        len(updated_rows), confirmed_count, fp_count,
    )


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Verify pending price alerts against full Silver/Gold data")
    parser.add_argument("--silver-root", default="/datalake/silver")
    parser.add_argument("--gold-root", default="/datalake/gold")
    parser.add_argument("--rules-config", default="/app/config/anomaly_rules.yaml")
    parser.add_argument("--clickhouse-host", default="clickhouse")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--redis-host", default="redis")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("verify_alerts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    verify_pending_alerts(spark, args)
    spark.stop()


if __name__ == "__main__":
    main()