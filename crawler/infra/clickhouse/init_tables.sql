CREATE DATABASE IF NOT EXISTS bigdata;

-- 1. Bảng lưu giá thời gian thực từ Speed Layer (ReplacingMergeTree tự khử trùng theo event_id)
CREATE TABLE IF NOT EXISTS bigdata.price_realtime (
    event_id String,
    platform LowCardinality(String),
    product_id String,
    product_key String,
    canonical_id String,
    name_raw String,
    price_current Int64,
    price_original Nullable(Int64),
    discount_pct Nullable(Float64),
    in_stock Nullable(UInt8),
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    processed_time DateTime64(3, 'UTC'),
    latency_sec Float64,
    is_late UInt8,
    is_anomaly UInt8,
    anomaly_score Float64,
    anomaly_reason Array(String),
    ingest_source LowCardinality(String)
) ENGINE = ReplacingMergeTree(processed_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (platform, product_key, event_time, event_id);

-- 2. Bảng thống kê cửa sổ trượt 1 giờ và 24 giờ (bước trượt 5 phút)
CREATE TABLE IF NOT EXISTS bigdata.price_window_stats (
    window_size LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    platform LowCardinality(String),
    product_key String,
    event_count UInt64,
    avg_price Float64,
    min_price Int64,
    max_price Int64,
    anomaly_count UInt64,
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_size, window_start, platform, product_key);

-- 3. Bảng cảnh báo giá bất thường (do alert_sink ghi vào và verify_alerts cập nhật trạng thái)
CREATE TABLE IF NOT EXISTS bigdata.price_alerts (
    alert_id String,
    event_id String,
    platform LowCardinality(String),
    product_id String,
    product_key String,
    canonical_id String,
    name_raw String,
    price_current Int64,
    price_original Nullable(Int64),
    discount_pct Nullable(Float64),
    baseline_median Nullable(Float64),
    baseline_mad Nullable(Float64),
    other_platform_min_price Nullable(Int64),
    anomaly_score Float64,
    anomaly_reason Array(String),
    status LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    alert_time DateTime64(3, 'UTC'),
    latency_sec Float64,
    verified_at Nullable(DateTime64(3, 'UTC'))
) ENGINE = ReplacingMergeTree(alert_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, alert_id);
