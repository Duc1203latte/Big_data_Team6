-- Bảng phía batch (tuần 3, thành viên 2). Chạy tự động lần đầu container clickhouse khởi động (volume trống);
-- chạy lại tay không sao: mọi lệnh đều IF NOT EXISTS.
--   docker exec -i clickhouse clickhouse-client -u bigdata --password ... < infra/clickhouse/init/01_gold_batch_view.sql
-- Ai ghi bảng nào thì người đó tạo bảng đó: price_realtime, price_window_stats, price_alerts do speed layer
-- (tuần 4) tạo trong file 02_*.sql. Mọi thời gian là UTC.

CREATE DATABASE IF NOT EXISTS bigdata;

-- Batch view: mỗi dòng = một SKU trong một ngày UTC, tính từ Silver bởi job daily_stats.
-- Job xoá cả partition của ngày rồi ghi lại, nên chạy lại cùng ngày không sinh dòng trùng.
-- price_serving (tuần 5) = price_batch_daily (date < ngày của mốc cắt) + price_realtime (event_time >= mốc cắt).
CREATE TABLE IF NOT EXISTS bigdata.price_batch_daily
(
    date                   Date,
    platform               LowCardinality(String),
    product_key            String,
    sku_id                 String,                  -- '' khi Silver null (cellphones)
    master_id              Nullable(String),        -- cùng master_id = cùng sản phẩm ở nhiều sàn
    category               LowCardinality(String),
    brand                  Nullable(String),
    name                   String,
    price_min              Int64,
    price_max              Int64,
    price_avg              Float64,
    price_median           Float64,
    price_last             Int64,                   -- giá ở lần quan sát cuối trong ngày
    price_original_median  Nullable(Float64),
    discount_pct_avg       Nullable(Float64),
    n_obs                  UInt32,
    first_event_time       DateTime64(3, 'UTC'),
    last_event_time        DateTime64(3, 'UTC'),
    updated_at             DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (platform, product_key, sku_id);

-- Mốc cắt giữa batch view và real-time view. Một dòng cho mỗi view; đọc bằng FINAL để lấy dòng mới nhất.
--   cutoff = 00:00 UTC của ngày sau ngày batch cuối cùng (batch phủ [đầu, cutoff), real-time phủ [cutoff, nay))
--   SELECT cutoff FROM bigdata.serving_cutoff FINAL WHERE view_name = 'price'
CREATE TABLE IF NOT EXISTS bigdata.serving_cutoff
(
    view_name        LowCardinality(String),
    cutoff           DateTime64(3, 'UTC'),
    batch_last_date  Date,
    updated_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY view_name;

-- Thống kê ngày theo sàn, danh mục, thương hiệu (bản sao của Gold daily_price_stats cho dashboard).
-- Tính trên giá trung vị trong ngày của từng SKU, để SKU crawl dày không nặng ký hơn SKU crawl thưa.
CREATE TABLE IF NOT EXISTS bigdata.daily_price_stats
(
    date          Date,
    platform      LowCardinality(String),
    category      LowCardinality(String),
    brand         String,                         -- 'unknown' khi không xác định được
    n_skus        UInt32,
    n_obs         UInt64,
    price_avg     Float64,
    price_median  Float64,
    price_std     Nullable(Float64),
    updated_at    DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (platform, category, brand);

-- Chênh lệch giá cùng sản phẩm giữa các sàn (dùng product_master), mỗi dòng = một master_id trong một ngày.
CREATE TABLE IF NOT EXISTS bigdata.daily_price_spread
(
    date               Date,
    master_id          String,
    name               String,
    category           LowCardinality(String),
    brand              Nullable(String),
    n_platforms        UInt8,
    price_min          Float64,
    price_max          Float64,
    spread_pct         Float64,                   -- (max - min) / min * 100
    cheapest_platform  LowCardinality(String),
    priciest_platform  LowCardinality(String),
    updated_at         DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY master_id;
