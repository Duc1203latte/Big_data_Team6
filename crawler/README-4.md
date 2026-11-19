# Giai đoạn 4 — Speed Layer và Cảnh báo giá bất thường

Tài liệu bàn giao tầng xử lý luồng (Speed Layer) cho thành viên khác (Serving Layer, Dashboard, Kiểm thử tải) và báo cáo tổng kết.

## 1. Các thành phần đã triển khai

| Thành phần | File | Mô tả |
| --- | --- | --- |
| Cấu hình ngưỡng | `config/anomaly_rules.yaml` | Ngưỡng tham số cho 3 luật bất thường, watermark (10 phút) và chu kỳ micro-batch (10 giây) |
| Thư viện Speed I/O | `common/speed_io.py` | Hàm chấm điểm 3 luật bất thường, HTTP client cho ClickHouse và RESP socket client cho Redis |
| Luồng chính | `jobs/price_stream.py` | Đọc `raw.price`, chuẩn hóa bằng `common.normalize`, khử trùng với watermark 10 phút, tra cứu Gold + Redis `price:last`, chấm điểm 3 luật, ghi ClickHouse (`price_realtime`, `price_window_stats`), đẩy cảnh báo vào `alerts.price` và lỗi vào `dlq.invalid` |
| Luồng lưu cảnh báo | `jobs/alert_sink.py` | Đọc `alerts.price`, ghi bảng ClickHouse `bigdata.price_alerts` (trạng thái `new`) và cập nhật Redis hash `alerts:active` |
| Luồng lưu bản ghi lỗi | `jobs/dlq_sink.py` | Đọc `dlq.invalid`, ghi bản ghi hỏng xuống HDFS `/datalake/quarantine/price` dạng Delta (partition theo `date`) |
| Đối soát ban đêm | `jobs/verify_alerts.py` | Đọc các cảnh báo `new` trong ClickHouse, tính lại 3 luật trên toàn bộ dữ liệu Silver/Gold đầy đủ, cập nhật trạng thái thành `confirmed` hoặc `false_positive` (đã tích hợp vào `spark/nightly_batch.sh`) |
| Công cụ tiêm sự kiện & đo trễ | `jobs/inject_test_events.py` | Bơm 36 sự kiện giả (10 giảm giá 70%, 10 nâng giá gốc khuyến mãi ảo, 10 rẻ hơn sàn khác, 6 giá sai > 2 tỷ VND) và xuất báo cáo độ trễ p50/p95 |
| Hạ tầng phục vụ | `infra/docker-compose.speed.yml`, `infra/clickhouse/init_tables.sql` | Container ClickHouse (cổng 8123/9000), Redis (cổng 6379) và DDL tạo 3 bảng `price_realtime`, `price_window_stats`, `price_alerts` |

## 2. Cách chạy Speed Layer

```bash
# 1. Bật ClickHouse và Redis (cùng mạng Docker bigdata_default)
docker compose -f infra/docker-compose.speed.yml up -d
docker exec -i clickhouse clickhouse-client --multiquery < infra/clickhouse/init_tables.sql

# 2. Chạy luồng chính price_stream và alert_sink ở chế độ nền (24/7)
bash spark/submit.sh --name price-stream --detach --ui-port 4041 --conf spark.cores.max=1 jobs/price_stream.py
bash spark/submit.sh --name alert-sink --detach --ui-port 4042 --conf spark.cores.max=1 jobs/alert_sink.py

# 3. Chạy dlq_sink và verify_alerts (theo đợt hoặc qua nightly_batch.sh)
bash spark/submit.sh --ui-port 4043 jobs/dlq_sink.py --available-now
bash spark/submit.sh --ui-port 4044 jobs/verify_alerts.py

# 4. Tiêm dữ liệu kiểm thử và in thống kê độ trễ
python -m jobs.inject_test_events
python -m jobs.inject_test_events --summary-only