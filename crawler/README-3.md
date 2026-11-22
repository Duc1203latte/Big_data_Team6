# Tuần 3: Batch layer, Gold và batch view (thành viên 2)

README bàn giao cho tuần 4 (speed layer) và tuần 5 (serving, dashboard): cách chạy, dữ liệu ở đâu, cấu hình,
số đo và lỗi còn biết. Schema từng bảng, khoá Redis, bảng ClickHouse, đề xuất công thức 3 luật:
**[`docs/gold_schema.md`](docs/gold_schema.md)** (người dùng Gold bắt buộc đọc).

## 1. Đã làm gì

| Việc theo kế hoạch | Ở đâu | Trạng thái |
| --- | --- | --- |
| 1. `entity_resolution` → Gold `product_master`, Qdrant | `jobs/entity_resolution.py`, `common/product_text.py`, `common/entity_rules.py` | Xong |
| 2. Gán nhãn 500 cặp, precision/recall, chọn ngưỡng | `jobs/er_labeling.py`, `labeling/` | **Công cụ xong, chờ nhóm gán nhãn** |
| 3. `price_baseline` | `jobs/price_baseline.py` | Xong |
| 4. `daily_stats` → `daily_price_stats` (+ `daily_price_spread`) | `jobs/daily_stats.py` | Xong |
| 5. ClickHouse, Redis, `price_batch_daily`, `serving_cutoff` | `infra/docker-compose.yml` (profile `serving`), `infra/clickhouse/init/` | Xong |
| 6. DAG `nightly_batch` đủ 5 job | `airflow/dags/nightly_batch.py`, `airflow/README.md` | Xong, **chưa chạy trên máy chính** |
| `compact_bronze` (của 2B, chưa có) | `jobs/compact_bronze.py` | Xong (xem mục 6) |
| Khảo sát Silver trước khi làm | `analysis/explore_silver.py`, `docs/gold_step0_findings.md` | Xong |

Kiểm tra trên máy cá nhân với bản sao Silver 01–15/11 (Spark local và trong image cụm, Python 3.10); **chưa chạy
trên cụm HDFS của máy chính**, nên mục 2 dưới đây là việc đầu tiên khi lên máy chính.

## 2. Đưa lên máy chính (một lần)

Trong thư mục `crawler/`, sau khi pull nhánh này:

```bash
# a) Image Spark có thêm thư viện Gold (pandas, fastembed, qdrant-client, clickhouse-connect, redis)
docker build -t bigdata-spark:3.5.8-gold infra/spark
# infra/.env: SPARK_IMAGE=bigdata-spark:3.5.8-gold, thêm khối "Serving" từ .env.example, ĐỔI CLICKHOUSE_PASSWORD
cd infra
docker stop kafka-to-bronze
docker compose --profile spark up -d --force-recreate     # cụm dùng image mới
docker start kafka-to-bronze                                # chạy tiếp từ checkpoint, không mất không trùng
docker compose --profile serving up -d                      # Qdrant, ClickHouse, Redis (tự tạo bảng ClickHouse)
bash hdfs/init_dirs.sh                                      # thêm /datalake/gold, /datalake/bronze_compacted
cd ..

# b) Dựng Gold lần đầu cho toàn bộ Silver đang có
bash spark/submit.sh --mem 3g --conf spark.driver.memory=2g jobs/entity_resolution.py \
    --qdrant-url http://qdrant:6333 --model-cache /opt/spark/work-dir/models     # lần đầu tải model ~240 MB
bash spark/submit.sh --mem 1536m jobs/price_baseline.py
bash spark/submit.sh --mem 1536m jobs/daily_stats.py \
    --clickhouse-url http://clickhouse:8123 --redis-url redis://redis:6379/0  # không --date: mọi ngày trong Silver

# c) Lịch chạy đêm: bỏ cron, bật Airflow (airflow/README.md)
crontab -e                      # xoá dòng nightly_batch.sh
bash airflow/install.sh && bash airflow/run.sh   # hoặc service systemd
```

RAM thêm trên máy chính: Qdrant ≤ 512 MB, ClickHouse ≤ 1 GB, Redis ≤ 256 MB (giới hạn trong compose), Airflow
khoảng 500 MB; job `entity_resolution` dùng container 3 GB khi chạy (khoảng 3 phút mỗi đêm).

## 3. Chạy trên máy cá nhân (phát triển)

```bash
cd crawler && source .venv/bin/activate
pip install -r requirements.txt -r requirements-gold.txt -r requirements-spark-local.txt   # cần Java 17
cd infra && docker compose --profile serving up -d && cd ..         # chỉ cần Qdrant, ClickHouse, Redis
export CLICKHOUSE_USER=bigdata CLICKHOUSE_PASSWORD=<như infra/.env>
SILVER=~/data/silver_20261116                                         # bản sao Silver trên Drive
bash spark/run_local.sh jobs/entity_resolution.py --silver-root $SILVER --gold-root datalake_local/gold \
    --qdrant-url http://localhost:6333
bash spark/run_local.sh jobs/price_baseline.py --silver-root $SILVER --gold-root datalake_local/gold
bash spark/run_local.sh jobs/daily_stats.py --silver-root $SILVER --gold-root datalake_local/gold \
    --clickhouse-url http://localhost:8123 --redis-url redis://localhost:6379/0
pytest -q tests/test_product_text.py tests/test_entity_rules.py
```

`spark/run_local.sh` chạy đúng job của cụm ở Spark local mode; lên máy chính chỉ đổi `run_local.sh` thành `submit.sh`
và bỏ `--silver-root`, `--gold-root` (mặc định là đường dẫn HDFS).

## 4. Dữ liệu ở đâu

| Dữ liệu | Máy chính | Gửi nhóm |
| --- | --- | --- |
| Gold `product_master`, `price_baseline`, `daily_price_stats`, `daily_price_spread`, `er_candidates` | HDFS `/datalake/gold/<bảng>` (Delta) | `exports/gold_YYYYMMDD.tar.gz` mỗi sáng (`export_for_team.sh`), đưa lên Drive cạnh Silver |
| Embedding sản phẩm | Qdrant collection `products` (`localhost:6333/dashboard`) | |
| Batch view | ClickHouse `bigdata.price_batch_daily`, `serving_cutoff`, `daily_price_stats`, `daily_price_spread` | |
| Giá mới nhất mỗi sàn | Redis `price:last:{master_id}` | |
| Bản gộp Bronze | HDFS `/datalake/bronze_compacted/<loại>/date=/platform=` | |
| Model embedding | volume Docker `bigdata_spark-work`, `/opt/spark/work-dir/models` | |

## 5. Số đo (cho báo cáo tuần 6)

Bản sao Silver 01–15/11/2026 (3.408.376 dòng price, 11.032 dòng product), MacBook Air, Spark local 4 core:

| Đo | Kết quả |
| --- | --- |
| `price_baseline` | 15.519 SKU, 17 giây; 93% SKU có MAD = 0 |
| SKU theo dõi sát có baseline `ok` | 5.929 / 6.056 (127 thiếu: SKU mới xuất hiện sau 06/11) |
| `entity_resolution` | 11.954 sản phẩm, ~1 phút 45 giây (embedding ~85 giây CPU) |
| Qdrant: 11.954 truy vấn top-10 có lọc | chế độ cục bộ (Python) 687 giây; server Docker vài giây |
| Cặp ứng viên Qdrant / vượt ngưỡng 0,85 / qua mọi luật | 103.785 / 12.311 / 1.085 |
| Sản phẩm khớp với ≥ 1 sàn khác | 8,0% tổng; điện thoại 55%, tablet 34%, laptop 25%, đồng hồ 19%, phụ kiện 1,8% |
| Cụm theo số sàn | 2 sàn: 279, 3 sàn: 81, 4 sàn: 29, 5 sàn: 8 |
| Ước lượng precision bằng mắt (40 cặp ngẫu nhiên đã ghép) | 40/40 đúng. **Số chính thức: `labeling/report.md` sau khi nhóm gán nhãn** |
| `daily_stats` (15 ngày) | 157.069 dòng `price_batch_daily`, 12.977 `daily_price_stats`, 4.861 `daily_price_spread`, 23 giây |

## 6. Chỗ khác kế hoạch và lỗi còn biết

- **Airflow cài thẳng trong WSL** (venv, SQLite, `airflow standalone`), không chạy trong Docker Compose + PostgreSQL như
  kế hoạch: nhẹ RAM, task chỉ cần gọi `spark/submit.sh`. Một DAG chạy tuần tự nên SQLite đủ.
- **`compact_bronze` không thay file trong `/datalake/bronze`**: Bronze do file sink của Structured Streaming ghi, danh
  sách file nằm trong `_spark_metadata`; thay file sẽ làm `rebuild_silver` lỗi, và README cấm sửa Bronze. Job ghi
  bản gộp (file ~128 MB, đã đối chiếu số dòng) sang `/datalake/bronze_compacted`. Số file nhỏ trong Bronze gốc vẫn tăng.
- **Precision chính thức chưa có**: cần nhóm gán nhãn `labeling/` rồi chạy `er_labeling evaluate`. Ngưỡng 0,85 hiện là tạm.
- **Thêm bảng `daily_price_spread`** (kế hoạch gộp chênh giá vào `daily_price_stats`): hai bảng khác đơn vị dòng
  (nhóm sàn-danh mục-thương hiệu và từng sản phẩm), tách ra cho dễ truy vấn.
- **`master_id` có thể đổi giữa các đêm** khi có sản phẩm mới hoặc đổi ngưỡng (băm từ `product_key` nhỏ nhất của cụm).
  Cảnh báo lưu lâu dài nên lưu `product_key`.
- **Chạy lại trên máy khác có thể lệch vài cặp** ở sát ngưỡng 0,6 (embedding tính trên CPU khác nhau lệch ở chữ số
  thập phân rất nhỏ); trên cùng một máy chạy lại ra đúng cụm cũ (Qdrant tìm chính xác, gom cụm có thứ tự cố định).
- **Ghép sản phẩm yếu ở phụ kiện và laptop không ghi mã máy**: tên phụ kiện tiki rất dài, ít trùng giữa sàn. Luật 2
  (rẻ hơn sàn khác) chủ yếu có tác dụng với điện thoại, tablet, laptop có mã.
- **fptshop không ghi dung lượng trong tên**: ghép với bản có giá gần nhất (lệch ≤ 15%), cụm vẫn giữ thống nhất
  dung lượng/RAM nên không gộp nhầm bản 256GB với 512GB qua fptshop.
- Silver có 99 dòng hoangha `name_raw = "FLASH SALE"` (nhãn khuyến mãi đè tên): Gold bỏ qua khi chọn tên. Nên sửa ở
  crawler hoặc `normalize.py`.
- Chưa có test Spark tự động cho `price_baseline`, `daily_stats` (đã đối chiếu tay với pandas). Test thuần: 68 test cho
  chuẩn hoá tên và luật ghép.
