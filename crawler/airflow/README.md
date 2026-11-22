# Airflow: DAG `nightly_batch`

Lịch chạy đêm của batch layer, thay cho cron `spark/nightly_batch.sh`:

```
check_cluster → compact_bronze → rebuild_silver → entity_resolution → price_baseline → daily_stats → export_for_team
```

- Chạy **00:30 UTC (07:30 giờ VN)** mỗi ngày, xử lý đúng ngày UTC vừa kết thúc (`{{ ds }}`).
- `catchup=True`, `max_active_runs=1`: cụm tắt mấy đêm thì khi bật lại Airflow tự chạy bù từng ngày, lần lượt.
- Mỗi task gọi `bash spark/submit.sh ...` (job chạy trong container driver riêng như mọi job khác), nên Airflow chỉ
  cần chạy trong WSL, dưới user có quyền `docker`.
- Mọi job ghi lại trọn ngày hoặc trọn bảng, nên thử lại (`retries=1`) hay chạy lại tay không sinh dữ liệu trùng.

Airflow cài thẳng trong WSL (venv riêng, SQLite, `airflow standalone`), không chạy trong Docker: nhẹ RAM hơn và không
phải cho container quyền điều khiển Docker. Đây là chỗ khác kế hoạch (kế hoạch ghi Docker Compose + PostgreSQL).

## Cài trên máy chính (một lần)

Cần trước: cụm đã chạy (`kafka`, `hdfs`, `spark`), profile `serving` đã bật, image Spark có thư viện Gold
(`SPARK_IMAGE=bigdata-spark:3.5.8-gold`, xem README-3 mục 2).

```bash
cd crawler
bash airflow/install.sh               # venv airflow/.venv, Airflow 2.10.5, tạo DB SQLite trong airflow/home/
bash airflow/run.sh                   # chạy thử ở foreground; UI http://localhost:8088
```

Tài khoản `admin`, mật khẩu trong `airflow/home/standalone_admin_password.txt`.

Chạy nền, tự bật khi WSL khởi động (systemd user service):

```bash
mkdir -p ~/.config/systemd/user
cp airflow/airflow.service ~/.config/systemd/user/
# sửa WorkingDirectory cho đúng đường dẫn crawler/ trên máy
systemctl --user daemon-reload && systemctl --user enable --now airflow
loginctl enable-linger $USER
journalctl --user -u airflow -f       # xem log
```

## Bật DAG

1. **Tắt cron cũ** (`crontab -e`, xoá hoặc comment dòng `nightly_batch.sh`), không thì hai lịch cùng dựng Silver.
2. Dựng sẵn các ngày trước `START` trong `dags/nightly_batch.py` (mặc định 22/11/2026) bằng tay, ví dụ:
   ```bash
   bash spark/submit.sh --mem 1536m jobs/rebuild_silver.py --date-from 2026-11-01 --date-to 2026-11-21
   bash spark/submit.sh --mem 3g --conf spark.driver.memory=2g jobs/entity_resolution.py \
       --qdrant-url http://qdrant:6333 --model-cache /opt/spark/work-dir/models
   bash spark/submit.sh --mem 1536m jobs/price_baseline.py
   bash spark/submit.sh --mem 1536m jobs/daily_stats.py --date-from 2026-11-01 --date-to 2026-11-21 \
       --clickhouse-url http://clickhouse:8123 --redis-url redis://redis:6379/0
   ```
3. Trong UI, bật (unpause) `nightly_batch`. Muốn chạy ngay một ngày: **Trigger DAG w/ config** hoặc
   `airflow dags trigger nightly_batch -e 2026-11-22T00:30:00+00:00` (sau `source airflow/env.sh`).

## Khi task lỗi

| Task lỗi | Thường do | Làm gì |
| --- | --- | --- |
| `check_cluster` | cụm hoặc `serving` đang tắt | bật lại (README mục 6), rồi **Clear** task trong UI |
| `entity_resolution` | Qdrant tắt; lần đầu không tải được model (cần Internet) | `docker start qdrant`; model lưu ở volume `spark-work`, tải 1 lần |
| `daily_stats` | ClickHouse tắt, sai mật khẩu trong `infra/.env` | kiểm tra `CLICKHOUSE_PASSWORD` |
| `export_for_team` | không ảnh hưởng dữ liệu đã dựng | chạy tay `bash spark/export_for_team.sh` |

Log từng task: UI → DAG → task → Logs, hoặc `airflow/home/logs/`.

## Tiêu chí "chạy thành công 3 đêm liên tiếp"

UI → `nightly_batch` → Grid: 3 cột (3 ngày) liên tiếp toàn xanh. Chụp màn hình này cho báo cáo.
