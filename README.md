# Big_data_Team6 — Hệ thống phát hiện bất thường giá và hàng giả đa sàn TMĐT

Bài tập lớn môn Lưu trữ và Xử lý Dữ liệu lớn. Thu thập giá sản phẩm từ 5 sàn TMĐT Việt Nam (Tiki, CellphoneS, Hoàng Hà, FPT Shop, Thế Giới Di Động), đưa qua Kafka vào data lake trên HDFS, làm sạch theo kiến trúc Medallion (Bronze → Silver → Gold), rồi phát hiện giá bất thường theo kiến trúc Lambda (batch layer + speed layer).

## 1. Kiến trúc

```
                         ┌────────────── máy chính (WSL) ──────────────┐
Crawler 5 sàn ──► Kafka ──► Bronze (HDFS) ──► Silver (Delta) ──► Gold (Delta)
   │  (tmux)       │       JSON gzip, thô     đã làm sạch          product_master, price_baseline
   ▼               │                                                    │
landing/ (JSONL,   │                                                    │  Batch layer
 bản dự phòng)     └──► Speed layer (Spark Streaming) ──► alerts.price  │  (Airflow mỗi đêm)
                                     │                                  │
                                     └────────► ClickHouse + Redis ◄────┘
                                                        │
                                                    Dashboard (Grafana)
```

| Tầng | Công nghệ | Việc | Trạng thái |
| --- | --- | --- | --- |
| Thu thập | Python (httpx), chạy trong tmux | Crawl giá 30–60 phút/lần ("hot"), crawl toàn bộ + trang chi tiết mỗi đêm | **Chạy liên tục từ 01/11/2026** |
| Hàng đợi | Kafka 3.9 (KRaft, 1 broker) | 5 topic: `raw.price`, `raw.product`, `raw.review`, `dlq.invalid`, `alerts.price` | **Chạy** |
| Bronze | HDFS 3.4 (1 NameNode, 2 DataNode, 2 bản sao) + Spark Structured Streaming | Lưu nguyên bản mọi event, exactly-once | **Chạy 24/7** |
| Silver | Delta Lake trên HDFS + Spark batch | Bỏ trùng, chuẩn hoá về một schema chung, gắn cờ chất lượng, tách bản ghi hỏng | **Tự cập nhật 07:30 sáng mỗi ngày** |
| Gold (thành viên 2) | Delta Lake + Spark, Airflow | `product_master` (chuẩn hoá tên, ghép cùng sản phẩm giữa các sàn), `price_baseline` | Tuần 3, chưa bắt đầu |
| Speed layer | Spark Streaming đọc Kafka | Phát hiện giá bất thường gần thời gian thực, ghi `alerts.price` | Tuần 4 |
| Phục vụ | ClickHouse, Redis, Grafana | Truy vấn, cache, dashboard | Sau, chạy trên máy thành viên khác |

Hai nguyên tắc xuyên suốt, tầng nào cũng phải giữ:

- **Mọi job chạy lại được.** Chạy 1 lần hay 5 lần trên cùng dữ liệu phải ra cùng kết quả. Nhờ vậy sập giữa chừng chỉ cần chạy lại.
- **Mọi mốc thời gian là UTC.** Ngày UTC kết thúc lúc 07:00 sáng giờ VN. Chỉ đổi sang giờ VN khi hiển thị.

## 2. Tiến độ và tài liệu

| Giai đoạn | Nội dung | Tài liệu | Trạng thái |
| --- | --- | --- | --- |
| 1 (tuần 1) | Crawler 5 sàn, ghi JSONL vào `landing/` | [`crawler/README.md`](crawler/README.md) | Xong 08/11/2026 |
| 2 (tuần 2) | Kafka, Bronze trên HDFS, Silver Delta, lịch chạy đêm | [`crawler/README-2A.md`](crawler/README-2A.md), [`crawler/docs/bronze_silver_schema.md`](crawler/docs/bronze_silver_schema.md) | Xong 10/11/2026 |
| 3 (tuần 3) | Gold + batch layer, chuyển lịch chạy đêm sang Airflow | | Chưa bắt đầu |
| 4 (tuần 4) | Speed layer, `alerts.price` | | Chưa bắt đầu |
| 5 (tuần 5) | Dashboard, báo cáo chất lượng dữ liệu, kiểm thử tải | | Chưa bắt đầu |

## 3. Dữ liệu đang có (ngày 10/11/2026)

| Bảng | Vị trí | Số dòng | Ghi chú |
| --- | --- | --- | --- |
| `landing/` | máy chính, `crawler/landing/` | ~2,1 triệu event | Bản gốc, khoảng 200 MB/ngày |
| Bronze | HDFS `/datalake/bronze/<price\|product>/platform=/date=/hour=` | 2,1 triệu price + 6.822 product | JSON gzip, giữ thô, có thể có bản trùng |
| **Silver price** | HDFS `/datalake/silver/price` (partition `date`, `platform`) | **2.096.994** (01–09/11) | Mỗi dòng = 1 lần quan sát giá của 1 SKU |
| **Silver product** | HDFS `/datalake/silver/product` (partition `platform`) | **6.822** | Thông tin sản phẩm từ trang chi tiết, giữ mọi lần quan sát |
| Quarantine | HDFS `/datalake/quarantine/<price\|product>` | 0 | Bản ghi hỏng, kèm lý do và dòng gốc |
| Silver mẫu | `crawler/datalake_local/silver_sample/` | 835.116 + 2.245 (05–07/11) | Cùng schema với Silver thật, dựng thẳng từ `landing/` |

- **Schema từng cột, ý nghĩa cờ chất lượng và các lưu ý khi dùng:** [`crawler/docs/bronze_silver_schema.md`](crawler/docs/bronze_silver_schema.md). Người làm tầng sau **bắt buộc đọc**.
- **Khoảng trống dữ liệu:** có những giờ không có dữ liệu, do máy tắt, WSL khởi động lại hoặc sàn chặn. Các khoảng này ghi trong bảng `crawl_runs` (`crawler/state/crawler.db`), xem nhanh bằng `python -m jobs.status --hours 24`. Khi tính trung bình giá, đừng coi khoảng trống là "giá không đổi".
- Cột `ingest_source`: `replay` là dữ liệu trước 15:28 UTC ngày 09/11, được nạp lại từ `landing/`; `live` là dữ liệu crawler gửi thẳng vào Kafka sau mốc đó.

## 4. Bàn giao cho tầng Gold (thành viên 2, tuần 3) và các tầng sau

### 4.1. Đầu vào của Gold

- Gold (thành viên 2) **chỉ đọc Silver** (`/datalake/silver/price`, `/datalake/silver/product`). Không đọc Bronze hay `landing/`.
- Luôn đọc **dưới dạng Delta**, không đọc thẳng file `.parquet`. Mỗi lần Silver chạy lại, file cũ được Delta đánh dấu xoá nhưng vẫn còn trên đĩa; đọc Parquet thô sẽ ra dòng trùng.
- Silver đã làm những việc sau, Gold không cần làm lại:
  - bỏ trùng theo `event_id`;
  - ép kiểu (giá là số nguyên VND, thời gian là timestamp UTC);
  - tính `discount_pct`;
  - gắn `dq_flags` (`MISSING_PRICE`, `ZERO_PRICE`);
  - đưa bản ghi hỏng ra quarantine.
- Silver **chưa** làm, đây là việc của Gold:
  - Chuẩn hoá tên sản phẩm (`name_raw` còn nguyên chữ gốc của sàn).
  - Ghép cùng một sản phẩm giữa các sàn (cùng một iPhone ở 5 sàn đang có 5 `product_key` khác nhau). Đây là `product_master`.
  - Tính giá tham chiếu theo sản phẩm và thời gian (`price_baseline`). Khi tính, lọc `price_current IS NOT NULL`.
- `product_key = "<platform>:<product_id>"` dùng để nối `silver_price` với `silver_product`. Đây cũng là key của message trong Kafka.

### 4.2. Cách lấy dữ liệu để phát triển

HDFS chỉ mở cổng cho máy chính, máy khác không nối thẳng vào được. Thành viên khác làm việc trên **bản sao** do máy chính tự đóng gói mỗi sáng (mục 7) và đưa lên Drive của nhóm:

**Drive dữ liệu: https://drive.google.com/drive/folders/1Sr4t0evhgkbKsQN8cgKpmJM1x1eazA6G**

**Bước 1. Tải gói mới nhất trên Drive:** `data_YYYYMMDD.tar.gz` (YYYYMMDD là ngày đóng gói, ví dụ `data_20261111.tar.gz`, khoảng 150 MB). Một gói có đủ phần cho mọi người:

| Thư mục trong gói | Cho ai | Nội dung |
| --- | --- | --- |
| `silver/` | Thành viên 2 (Gold), ai cần phân tích | **Nguyên bảng** Silver `price/` và `product/` (Delta), dữ liệu từ 01/11 tới hết ngày trước ngày đóng gói. Chỉ cần gói mới nhất |
| `landing/` | Người làm speed layer | File JSONL gốc của crawler, **chỉ những ngày mới** kể từ gói trước. Cần giữ đủ các gói để có đủ các ngày |
| `MANIFEST.txt` | Mọi người | Số dòng từng bảng, từng ngày, để kiểm tra |

Dữ liệu ngày 01–09/11 gửi lần đầu theo kiểu cũ, là các file riêng `silver_20261110.tar.gz` và `landing_2026-11-DD.tar.gz` (cách dùng ở cuối mục 7).

**Bước 2. Giải nén.** File `.tar.gz` là thư mục được nén. Giải nén bằng lệnh `tar` (có sẵn trên WSL/Linux/macOS và Windows 10 trở lên):

```bash
# WSL / Linux / macOS
mkdir -p ~/data
tar xzf data_20261111.tar.gz -C ~/data
```

```powershell
# Windows (PowerShell hoặc cmd), thư mục đích phải có sẵn
mkdir D:\data
tar -xzf data_20261111.tar.gz -C D:\data
```

Kết quả:

```
data/data_20261111/
├── MANIFEST.txt
├── silver/
│   ├── price/                 # bảng silver_price
│   │   ├── _delta_log/        # nhật ký Delta, BẮT BUỘC giữ nguyên
│   │   └── date=2026-11-01/platform=tiki/part-*.parquet ...
│   └── product/               # bảng silver_product
│       ├── _delta_log/
│       └── platform=tiki/part-*.parquet ...
└── landing/
    ├── price/platform=tiki/date=2026-11-10/hour=00/*.jsonl ...
    └── product/...
```

Lưu ý khi giải nén:
- Giữ nguyên cấu trúc thư mục. **Không** đổi tên, xoá hay chép riêng file nào trong `silver/price/`, `silver/product/`, nhất là `_delta_log/`. Thiếu nhật ký thì bảng không đọc được, hoặc đọc ra sai.
- Không giải nén bằng cách mở file trong File Explorer rồi kéo thả từng phần. Dùng lệnh `tar` ở trên.
- Gói mới về thì **xoá cả thư mục gói cũ** rồi giải nén gói mới, không giải nén đè. Sau đó đổi đường dẫn trong code sang thư mục mới. Người làm speed layer chép phần `landing/` ra trước khi xoá (mục 7).

**Bước 3. Kiểm tra đã giải nén đúng.** Mở `MANIFEST.txt` xem số dòng, rồi đếm lại:

```python
from deltalake import DeltaTable
for t in ("price", "product"):
    print(t, DeltaTable(f"data/data_20261111/silver/{t}").to_pyarrow_table().num_rows)
```

**Bước 4. Đọc dữ liệu.**

```python
# Không cần Spark: pip install deltalake pyarrow pandas
from datetime import date
from deltalake import DeltaTable

price = DeltaTable("data/data_20261111/silver/price")
df = price.to_pandas(filters=[("date", "=", date(2026, 11, 9)), ("platform", "=", "tiki")])  # date là kiểu date, không phải chuỗi
product = DeltaTable("data/data_20261111/silver/product").to_pandas()
```

```python
# Spark 3.5 + package io.delta:delta-spark_2.12:3.3.2
df = spark.read.format("delta").load("data/data_20261111/silver/price").where("date = '2026-11-09'")
```

Chưa có bản sao thì dùng ngay Silver mẫu trong `crawler/datalake_local/silver_sample/` (ngày 05–07/11, cùng schema). Code viết trên bản mẫu hay bản sao, sang Silver thật **chỉ đổi đường dẫn**.

### 4.3. Đưa job lên chạy thật

- Viết job nhận đường dẫn đầu vào và đầu ra làm tham số (`--silver-root`, `--gold-root`…), không viết cứng.
- Đẩy code lên repo. Người giữ máy chính chạy trên cụm bằng `bash spark/submit.sh <script.py> ...`. Script này chỉ mount thư mục `crawler/` vào container, nên job Spark cần nằm trong `crawler/` (ví dụ `crawler/jobs/`). Muốn để chỗ khác thì báo để sửa `submit.sh`.
- Gold đề xuất ghi ra `/datalake/gold/<tên bảng>` dạng Delta.
- Lịch chạy đêm hiện là cron (`crawler/spark/nightly_batch.sh`). Tuần 3 chuyển sang Airflow DAG `nightly_batch`: Silver chạy trước, Gold chạy sau.
- Muốn làm sạch thêm gì thì sửa trong `crawler/common/normalize.py` (có test), **không viết logic làm sạch thứ hai ở chỗ khác**. Batch và streaming phải cho cùng kết quả.

### 4.4. Ghi chú cho speed layer (tuần 4)

- Đọc topic `raw.price` (6 partition, giữ 3 ngày). Key là `product_key`, nên mọi event của cùng một sản phẩm nằm trong cùng partition và giữ đúng thứ tự thời gian.
- Header `ingest_source` cho biết event là `live` hay `replay`.
- Dùng lại `common/normalize.py` để làm sạch từng event, giống batch.
- Ghi cảnh báo vào topic `alerts.price` (đã tạo sẵn: 3 partition, giữ 7 ngày).
- Speed layer cần Kafka **trực tiếp**, bản sao file không thay được. Khi phát triển trên máy riêng: tự dựng Kafka (mục 5.2, chỉ cần bước Kafka), lấy `landing/` trong gói dữ liệu hằng ngày (mục 4.2, mục 7), rồi bơm vào bằng `replay_producer --rate`. Chạy thật thì chạy trên máy chính, hoặc nối mạng tới Kafka của máy chính (nhóm cần chốt).
- Tuần 5 kiểm thử tải cũng bằng `replay_producer --rate <số event/giây>`.

## 5. Cài đặt

Môi trường đang dùng: Windows + WSL2 Ubuntu (bật systemd), Python 3.11, Docker Engine cài trong Ubuntu (không dùng Docker Desktop).

```bash
git clone https://github.com/Duc1203latte/Big_data_Team6.git
cd Big_data_Team6/crawler
```

### 5.1. Chỉ cần đọc dữ liệu (làm Gold, phân tích)

```bash
python3 --version                  # cần 3.11 trở lên
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt    # có sẵn deltalake, pyarrow, pandas
```

Thế là đọc được Silver mẫu và bản sao Silver (mục 4.2). Muốn chạy thử job Spark trên máy mình thì cài thêm theo mục 5.2, chỉ phần Spark.

### 5.2. Dựng toàn bộ hệ thống

Cần khoảng **8–10 GB RAM** cho WSL: Kafka 1 GB, HDFS khoảng 2 GB, Spark khoảng 3,5 GB, mỗi job Spark thêm 1–1,5 GB, cộng crawler.

**a) WSL.** Bật systemd trong `/etc/wsl.conf`:

```ini
[boot]
systemd=true
```

Giới hạn RAM của WSL trong `C:\Users\<tên>\.wslconfig` (phía Windows), để Docker không ăn hết RAM của máy:

```ini
[wsl2]
memory=10GB
swap=4GB
```

Sửa xong thì `wsl --shutdown` trong PowerShell rồi mở lại.

**b) Docker Engine** trong Ubuntu, theo https://docs.docker.com/engine/install/ubuntu/ (thêm apt repo của Docker rồi `sudo apt install docker-ce docker-ce-cli containerd.io docker-compose-plugin`). Sau đó cho user chạy docker không cần sudo:

```bash
sudo usermod -aG docker $USER      # đăng xuất rồi vào lại WSL
docker run --rm hello-world
```

**c) Python và cấu hình crawler:**

```bash
cd crawler
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env               # sửa SINK=file,kafka khi đã có Kafka; CONTACT_EMAIL để trống hoặc email nhóm
pytest -q                          # không gọi sàn; test cần Kafka tự bỏ qua khi chưa có broker
```

**d) Kafka, HDFS, Spark.** Bật theo thứ tự, đợt sau chỉ bật khi đợt trước đã chạy ổn:

```bash
cp infra/.env.example infra/.env
cd infra
docker compose --profile kafka up -d
bash kafka/create_topics.sh                    # tạo 5 topic, chạy lại không sao
docker compose --profile hdfs up -d
docker exec namenode hdfs dfsadmin -safemode wait
bash hdfs/init_dirs.sh                         # tạo /datalake/{bronze,silver,quarantine}, /checkpoints
docker compose --profile spark up -d
cd ..
```

Kiểm tra: HDFS http://localhost:9870 (2 DataNode sống), Spark http://localhost:8080 (1 worker).

**e) Bronze chạy 24/7:**

```bash
bash spark/submit.sh --name kafka-to-bronze --detach --conf spark.cores.max=1 jobs/kafka_to_bronze.py
docker logs -f kafka-to-bronze                 # mỗi 5 phút một dòng "Batch N ... rows=..."
```

**f) Crawler** chạy native trong tmux, không đưa vào Docker:

```bash
tmux new -s crawler
source .venv/bin/activate
python -m jobs.crawl_hot --platform tiki --platform hoangha --platform fptshop --platform cellphones --platform tgdd
# Ctrl+B rồi C mở cửa sổ mới:
python -m jobs.crawl_full --daily-at 17:00 --platform tiki --platform hoangha --platform fptshop --platform cellphones --platform tgdd
# Ctrl+B rồi D để thoát ra, crawler vẫn chạy
```

Trong terminal của VS Code, Ctrl+B có thể bị VS Code chặn: muốn thoát thì đóng tab terminal, **không bấm Ctrl+C** (sẽ dừng crawler).

`--daily-at 17:00` là giờ UTC, tức 00:00 giờ VN.

**g) Silver và lịch chạy đêm:**

```bash
bash spark/submit.sh --mem 1536m jobs/rebuild_silver.py --date-from 2026-11-01 --date-to 2026-11-09   # dựng lần đầu
crontab -e
# thêm dòng (sửa đường dẫn cho đúng máy):
# 30 7 * * * /đường/dẫn/crawler/spark/nightly_batch.sh >> /đường/dẫn/crawler/logs/nightly_batch.log 2>&1
```

Có sẵn `landing/` cũ (ví dụ chép từ máy khác) thì nạp vào Kafka bằng `replay_producer` trước khi dựng Silver (mục 7.2).

## 6. Vận hành máy chính

| Việc | Lệnh (trong `crawler/`) |
| --- | --- |
| Tình trạng crawler | `python -m jobs.status --hours 24` |
| Bronze có chạy không | `docker logs --tail 5 kafka-to-bronze` |
| Lịch chạy đêm | `tail logs/nightly_batch.log`, ngày đã dựng xong: `cat state/nightly_batch_last_day.txt` |
| Dựng lại Silver tay | `bash spark/submit.sh --mem 1536m jobs/rebuild_silver.py --date 2026-11-09` |
| Đóng gói dữ liệu gửi nhóm | tự chạy sau lịch đêm; chạy tay: `bash spark/export_for_team.sh` (mục 7) |
| Message crawler gửi Kafka bị lỗi | `python -m jobs.replay_producer --failed` (gửi lại `state/kafka_failed/`) |

**Tắt cụm để lấy RAM** (crawler và Kafka vẫn chạy):

```bash
docker stop kafka-to-bronze
cd infra && docker compose stop spark-master spark-worker namenode datanode1 datanode2
```

**Bật lại:**

```bash
cd infra && docker compose --profile hdfs --profile spark up -d
docker exec namenode hdfs dfsadmin -safemode wait
docker start kafka-to-bronze                   # đọc tiếp từ checkpoint, không mất, không trùng
```

Kafka giữ dữ liệu 3 ngày, nên tắt cụm dưới 3 ngày thì Bronze tự đuổi kịp khi bật lại. Hôm nào 07:30 cụm đang tắt, lịch chạy đêm sẽ bỏ qua và tự làm bù vào lần sau.

**Không được làm:**
- xoá `/checkpoints` trên HDFS (Bronze sẽ đọc lại Kafka từ đầu, nạp trùng);
- sửa hay xoá `landing/` và `/datalake/bronze`;
- commit `landing/`, `state/`, `logs/`, `.env`, `exports/`;
- `wsl --shutdown` khi crawler đang chạy.

## 7. Cập nhật dữ liệu cho thành viên khác

**Drive dữ liệu: https://drive.google.com/drive/folders/1Sr4t0evhgkbKsQN8cgKpmJM1x1eazA6G**

### 7.1. Máy chính: tự đóng gói mỗi sáng

Sau khi dựng Silver lúc 07:30, `spark/nightly_batch.sh` tự gọi `spark/export_for_team.sh` để đóng gói **một file** `crawler/exports/data_YYYYMMDD.tar.gz` (khoảng 150 MB, xong khoảng 07:33). Gói gồm:

- `silver/`: nguyên bảng Silver. Bảng Delta gồm file dữ liệu cộng nhật ký `_delta_log`, chép ghép từng phần sẽ hỏng bảng, nên luôn gửi nguyên bảng.
- `landing/`: những ngày UTC đã xong mà chưa gửi lần trước. Ngày đã gửi ghi trong `state/export_last_landing_day.txt`.
- `MANIFEST.txt`: số dòng để người nhận đối chiếu.

Thư mục `exports/` chỉ giữ 3 gói mới nhất, gói cũ hơn tự xoá.

**Việc của người giữ máy chính mỗi ngày:** kiểm tra log rồi tải gói lên Drive.

```bash
cd crawler
tail -2 logs/nightly_batch.log      # dòng cuối phải là "export_for_team: done exports/data_YYYYMMDD.tar.gz ..."
```

Tải file lên thư mục Drive ở trên. Từ Windows, mở thư mục gói ở `\\wsl$\Ubuntu\home\<user>\projects\bigdata-crawler\crawler\exports`. Gói trên Drive cũ quá thì xoá bớt, nhưng **giữ các gói còn `landing/` mà người làm speed layer chưa lấy**.

Hôm nào lịch tự động không chạy (cụm tắt lúc 07:30), sau khi bật cụm thì chạy tay:

```bash
bash spark/nightly_batch.sh         # dựng bù Silver rồi tự đóng gói
bash spark/export_for_team.sh       # hoặc chỉ đóng gói lại, khi Silver đã xong
```

### 7.2. Người nhận

**Thành viên 2 (Gold):** chỉ cần gói mới nhất. Làm theo mục 4.2.

**Người làm speed layer:** mỗi gói chỉ có `landing/` của các ngày mới, nên phải gom dần vào thư mục `crawler/landing/` của repo trên máy mình. Kafka đã dựng theo mục 5.2 (chỉ phần Kafka). Ví dụ:

```bash
cd crawler                                              # thư mục crawler/ của repo
tar xzf ~/Downloads/data_20261111.tar.gz -C /tmp
cp -r /tmp/data_20261111/landing/. landing/             # gom vào landing/, các ngày không đè nhau
cat /tmp/data_20261111/MANIFEST.txt                     # xem gói có những ngày nào
python -m jobs.replay_producer --date-from 2026-11-10 --date-to 2026-11-10 --rate 50
```

- `replay_producer` giữ nguyên `event_id` và `event_time`, gắn `ingest_source=replay`.
- Nó nhớ file đã gửi (`state/replay_progress.json`), nên gom thêm ngày mới rồi chạy lại chỉ gửi phần mới.
- `--rate` chỉnh tốc độ để giả lập luồng chậm hay dồn dập.
- Bỏ qua cảnh báo "No --until given": máy không có crawler chạy thật nên không bị trùng.

**Gói gửi lần đầu (01–09/11, kiểu cũ):**
- `silver_20261110.tar.gz` giải nén ra `silver_20261110/price` và `silver_20261110/product`, đọc như mục 4.2.
- `landing_2026-11-DD.tar.gz` giải nén **ngay trong thư mục `crawler/`** là file tự vào `landing/`.

**Chỉ một crawler chạy thật, là crawler của máy chính.** Thành viên khác không tự crawl song song: gấp đôi request dễ bị sàn chặn, và hai bộ dữ liệu crawl cùng giờ sẽ thành các quan sát trùng nhau với `event_id` khác nhau.
