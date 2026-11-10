# Giai đoạn 2 — Kafka, Bronze và Silver

Spec cho giai đoạn 2 của dự án Lưu trữ và Xử lý Dữ liệu lớn, tiếp nối `README.md` (giai đoạn 1, crawler). Toàn bộ code giai đoạn 2 nằm trong thư mục `crawler/` này, dùng chung `common/` và `.venv` với crawler.

> **Trạng thái: xong ngày 10/11/2026.** Phần dưới là spec viết trước khi làm, giữ nguyên để đối chiếu. Những chỗ làm thực tế khác spec được ghi chú ngay tại chỗ (đánh dấu **Thực tế:**). Kết quả và số liệu đã kiểm chứng ở **mục 14**. Hướng dẫn cài đặt, vận hành và gửi dữ liệu cho nhóm ở `README.md` gốc của repo.

## Quy ước code

Comment chi tiết bằng tiếng Việt. Tên biến, tên hàm, log message bằng tiếng Anh. **Tên trường luôn theo `common/schema.py`** (ví dụ `event_time`, `rating_avg`), không tự đặt tên mới cho cùng một thứ.

---

## 1. Hiện trạng

Giai đoạn 1 đã xong (đạt đủ 7 tiêu chí, xem `README.md` mục 9) và đang chạy:

- Crawler chạy native trong WSL qua `tmux`, **5 sàn** (tiki, cellphones, hoangha, fptshop, tgdd), liên tục từ 01/11/2026. Repo có thêm `platforms/lazada.py` nhưng máy này không chạy Lazada.
- Ngày 08/11: **~1,67 triệu price event + ~5.400 product event** trong `landing/` dạng JSONL, partition `<event_type>/platform=/date=/hour=` theo **UTC**. Tăng ~280.000 event/ngày, nên lúc replay (mục 8) sẽ vào khoảng 2,5–3 triệu.
- `event_id` sinh deterministic bằng uuid5, nên replay bao nhiêu lần cũng dedup được.
- Bảng `crawl_runs` trong SQLite (`state/crawler.db`) ghi lại các đợt chạy và khoảng trống.
- Sink đã chọn được qua biến `SINK` trong `.env` (`common/sink.py`, hàm `make_sink`). Giai đoạn 2 chỉ thêm loại sink mới vào đó.

Giai đoạn 2 làm hai phần:

- **2A:** mọi event từ crawler đi qua Kafka và được lưu nguyên bản lên HDFS Bronze. Dữ liệu cũ trong `landing/` cũng được nạp vào bằng replay producer.
- **2B:** làm sạch Bronze thành Silver: một schema chung cho mọi sàn, chạy lại nhiều lần vẫn cho cùng kết quả.

Vị trí trong kiến trúc tổng thể:

```
Crawler  →  Kafka  →  Bronze (HDFS)  →  Silver (Delta)  →  Gold
   ✓         2A           2A                 2B          tuần 3
```

Tuần 3 (thành viên 2) làm Gold và batch layer, đang bị chặn vì chưa có Silver. Mục 10.4 có cách tháo chặn.

## 2. Nguyên tắc thiết kế

**1. Không sửa `platforms/`.** Đổi crawler sang Kafka chỉ được thêm `KafkaSink` vào `common/sink.py` và sửa `.env`. Nếu phải sửa parser, tức thiết kế giai đoạn 1 sai, báo lại đừng tự chữa.

**2. Bronze là bất biến và thô.** Bronze lưu nguyên payload gốc, chỉ thêm metadata nạp (`ingest_time`, `kafka_partition`, `kafka_offset`). Không làm sạch, không ép kiểu, không loại bản ghi lỗi ở tầng này. Bronze cho phép trùng (ví dụ replay 2 lần); dedup là việc của Silver.

**3. Mọi job phải chạy lại được.** Chạy `rebuild_silver` 1 lần hay 5 lần trên cùng ngày phải ra kết quả giống nhau. Cơ chế: dedup theo `event_id` và `MERGE` thay vì `INSERT`.

**4. Normalize viết một lần, dùng hai nơi.** `common/normalize.py` dùng cho cả batch `rebuild_silver` (2B) và streaming ở tuần 4. Không được viết hai bản logic song song, lệch nhau là sai số báo cáo không giải thích được.

**5. Ghi song song khi chuyển sink.** Khi crawler chuyển sang Kafka, vẫn giữ `FileSink` ghi ra `landing/` (`SINK=file,kafka`). Tốn vài trăm MB/ngày nhưng không mất dữ liệu nếu Kafka hoặc Bronze có vấn đề.

**6. Payload giữ nguyên từng byte.** Message Kafka chính là một dòng JSONL (cùng cách serialize với `FileSink`). Replay gửi nguyên dòng đọc từ file, không parse rồi serialize lại, để `event_id`, `event_time` và mọi trường không thể bị đổi.

## 3. Ràng buộc môi trường (quan trọng, đọc kỹ)

Máy: Intel Core i9-13900H, 16 GB RAM (WSL thấy ~15 GB), Windows + WSL2 (Ubuntu, **đã bật systemd**). Crawler đang chạy trên đó. Toàn bộ stack không bật cùng lúc được.

### Dựng theo đợt

| Đợt | Thành phần | RAM dự kiến | Bật khi nào |
| --- | --- | --- | --- |
| 1 | Kafka 1 broker (KRaft) | ~1 GB | bước 3 |
| 2 | HDFS 1 NameNode + 2 DataNode | ~2 GB | bước 5 |
| 3 | Spark 1 master + 1 worker | ~3,5 GB | bước 7 |
| 4 | ClickHouse, Redis, Grafana, Airflow | ~4 GB | **không thuộc giai đoạn 2**, đẩy sang máy thành viên khác |

Đợt 1 đến 3 khoảng 6,5 GB. Đó là trần, không được vượt.

### Bắt buộc

- Mỗi service trong `docker-compose.yml` phải có `deploy.resources.limits.memory`. Không service nào được để không giới hạn.
- Crawler **không** đưa vào Docker. Nó tiếp tục chạy native trong tmux.
- `.wslconfig` phải giới hạn RAM cho WSL, để Docker không bao giờ ăn tới mức OOM killer giết crawler.
- Airflow hoãn sang tuần 3. Giai đoạn 2 dùng cron. Trong báo cáo vẫn có Airflow, chỉ là lên sau.

### Docker

Máy chưa có Docker. Đề xuất **Docker Engine cài thẳng trong Ubuntu (WSL)** bằng apt, không dùng Docker Desktop: nhẹ hơn, không cần cài gì phía Windows, và vì WSL đã bật systemd nên cài xong chạy được ngay, **không phải khởi động lại WSL** (không giết crawler).

### Kafka trong Docker, client ở ngoài

Producer (crawler, replay) chạy trong WSL, còn Spark chạy trong container, nên Kafka cần **hai listener**:

- `localhost:9092` cho tiến trình chạy trong WSL (crawler, replay_producer, test)
- `kafka:29092` cho container khác trong cùng mạng Docker (Spark)

Thiếu listener thứ hai là lỗi kinh điển: Spark kết nối được lần đầu rồi bị trả về địa chỉ `localhost` của chính container.

### HDFS

HDFS trong Docker hay vỡ ở mấy chỗ: format NameNode nhiều lần, hostname không phân giải được giữa container, permission `/datalake`. Làm đúng thứ tự: NameNode format một lần duy nhất, dùng named volume chứ không bind mount, đặt `dfs.replication=2`, kiểm tra `hdfs dfsadmin -report` thấy đủ 2 DataNode mới đi tiếp.

**Thực tế:** `dfs.replication` là cấu hình **phía ghi**, đặt ở NameNode chưa đủ. Spark không đặt thì ghi mọi file với 3 bản sao, cụm 2 DataNode không bao giờ đủ nên HDFS báo thiếu bản sao mãi. Đã thêm `spark.hadoop.dfs.replication 2` vào `spark/conf/spark-defaults.conf` và hạ file cũ bằng `hdfs dfs -setrep -R 2 /datalake /checkpoints`.

Nếu HDFS ăn quá nhiều RAM hoặc dựng mãi không xong thì MinIO thay được (S3A, Spark và Delta đọc ghi bình thường, nhẹ hơn nhiều). Nhưng proposal và sơ đồ kiến trúc đã ghi HDFS nên phải thống nhất với nhóm trước khi đổi, và nếu đổi thì ghi rõ lý do để đưa vào báo cáo.

### Spark

Mặc định theo proposal: cluster 1 master + 1 worker trong Docker, job nộp bằng `spark/submit.sh` (chạy `spark-submit` trong container, code mount vào). Phương án dự phòng nếu thiếu RAM: chạy Spark **local mode** trong WSL (cần Java 17 và `pyspark`, nhẹ hơn ~1,5 GB, nhưng không có cluster để trình bày). Chốt ở bước 7.

**Thực tế:** chọn cluster (1 master + 1 worker, 4 core, 1,5 GB cho executor). Mỗi job chạy driver trong một container riêng do `spark/submit.sh` tạo, có giới hạn RAM riêng (`--mem`, mặc định 1 GB).

## 4. Bước 0 — Báo cáo môi trường trước khi code

Việc đầu tiên, không viết code: kiểm tra và báo lại các số sau.

```bash
# RAM và swap trong WSL
free -h
# Dung lượng đĩa còn lại
df -h /
# systemd có chạy không (cần cho Docker Engine)
ps -p 1 -o comm=
# Docker đã có chưa
docker --version && docker compose version
docker info | grep -i -E "memory|cpus"
# Crawler còn chạy không
tmux ls
pgrep -af "jobs.crawl"
# Dữ liệu landing đang có (đếm nhanh bằng export_csv: python -m jobs.export_csv rồi xem exports/summary.md)
du -sh landing/
find landing -name "*.jsonl" | wc -l
```

`.wslconfig` hiện chưa có. Nội dung đề xuất, tạo ở `C:\Users\PC\.wslconfig`:

```ini
[wsl2]
memory=10GB
processors=8
swap=4GB
```

`.wslconfig` chỉ có hiệu lực sau `wsl --shutdown`, mà lệnh này **giết crawler**. Chọn một lúc máy rảnh (sau khi `crawl_full` đêm xong, khoảng 4h sáng VN), dừng crawler đúng cách, shutdown WSL, rồi khởi động lại crawler theo `README.md` mục 3.5.

## 5. Cấu trúc thêm vào repo

Giữ nguyên cây thư mục giai đoạn 1 (`crawler/`), thêm:

```
crawler/
├── README-2A.md                    # file này
├── infra/
│   ├── docker-compose.yml          # tất cả service, dùng profile để bật theo đợt
│   ├── .env.example                # version image, cluster id, giới hạn RAM (.env thật không commit)
│   ├── kafka/
│   │   └── create_topics.sh
│   └── hdfs/
│       └── init_dirs.sh            # tạo /datalake, /checkpoints, set permission
├── common/
│   ├── sink.py                     # THÊM KafkaSink, MultiSink vào đây
│   └── normalize.py                # MỚI — dùng cho cả batch và streaming
├── jobs/
│   ├── replay_producer.py          # MỚI — landing/ -> Kafka
│   ├── kafka_to_bronze.py          # MỚI — Spark Structured Streaming
│   ├── rebuild_silver.py           # MỚI — Spark batch
│   └── build_silver_sample.py      # MỚI — tháo chặn thành viên 2, xem 10.4
├── spark/
│   ├── submit.sh                   # wrapper spark-submit, gắn sẵn package
│   ├── nightly_batch.sh            # THỰC TẾ — lịch chạy đêm (cron), xem 10.3
│   ├── smoke.py                    # THỰC TẾ — job thử cụm ở bước 7
│   └── conf/spark-defaults.conf
├── tests/
│   ├── fixtures/normalize/         # MỚI — dòng JSONL thật của từng sàn
│   ├── test_normalize.py           # MỚI
│   └── test_kafka_sink.py          # MỚI
└── docs/
    ├── api_limits.md               # có từ giai đoạn 1
    └── bronze_silver_schema.md     # MỚI — gửi thành viên 2
```

Dùng Docker Compose profile để bật theo đợt: `docker compose --profile kafka up -d`, rồi `--profile hdfs`, rồi `--profile spark`.

Thư viện mới (thêm vào `requirements.txt` đúng ở bước cần, sau khi nhóm đồng ý): `confluent-kafka` (bước 4), thư viện ghi Delta cho bước 2 (xem 10.4). Spark chạy trong container nên WSL không cần `pyspark` trừ khi chọn local mode.

## 6. Kafka

### 6.1. Topic

| Topic | Partition | Key | Retention | Cleanup | Ghi chú |
| --- | --- | --- | --- | --- | --- |
| `raw.price` | 6 | `product_key` | 3 ngày | delete | luồng chính |
| `raw.product` | 3 | `product_key` | — | **compact** | giữ bản mới nhất mỗi sản phẩm |
| `raw.review` | 3 | `product_key` | 7 ngày | delete | tạo sẵn; giai đoạn 1 không crawl review nên chưa có producer |
| `dlq.invalid` | 1 | `null` | 14 ngày | delete | message không parse được |
| `alerts.price` | 3 | `product_key` | 7 ngày | delete | tuần 4 mới dùng, tạo sẵn |

`product_key = "<platform>:<product_id>"` (đã có hàm `make_product_key` trong `common/ids.py`). Không dùng `product_id` trần vì hai sàn có thể trùng id.

`replication.factor=1` vì chỉ có 1 broker. Đây là giới hạn môi trường thử nghiệm, bù lại bằng Bronze trên HDFS giữ toàn bộ event, cộng `FileSink` vẫn ghi `landing/` — câu này cần có trong báo cáo.

Key là `product_key` để mọi event của cùng một sản phẩm vào cùng partition, nhờ đó thứ tự giá theo thời gian của từng sản phẩm được giữ. Tuần 4 phát hiện giá bất thường dựa vào thứ tự này, nên **không được để key null**.

### 6.2. Message

Dùng JSON, không dùng Avro. Lý do: nhẹ, không cần Schema Registry (thêm 1 service và ~400 MB RAM). Trade-off là không kiểm tra schema ở tầng broker, bù lại bằng pydantic validate ở producer (event được tạo từ model trong `common/schema.py`) và quarantine ở Silver. Ghi lý do này vào báo cáo.

```
key     : product_key (utf-8 string), ví dụ "tiki:278628866"
value   : đúng một dòng JSONL như FileSink ghi (orjson của model_dump(mode="json")), không có "\n"
headers : event_type, schema_version, crawler_version, ingest_source (live|replay)
```

Header `ingest_source` phân biệt event từ crawler thật và event từ replay, để lúc đo throughput ở tuần 5 không lẫn. **Không đặt tên là `source`**: `PriceEvent` đã có trường `source` (listing|detail) với nghĩa khác.

### 6.3. Producer

```python
{
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,     # bắt buộc
    "acks": "all",                  # bắt buộc
    "compression.type": "snappy",
    "linger.ms": 50,
    "batch.size": 65536,
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 120000,
}
```

Dùng `confluent-kafka` (binding của librdkafka), không dùng `kafka-python`: nhanh hơn nhiều và có idempotent producer thật.

```bash
pip install "confluent-kafka==2.*"
```

## 7. KafkaSink

Thêm vào `common/sink.py`, cùng interface `EventSink` của giai đoạn 1:

```python
class KafkaSink(EventSink):
    """Đẩy event vào Kafka. Topic suy ra từ event_type: price -> raw.price.

    - write(events): produce cả lô rồi flush(timeout). Đúng hợp đồng EventSink:
      write() trả về là dữ liệu đã nằm an toàn ở đích. Crawler chỉ ~0,2 request/giây,
      mỗi lô ~40 event, nên flush mỗi lô không làm chậm vòng crawl.
    - Message giao thất bại (delivery callback lỗi, hết timeout): log ERROR, ghi dòng JSONL
      ra state/kafka_failed/<topic>-<run_id>.jsonl để replay_producer gửi lại sau,
      không raise làm chết vòng crawl.
    - close(): flush lần cuối có timeout.
    - Đếm số message thành công / thất bại theo topic, log ở close().
      crawl_runs.events_count giữ nguyên ý nghĩa (số event crawl được), không đổi schema.
    """
```

Và một `MultiSink` để ghi song song (nguyên tắc 5):

```python
class MultiSink(EventSink):
    """Ghi vào nhiều sink theo thứ tự. Một sink lỗi không được làm sink khác dừng:
    bắt lỗi từng sink, log ERROR, ghi tiếp sink sau."""
```

Chọn sink qua `.env` như giai đoạn 1, không thêm file config mới:

```
SINK=file,kafka                      # nhiều sink cách nhau dấu phẩy -> MultiSink
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

`make_sink` đọc danh sách này; `SINK=file` (mặc định) giữ nguyên hành vi giai đoạn 1.

Test:

- **Offline (chạy trong `pytest` thường):** thay producer thật bằng producer giả, kiểm tra topic, key, header, payload đúng từng byte, lỗi giao hàng thì ghi `state/kafka_failed/`, `MultiSink` vẫn ghi file khi Kafka lỗi.
- **Với broker thật (đánh dấu `@pytest.mark.kafka`, tự bỏ qua khi không có broker):** produce 100 event rồi consume lại đủ 100, `event_id` không trùng. Không dùng testcontainers (thêm thư viện, cần Docker trong test).

## 8. Replay producer

Nạp dữ liệu cũ trong `landing/` vào Kafka. Đây cũng là công cụ kiểm thử tải ở tuần 5, nên viết cho tử tế ngay từ đầu.

```
python -m jobs.replay_producer \
    --event-type all \                    # price | product | all
    --platform all \
    --date-from 2026-11-01 --date-to 2026-11-08 \
    --until 2026-11-10T03:00Z \           # mốc chuyển crawler sang MultiSink (bước 9)
    --rate 500 \                          # event/giây, 0 = nhanh nhất có thể
    --ingest-source replay \
    --dry-run                             # chỉ đếm, không produce
```

Yêu cầu:

- Đọc file theo thứ tự `date`, `hour` tăng dần, giữ đúng trình tự thời gian.
- **Gửi nguyên dòng JSONL** làm value (nguyên tắc 6). Chỉ parse để lấy `platform`, `product_id`, `event_type` làm key và header. Không sinh lại `event_id`, không đổi `event_time`.
- **Chỉ nạp file đã ghi xong:**
  - bỏ qua partition giờ `>= --until`. Sau mốc này crawler đã tự gửi vào Kafka qua `MultiSink`, nạp thêm là trùng (Silver vẫn dedup được nhưng làm lệch số liệu đối chiếu).
  - bỏ qua file của run còn `running` trong `crawl_runs` (tên file `part-<run_id>.jsonl` chứa run_id). File đó crawler vẫn đang ghi tiếp; nếu nạp và đánh dấu "xong" bây giờ thì phần ghi sau sẽ bị bỏ sót.
- Throttle đúng theo `--rate` bằng token bucket.
- Có checkpoint: ghi file đã nạp xong (đường dẫn + kích thước + số dòng) vào `state/replay_progress.json`, chạy lại thì bỏ qua file đã xong. File đã xong mà kích thước thay đổi thì báo lỗi, không tự nạp lại.
- In tiến độ mỗi 10 giây: đã nạp bao nhiêu, còn bao nhiêu, tốc độ thực tế.
- Có chế độ gửi lại `state/kafka_failed/` (mục 7).

**Thứ tự chạy:** bật `kafka_to_bronze` trước rồi mới replay, để đo được luôn độ trễ xử lý và không phụ thuộc retention 3 ngày của `raw.price`. Với `--rate 500`, 3 triệu event mất khoảng 1 giờ 40 phút.

## 9. kafka_to_bronze

Spark Structured Streaming, đọc `raw.price` và `raw.product`, ghi xuống Bronze. Mỗi topic một query riêng, mỗi query một checkpoint riêng.

### Phiên bản

Chốt cặp phiên bản rồi không đổi, lệch là vỡ:

| Thành phần | Phiên bản |
| --- | --- |
| Spark | 3.5.x (**thực tế: 3.5.8**, image `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`) |
| Scala | 2.12 |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.x` (**thực tế: 3.5.8**) |
| Delta | `io.delta:delta-spark_2.12` 3.2.x (dùng ở 2B) (**thực tế: 3.3.2**, bản hỗ trợ Spark 3.5) |
| Hadoop client | khớp với version HDFS trong compose |

Kiểm tra lại cặp Spark và Delta trên trang tương thích của Delta trước khi cài, đừng tin con số trong file này. Gắn package qua `--packages` trong `spark/submit.sh`, không tải jar tay.

### Logic

```python
# Đọc (Spark trong container nên dùng listener kafka:29092)
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "raw.price")              # query thứ hai: raw.product
      .option("startingOffsets", "earliest")         # lần đầu; sau đó checkpoint quyết định
      .option("maxOffsetsPerTrigger", 50000)         # chặn tải, tránh OOM
      .load())

value = col("value").cast("string")
event_time = get_json_object(value, "$.event_time")  # tên trường theo common/schema.py

# Thêm metadata nạp, GIỮ NGUYÊN payload gốc trong cột raw
out = df.select(
    value.alias("raw"),                               # payload thô, không parse
    col("key").cast("string").alias("kafka_key"),
    col("topic"), col("partition").alias("kafka_partition"), col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingest_time"),
    # chỉ parse vài trường để lấy cột partition, không parse cả event
    coalesce(get_json_object(value, "$.platform"), lit("_unknown")).alias("platform"),
    coalesce(substring(event_time, 1, 10), lit("_unknown")).alias("date"),
    coalesce(substring(event_time, 12, 2), lit("_unknown")).alias("hour"),
)
```

Cột partition `date` và `hour` lấy từ `event_time` (UTC, dạng `2026-11-08T07:49:05Z`), **không lấy từ `ingest_time`**. Lý do: replay dữ liệu cũ phải rơi vào đúng ngày gốc của nó, không dồn hết vào ngày chạy replay. Đây cũng chính là partition của `landing/`, nên đối chiếu từng giờ được.

Nếu `platform` hoặc `event_time` không đọc được thì vào `platform=_unknown/date=_unknown/`, không làm chết job.

**Thực tế** (`jobs/kafka_to_bronze.py`), khác đoạn code trên ở mấy chỗ:
- `date`/`hour` chỉ cắt khi `event_time` đúng khuôn `YYYY-MM-DDTHH`, nếu không thì `_unknown` (tránh partition rác).
- Thêm cột `ingest_source` (`live`/`replay`) đọc từ header Kafka (`includeHeaders=true`). Bronze ghi trước 10/11 không có cột này; Silver suy theo mốc chuyển sink.
- `repartition("platform", "date", "hour")` trước khi ghi: mỗi micro-batch chỉ ra 1 file cho mỗi partition Bronze.
- Khi đọc Bronze phải tắt suy kiểu partition (`hour=07` bị đọc thành số 7) hoặc khai báo schema; `rebuild_silver` khai báo schema.

### Ghi

```
/datalake/bronze/<event_type>/platform=<sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-*.json.gz
```

- Format JSON nén gzip (đúng proposal — Bronze giữ thô, Parquet để dành cho Silver).
- File sink của Structured Streaming (`format("json")`, `partitionBy("platform", "date", "hour")`), **không dùng `foreachBatch`**: file sink ghi exactly-once nhờ thư mục `_spark_metadata`, nên tắt bật lại không trùng.
- `trigger(processingTime="5 minutes")`, `outputMode("append")`.
- `checkpointLocation=/checkpoints/kafka_to_bronze/<event_type>` — **không bao giờ xoá**, xoá là mất vị trí đọc và sẽ nạp trùng.
- Đọc Bronze luôn đọc từ thư mục gốc `/datalake/bronze/<event_type>` (Spark dùng `_spark_metadata` để bỏ file rác của batch hỏng), lọc ngày bằng điều kiện partition, không trỏ thẳng vào thư mục con.

### Lưu ý

Bronze sẽ có bản ghi trùng nếu replay chạy 2 lần. Đó là chấp nhận được và đúng thiết kế: Bronze thô và append-only, Silver dedup theo `event_id`. Đừng cố chống trùng ở tầng này.

File nhỏ: mỗi 5 phút ghi tối đa 1 file cho mỗi sàn đang có dữ liệu, tức ~1.500 file/ngày cho `raw.price` — HDFS chịu được thoải mái trong 1 tháng. **Không gộp file tại chỗ** trong thư mục của file sink: `_spark_metadata` vẫn trỏ tới file cũ, gộp tại chỗ làm hỏng việc đọc. Nếu sau này số file làm chậm `rebuild_silver` thì gộp sang thư mục riêng (`/datalake/bronze_compacted/`), xem 10.3.

## 10. Phần 2B — Bronze thành Silver

### 10.1. `common/normalize.py`

Hàm thuần, không I/O, không Spark. Dùng được cả trong batch và streaming.

Crawler **đã** tách sẵn số và chữ: `price_current` là số nguyên VND, chữ gốc ở `price_current_raw`; `event_time` đã là UTC cắt tới giây. Vì vậy normalize chủ yếu kiểm tra và bổ sung, không parse lại giá từ đầu. Giá trị số của crawler là chuẩn; `parse_price_vnd` chỉ dùng khi `price_current` rỗng mà chữ gốc đọc được.

```python
def parse_price_vnd(raw: str | int | None) -> int | None:
    """'25.990.000₫', '25,990,000 VND', 25990000 -> 25990000.
    Trả None nếu không đọc được ('Liên hệ', 'lowPrice=...;highPrice=...' của Hoàng Hà). Không raise."""

def to_utc(ts: str | datetime | None) -> datetime | None:
    """Đưa về UTC có timezone. Chuỗi không có tz thì coi là UTC+7
    (giờ sàn Việt Nam) rồi đổi sang UTC. Dữ liệu crawler luôn có tz, hàm này để phòng."""

def compute_discount_pct(current: int | None, original: int | None) -> float | None:
    """None nếu thiếu một trong hai, hoặc original <= 0, hoặc current > original."""

def validate(event: dict) -> tuple[list[str], list[str]]:
    """Trả về (errors, flags).

    errors — bản ghi hỏng thật, đi vào quarantine:
    - MISSING_EVENT_ID
    - MISSING_PRODUCT_ID
    - BAD_TIMESTAMP
    - UNKNOWN_PLATFORM     (ngoài tiki, cellphones, hoangha, fptshop, tgdd, lazada)
    - PRICE_OUT_OF_RANGE   (có giá nhưng ngoài 1.000 đến 2.000.000.000 VND)

    flags — bản ghi hợp lệ nhưng thiếu dữ liệu, VẪN vào Silver, ghi vào cột dq_flags:
    - MISSING_PRICE        (sản phẩm "Liên hệ", chưa có giá; chữ gốc vẫn ở price_current_raw)
    - ZERO_PRICE           (sàn ghi giá 0: fptshop, cellphones, tgdd; Silver đổi giá thành null)
    """
```

Lý do tách flags khỏi errors: sản phẩm "Liên hệ" (0,5–3% event ở cellphones, hoangha) và giá 0 vẫn là quan sát thật: sản phẩm có trên sàn, chỉ chưa có giá. Bỏ vào quarantine sẽ làm Gold tưởng sản phẩm biến mất.

Test bắt buộc: lấy mẫu thật của **cả 5 sàn** từ `landing/` vào `tests/fixtures/normalize/`, mỗi sàn ít nhất 3 ca (bình thường, hết hàng hoặc thiếu trường, không có giá hoặc giá 0). Mỗi sàn có kiểu dữ liệu riêng, đây là chỗ dễ sai nhất của cả giai đoạn 2.

### 10.2. Schema Silver

Ghi ra `docs/bronze_silver_schema.md` và gửi thành viên 2 ngay khi chốt.

`silver_price` (Delta, partition theo `date`, `platform`):

| Cột | Kiểu | Ghi chú |
| --- | --- | --- |
| `event_id` | string | khoá dedup, MERGE theo cột này |
| `platform` | string | partition |
| `product_id` | string | id gốc của sàn |
| `product_key` | string | `platform:product_id` |
| `sku_id` | string | nullable |
| `url` | string | |
| `name_raw` | string | tên gốc, chưa chuẩn hoá |
| `source` | string | `listing` hoặc `detail` (trang lấy được giá) |
| `price_current` | bigint | VND, null nếu "Liên hệ" hoặc giá 0 |
| `price_current_raw` | string | chữ gốc, để kiểm tra |
| `price_original` | bigint | nullable |
| `discount_pct` | double | nullable, tính ở Silver |
| `currency` | string | `VND` |
| `in_stock` | boolean | nullable |
| `seller_id` | string | nullable, chỉ marketplace có (tiki) |
| `seller_name` | string | nullable |
| `rating_avg` | double | nullable |
| `rating_count` | int | nullable |
| `review_count` | int | nullable |
| `sold_count` | int | nullable |
| `event_time` | timestamp | UTC, thời điểm crawl |
| `ingest_time` | timestamp | từ Bronze |
| `ingest_source` | string | `live` hoặc `replay` |
| `date` | date | partition, lấy từ `event_time` |
| `crawler_version` | string | |
| `dq_flags` | array<string> | rỗng nếu đủ dữ liệu, xem 10.1 |

`silver_product` (Delta, partition theo `platform`): các trường của `ProductEvent` (`brand_raw`, `category_raw`, `specs_raw` dạng chuỗi JSON, `image_url`, `rating_avg`, `rating_count`, `rating_1_count`…`rating_5_count`) cộng `event_id`, `product_key`, `event_time`, `ingest_time`. MERGE theo `event_id`, giữ lịch sử; Gold tự lấy bản mới nhất theo `product_key`.

Bản ghi lỗi đi vào `/datalake/quarantine/<event_type>/date=/` kèm cột `dq_errors array<string>` và `raw string`. **Không bỏ đi.** Tuần 5 cần số liệu "bao nhiêu phần trăm bản ghi vào quarantine, vì lý do gì" cho phần data quality của báo cáo; tỉ lệ `dq_flags` cũng báo cáo cùng.

Chuẩn hoá tên sản phẩm và match sản phẩm giữa các sàn **không làm ở đây**. Đó là `product_master` ở tầng Gold, việc của tuần 3.

### 10.3. Các job

**`rebuild_silver.py`** (Spark batch):
```
python -m jobs.rebuild_silver --date 2026-11-08 --event-type price
python -m jobs.rebuild_silver --date-from 2026-11-01 --date-to 2026-11-08
```
Đọc Bronze theo ngày → parse JSON đầy đủ → áp `normalize` → `validate` → hợp lệ thì `MERGE` vào Silver theo `event_id`, lỗi thì ghi quarantine. Chạy lại cùng một ngày phải ra số bản ghi y hệt.

**Gộp file Bronze** (tuỳ chọn, chỉ làm nếu số file làm chậm `rebuild_silver`): gộp một ngày đã xong sang `/datalake/bronze_compacted/<event_type>/platform=/date=/`, file khoảng 128 MB, không đổi nội dung và số bản ghi; `rebuild_silver` đọc bản đã gộp nếu có. Không động vào thư mục của file sink (mục 9).

**Lịch chạy:** cron lúc **01:00 giờ VN (18:00 UTC)**, `rebuild_silver` cho **ngày UTC hôm trước** (partition theo UTC, ngày UTC kết thúc lúc 07:00 sáng VN). Chuyển sang Airflow DAG `nightly_batch` ở tuần 3. Lưu ý `crawl_full` chạy 00:00–04:00 giờ VN; nó chủ yếu chờ mạng nên chạy cùng Spark được, nhưng theo dõi RAM đêm đầu.

**Thực tế: chạy lúc 07:30 giờ VN (00:30 UTC)**, script `spark/nightly_batch.sh`. Lúc 01:00 VN vẫn là 18:00 UTC của ngày đang dở, "ngày UTC hôm trước" khi đó là D-1, nên Silver chậm khoảng 1 ngày rưỡi và còn chạy trùng `crawl_full`. Lúc 07:30 ngày UTC vừa kết thúc 30 phút (đủ để Bronze ghi nốt), Silver chỉ chậm khoảng 30 phút. Script còn:
- tự bù ngày lỡ: nhớ ngày cuối đã dựng ở `state/nightly_batch_last_day.txt`, lần sau chạy tiếp từ ngày kế tiếp;
- luôn làm lại thêm ngày hôm kia, để nhặt event vào Bronze muộn;
- không chạy, và không đánh dấu ngày đó là xong, nếu HDFS, Spark hoặc `kafka-to-bronze` đang tắt;
- cấp 1,5 GB cho driver (đo được đỉnh khoảng 990 MB khi để mặc định 1 GB).

### 10.4. Tháo chặn thành viên 2 (làm sớm)

`build_silver_sample.py`: đọc thẳng file JSONL trong `landing/` (không cần Kafka, HDFS hay Spark), áp `normalize`, ghi bảng Delta ra đường dẫn local `./datalake_local/silver_sample/price/`. Lấy 2 đến 3 ngày dữ liệu là đủ.

```
python -m jobs.build_silver_sample --date-from 2026-11-05 --date-to 2026-11-07
```

Ghi Delta không cần Spark bằng thư viện `deltalake` (delta-rs, cài bằng pip, không cần Java) cộng `pyarrow`. **Cần nhóm đồng ý thêm thư viện.** Nếu không thì ghi Parquet, thành viên 2 đọc bằng Spark như bình thường.

Mục đích: thành viên 2 có bảng Silver đúng schema để viết `product_master` và `price_baseline` ngay, không phải chờ 2A xong. Khi Silver thật lên thì job của thành viên 2 chỉ đổi đường dẫn đọc. Bảng mẫu có `ingest_time = null`, `ingest_source = "sample"`.

Job này dùng đúng `common/normalize.py`, không viết logic riêng.

## 11. Thứ tự thực hiện

Mỗi bước xong thì dừng và báo.

**Bước 0 — Báo cáo môi trường.** Mục 4. Không code.

**Bước 1 — `common/normalize.py` + test.** Không cần Docker, không cần Kafka. Làm trước vì thành viên 2 đang chờ, và vì nó là phần dễ sai nhất. Lấy mẫu thật cả 5 sàn từ `landing/`.

**Bước 2 — `jobs/build_silver_sample.py`.** Tháo chặn thành viên 2. Chạy xong gửi `docs/bronze_silver_schema.md` cho nhóm.

**Bước 3 — Cài Docker Engine + Docker Compose đợt 1 (Kafka) + `create_topics.sh`.** Chỉ Kafka. Kiểm tra: `--list` thấy đủ 5 topic, `--describe raw.price` đúng 6 partition. Produce và consume thử 10 message bằng console tool.

**Bước 4 — `KafkaSink` + `MultiSink` + `SINK` nhiều giá trị.** Test offline và test với broker vừa dựng. **Chưa chuyển crawler thật sang.**

**Bước 5 — Docker Compose đợt 2 (HDFS) + `init_dirs.sh`.** Kiểm tra `hdfs dfsadmin -report` thấy 2 DataNode, tạo được `/datalake/bronze`, `/datalake/silver`, `/datalake/quarantine`, `/checkpoints`.

**Bước 6 — `jobs/replay_producer.py`.** Viết và test với `--dry-run` và `--rate 10` trên một file nhỏ. **Chưa replay toàn bộ.**

**Bước 7 — Docker Compose đợt 3 (Spark) + `spark/submit.sh`.** Chốt cluster hay local mode (mục 3). Chạy thử một job Spark đọc 1 file từ HDFS và đọc được topic Kafka qua `kafka:29092`.

**Bước 8 — `jobs/kafka_to_bronze.py`.** Bật streaming, để chạy. Kiểm tra Bronze có file, đúng partition, `date`/`hour` lấy theo `event_time`.

**Bước 9 — Chuyển crawler sang `SINK=file,kafka`.** Bước duy nhất cần khởi động lại crawler. Làm giữa hai vòng `crawl_hot`, ngoài giờ `crawl_full`, có người ngồi xem. **Ghi lại mốc giờ chuyển** (dùng làm `--until` của replay). Kiểm tra event mới chảy qua Kafka xuống Bronze trong vòng 5 phút.

**Bước 10 — Replay dữ liệu cũ.** `--until` = mốc ở bước 9, `--rate 500`. Theo dõi RAM và lag của consumer. Xong thì so số event trong Bronze với offset Kafka và với số dòng trong `landing/` trước mốc.

**Bước 11 — `jobs/rebuild_silver.py`.** Chạy cho toàn bộ khoảng ngày đã có. So số bản ghi Silver cộng quarantine với số `event_id` khác nhau trong Bronze.

**Bước 12 — Cron `nightly_batch`.** 01:00 giờ VN. (**Thực tế: 07:30 giờ VN**, lý do ở 10.3.)

## 12. Tiêu chí hoàn thành

**2A:**
- [x] 5 topic tồn tại đúng cấu hình partition, key, retention
- [x] Số event trong Bronze khớp với số message đã vào Kafka (so theo offset từng partition)
- [x] Tắt `kafka_to_bronze` giữa chừng rồi bật lại: không mất, không trùng
- [x] Toàn bộ event trong `landing/` trước mốc chuyển sink đã có trong Bronze, nằm đúng partition ngày giờ gốc
- [x] Crawler chạy qua `SINK=file,kafka`, event mới xuống Bronze trong vòng 5 phút
- [x] Crawler không chết trong suốt quá trình: `crawl_runs` không có dòng `failed`/`killed` mới ngoài các lần dừng có chủ đích (bước 9, tắt máy). Từ lúc chuyển sink chỉ có 3 run `killed`, do WSL tự khởi động lại lúc 08:54 UTC ngày 10/11 (máy hết RAM vì project khác), crawler không lỗi

**2B:**
- [x] `normalize` có test pass cho cả 5 sàn, mỗi sàn 3 ca (92 test)
- [x] Chạy `rebuild_silver` 2 lần trên cùng một ngày ra cùng số bản ghi
- [x] Số bản ghi Silver + quarantine = số `event_id` khác nhau trong Bronze của ngày đó
- [ ] Có số liệu tỉ lệ quarantine theo từng loại lỗi, và tỉ lệ `dq_flags`, theo từng sàn (cho báo cáo)
- [ ] `docs/bronze_silver_schema.md` đã gửi nhóm

## 13. Lệnh hay dùng

Đường dẫn script Kafka trong container tuỳ image (image `apache/kafka`: `/opt/kafka/bin/`). Chạy trong `crawler/infra/`.

```bash
# Kafka
docker compose --profile kafka up -d
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic raw.price
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic raw.price --from-beginning --max-messages 5
# Offset cuối từng partition (dùng để đối chiếu với Bronze)
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic raw.price

# HDFS
docker exec -it namenode hdfs dfsadmin -report
docker exec -it namenode hdfs dfs -ls -R /datalake/bronze | head -50
docker exec -it namenode hdfs dfs -du -h /datalake

# Spark (chạy trong crawler/)
bash spark/submit.sh --name kafka-to-bronze --detach --conf spark.cores.max=1 jobs/kafka_to_bronze.py
docker logs --tail 5 kafka-to-bronze
bash spark/submit.sh --ui-port 4041 --mem 1536m jobs/rebuild_silver.py --date 2026-11-09
bash spark/nightly_batch.sh --dry-run      # xem lần chạy đêm tới sẽ dựng những ngày nào
# Spark UI của kafka-to-bronze: http://localhost:4040, master UI: http://localhost:8080

# Theo dõi tài nguyên
docker stats --no-stream
free -h

# Crawler (chỉ xem, không can thiệp)
python -m jobs.status --hours 6
tmux attach -t crawler      # thoát: đóng tab terminal (Ctrl+B bị VS Code chặn), KHÔNG Ctrl+C
```

## 14. Kết quả thực hiện (10/11/2026)

Số liệu đều đã kiểm chứng bằng lệnh, không ước lượng.

**Mốc chuyển crawler sang `SINK=file,kafka`:** `2026-11-09T15:28:38Z` (lưu ở `state/sink_switch_until.txt`, và là `SWITCH_TIME` trong `jobs/rebuild_silver.py`). Vòng crawl đầu sau mốc: 5.998 dòng `landing/` = 5.998 message Kafka = 5.998 dòng Bronze, xuống Bronze trong vòng 5 phút.

**Replay (bước 10):** 1.995.998 event trước mốc, tốc độ 500 event/giây, 22:49–23:55 giờ VN ngày 09/11, 0 lỗi, 0 vào `dlq.invalid`.

**Bronze (kiểm tra sáng 10/11):**
- 2.102.273 dòng price + 6.822 dòng product; offset liền mạch từ 0 ở cả 9 partition Kafka.
- 957/957 nhóm (sàn, ngày, giờ) khớp `landing/` cả số dòng lẫn checksum; 0 dòng sai partition, 0 dòng `_unknown`.
- Khoảng 280 MB gzip (gấp đôi với 2 bản sao), 1.482 file.
- Thử tắt giữa batch rồi bật lại: batch đó chạy lại đúng 39.996 dòng, không mất, không trùng.
- Tốc độ: 6.000–20.000 dòng/giây mỗi batch; driver khoảng 550 MB, worker khoảng 900 MB.

**Silver (ngày 01–09/11, dựng ngày 10/11):**

| | price | product |
| --- | --- | --- |
| Dòng Bronze | 2.097.026 | 6.822 |
| `event_id` khác nhau trong Bronze | 2.096.994 | 6.822 |
| Silver | 2.096.994 | 6.822 |
| Quarantine | 0 | 0 |
| `ingest_source = live` (09/11) | 107.293 | 556 |

- Từng ngày trong 9 ngày: Silver + quarantine = số `event_id` khác nhau trong Bronze.
- 32 dòng trùng `event_id` trong Bronze được bỏ.
- Ngày 05–07/11 khớp **từng dòng** với Silver mẫu (835.116 price, 2.245 product), cùng kiểu dữ liệu từng cột.
- Chạy lại cùng ngày ra y hệt; nhánh quarantine thử bằng Bronze giả (`BAD_JSON`, `PRICE_OUT_OF_RANGE`, `MISSING_PRODUCT_ID`) đúng mã lỗi, chạy lại vẫn đúng số dòng.
- `dq_flags` cả 9 ngày: `MISSING_PRICE` 7.052 (0,34%), `ZERO_PRICE` 219 (0,01%).
- Thời gian: 3 phút 24 giây cho 9 ngày (khoảng 20 giây mỗi ngày price). Silver price 115 MB (gấp đôi với 2 bản sao).

**Còn lại:** số liệu tỉ lệ `dq_flags` và quarantine theo từng sàn cho báo cáo (mục 12).
