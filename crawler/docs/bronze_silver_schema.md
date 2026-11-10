# Schema Bronze và Silver

Tài liệu cho người làm tầng Gold (`product_master`, `price_baseline`) và các phần đọc dữ liệu đã làm sạch. Spec đầy đủ: `README-2A.md` mục 9 và 10.

```
Crawler → Kafka → Bronze (HDFS, JSON gzip, thô) → Silver (Delta, đã làm sạch) → Gold
```

Gold **chỉ đọc Silver**, không đọc Bronze hay `landing/`.

## 1. Bảng Silver mẫu (dùng ngay, chưa cần Kafka/HDFS)

Trong lúc chờ đường ống thật, có bảng Silver mẫu **cùng schema** dựng thẳng từ dữ liệu crawler:

```bash
python -m jobs.build_silver_sample --date-from 2026-11-05 --date-to 2026-11-07
```

| Bảng | Đường dẫn | Số dòng (05–07/11) |
| --- | --- | --- |
| `silver_price` | `datalake_local/silver_sample/price/` | 835.116 |
| `silver_product` | `datalake_local/silver_sample/product/` | 2.245 |
| quarantine | `datalake_local/silver_sample/quarantine/<price\|product>/` | 0 (chỉ tạo khi có bản ghi lỗi) |

Khác Silver thật đúng 2 cột: `ingest_time = null`, `ingest_source = "sample"`. Khi Silver thật lên HDFS, job Gold **chỉ đổi đường dẫn đọc**.

### Silver thật (HDFS, `jobs/rebuild_silver.py`)

| Bảng | Đường dẫn |
| --- | --- |
| `silver_price` | `/datalake/silver/price` |
| `silver_product` | `/datalake/silver/product` |
| quarantine | `/datalake/quarantine/<price\|product>` |

`ingest_source` là `live` (crawler gửi thẳng vào Kafka) hoặc `replay` (nạp lại từ `landing/`). Quarantine thật thay cột `source_file` của bản mẫu bằng vị trí Kafka: `kafka_partition`, `kafka_offset`, cùng `ingest_time`, `ingest_source`.

### Cách đọc

Bảng là **Delta Lake** (protocol reader 1 / writer 2, Spark Delta 3.x đọc được).

```python
# Spark (cần package io.delta:delta-spark_2.12:3.2.x)
df = spark.read.format("delta").load("datalake_local/silver_sample/price")
df.where("date = '2026-11-06' AND platform = 'tiki'")

# Không có Spark: thư viện deltalake (pip install deltalake pyarrow)
from deltalake import DeltaTable
pdf = DeltaTable("datalake_local/silver_sample/price").to_pandas()
```

**Luôn đọc dưới dạng Delta, đừng đọc thẳng các file `.parquet` trong thư mục.** Mỗi lần chạy lại, Delta đánh dấu file cũ là đã xoá nhưng file vẫn nằm trên đĩa; đọc Parquet thô sẽ ra dòng trùng.

## 2. Bronze (tầng thô, chỉ để tham khảo)

```
/datalake/bronze/<price|product>/platform=<sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-*.json.gz
```

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `raw` | string | nguyên một dòng JSON do crawler sinh ra, không sửa |
| `kafka_key` | string | `platform:product_id` |
| `topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` | | vị trí message trong Kafka |
| `ingest_time` | timestamp | lúc ghi xuống Bronze |
| `ingest_source` | string | `live` / `replay`, từ header Kafka. Không có ở Bronze ghi trước 10/11/2026: Silver suy theo mốc chuyển crawler sang Kafka (2026-11-09T15:28:38Z) |
| `platform`, `date`, `hour` | string | partition, `date`/`hour` lấy từ `event_time` (UTC) |

Bronze **có thể có bản trùng** (replay chạy 2 lần). Silver mới là nơi bỏ trùng.

## 3. `silver_price`

Mỗi dòng = một lần quan sát giá của một SKU trên một sàn. Partition theo `date`, `platform`. Khoá: `event_id` (duy nhất trong bảng).

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `event_id` | string | khoá, uuid5 sinh từ sàn + sản phẩm + SKU + loại + thời điểm |
| `platform` | string | `tiki`, `cellphones`, `hoangha`, `fptshop`, `tgdd` (`lazada` có code nhưng chưa có dữ liệu) |
| `product_id` | string | id gốc của sàn (tiki là số, các sàn khác là slug URL) |
| `product_key` | string | `platform:product_id`, dùng để nối với `silver_product` |
| `sku_id` | string | biến thể (màu, dung lượng…); null ở cellphones |
| `url` | string | trang sản phẩm |
| `name_raw` | string | tên gốc của sàn, **chưa chuẩn hoá** |
| `source` | string | `listing` (trang danh sách, ~99%) hoặc `detail` (trang chi tiết) |
| `price_current` | bigint | giá bán (VND). **null** khi không có giá thật, xem `dq_flags` |
| `price_current_raw` | string | chữ giá gốc trên trang (`38.690.000đ`, `Liên hệ`…) |
| `price_original` | bigint | giá gốc trước giảm; null nếu sàn không ghi hoặc ghi 0 |
| `discount_pct` | double | % giảm = (gốc − bán) / gốc × 100, làm tròn 2 số. 0.0 nếu không giảm. null nếu thiếu giá hoặc giá gốc thấp hơn giá bán |
| `currency` | string | luôn `VND` |
| `in_stock` | boolean | còn hàng; null nếu trang không cho biết (tiki, cellphones gần như luôn null, hoangha ~1/3 null) |
| `seller_id` | string | id shop, chỉ tiki có |
| `seller_name` | string | tên shop, rất ít (tiki trang chi tiết) |
| `rating_avg` | double | điểm đánh giá 0–5 |
| `rating_count` | int | số lượt đánh giá |
| `review_count` | int | số bình luận |
| `sold_count` | int | số đã bán (tiki, fptshop, tgdd; cellphones và hoangha không có) |
| `event_time` | timestamp (UTC) | thời điểm crawl |
| `ingest_time` | timestamp (UTC) | lúc vào Bronze; null ở bảng mẫu |
| `ingest_source` | string | `live`, `replay`, hoặc `sample` |
| `date` | date | partition, **ngày UTC** của `event_time` (07:00 sáng VN mới sang ngày mới) |
| `crawler_version` | string | phiên bản crawler |
| `dq_flags` | array<string> | rỗng nếu đủ dữ liệu, xem mục 5 |

## 4. `silver_product`

Thông tin ít đổi của sản phẩm, lấy từ trang chi tiết (crawl thưa, mỗi đêm vài trăm sản phẩm mỗi sàn). Partition theo `platform`. Giữ **mọi lần quan sát**: Gold tự lấy bản mới nhất theo `product_key` (`event_time` lớn nhất).

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `event_id`, `platform`, `product_id`, `product_key`, `url`, `name_raw` | string | như `silver_price` |
| `brand_raw` | string | thương hiệu, chữ gốc |
| `category_raw` | string | danh mục, chữ gốc |
| `specs_raw` | string | thông số kỹ thuật, **chuỗi JSON** (key đã sắp xếp); đọc bằng `from_json` / `json.loads` |
| `image_url` | string | ảnh chính |
| `rating_avg` | double | |
| `rating_count`, `rating_1_count` … `rating_5_count` | int | phân bố sao; null nếu sàn không công bố |
| `event_time`, `ingest_time` | timestamp (UTC) | |
| `ingest_source` | string | |
| `crawler_version` | string | |

## 5. Chất lượng dữ liệu

Hai mức, tách bạch:

**`dq_flags` — bản ghi hợp lệ nhưng thiếu dữ liệu, VẪN ở Silver:**

| Cờ | Khi nào | Silver xử lý |
| --- | --- | --- |
| `MISSING_PRICE` | sản phẩm "Liên hệ" / chưa có giá (cellphones, hoangha, tgdd) | `price_current = null` |
| `ZERO_PRICE` | sàn ghi giá 0 (fptshop, cellphones, tgdd) | `price_current = null` |

Sản phẩm có cờ vẫn là quan sát thật (đang bày bán, chỉ chưa có giá), nên đừng coi là "biến mất". Khi tính baseline giá thì lọc `price_current IS NOT NULL`.

**Quarantine — bản ghi hỏng, KHÔNG vào Silver:** `dq_errors` gồm `MISSING_EVENT_ID`, `MISSING_PRODUCT_ID`, `BAD_TIMESTAMP`, `UNKNOWN_PLATFORM`, `PRICE_OUT_OF_RANGE` (ngoài 1.000 – 2.000.000.000 VND), `BAD_JSON`. Bảng quarantine có thêm cột `raw` (dòng gốc) để xem lại.

Số liệu thật 05–07/11 (835.116 price event): 0 vào quarantine; `MISSING_PRICE` 2.831 (0,34%); `ZERO_PRICE` 78 (0,01%).

## 6. Lưu ý khi làm Gold

- **Tên chưa chuẩn hoá, chưa ghép sản phẩm giữa các sàn.** `name_raw` là tên gốc; cùng một iPhone ở 5 sàn có 5 `product_key` khác nhau. Chuẩn hoá tên và ghép sản phẩm là việc của `product_master` (Gold).
- **Giá gốc thấp hơn giá bán** (~0,8% event của hoangha): dữ liệu sàn sai, giữ nguyên hai cột giá nhưng `discount_pct = null`.
- **Mật độ quan sát khác nhau:** sàn và sản phẩm trong nhóm "hot" được crawl mỗi 30–60 phút; phần còn lại chỉ có ở lượt crawl đêm (tiki, hoangha nhiều dòng nhất). Khi tính trung bình giá theo thời gian nên gom theo giờ/ngày trước.
- **Mốc thời gian đều là UTC.** Đổi sang giờ VN (+7) khi hiển thị.
- **Khoảng trống dữ liệu** (máy tắt, sàn chặn) được ghi trong bảng `crawl_runs` của crawler (`state/crawler.db`), xem `docs/api_limits.md`.
