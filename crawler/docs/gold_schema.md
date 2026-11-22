# Schema Gold, batch view và khoá Redis

Tài liệu cho người dùng Gold: speed layer (tuần 4, thành viên 3) và serving, dashboard (tuần 5, thành viên 4).
Gold do batch layer (tuần 3, thành viên 2) tạo, mỗi đêm qua DAG `nightly_batch` (`airflow/README.md`).

```
Silver ──► entity_resolution ──► product_master ─┐
       └─► price_baseline ─────────────────────── ├─► speed layer (stream-static join, nạp lại mỗi micro-batch)
       └─► daily_stats ──► daily_price_stats      │
                       ├─► daily_price_spread     │
                       └─► ClickHouse price_batch_daily + serving_cutoff ─► price_serving (tuần 5)
                       └─► Redis price:last:{master_id} (giá trị ban đầu)
```

**Nguyên tắc chung**

- Mọi bảng Gold là **Delta Lake** trên HDFS `/datalake/gold/<bảng>`. Luôn đọc dưới dạng Delta, không đọc thẳng file `.parquet`.
- Ghi đè **nguyên tử** (Delta overwrite): speed layer không bao giờ đọc phải bảng ghi dở.
- Cập nhật mỗi sáng sau khi Silver dựng xong (khoảng 07:30–08:00 giờ VN). Speed layer nạp lại mỗi micro-batch nên tự thấy bản mới.
- Mọi thời gian là **UTC**. `date` là ngày UTC.
- **Cột và khoá trong tài liệu này là hợp đồng**: đổi ngưỡng ghép hay thêm dữ liệu không đổi cấu trúc bảng. Muốn đổi cột thì báo trước người dùng.
- Phát triển trên máy khác: lấy file `gold_YYYYMMDD.tar.gz` trên Drive (cùng chỗ với Silver), giải nén giống Silver (README mục 4.2).

Số liệu trong tài liệu: bản dựng từ Silver 01–15/11/2026.

## 1. `product_master`: khớp cùng sản phẩm giữa các sàn

Mỗi dòng = một `product_key`. **Cùng `master_id` = cùng một sản phẩm** (cùng model, dung lượng, RAM) ở nhiều sàn.
Sản phẩm không khớp với sàn nào vẫn có dòng, với `cluster_size = 1`. 11.954 dòng.

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `product_key` | string | khoá, `platform:product_id` như Silver |
| `platform` | string | |
| `master_id` | string | id cụm, `m_` + 16 ký tự hex. **Có thể đổi** khi cụm đổi (sản phẩm mới, đổi ngưỡng): chỉ dùng để tra cứu trong micro-batch, cảnh báo lưu lâu dài thì lưu `product_key` |
| `cluster_size` | int | số sàn trong cụm (1–5). Mỗi cụm tối đa 1 sản phẩm mỗi sàn |
| `match_score` | double | cosine của cặp đã đưa sản phẩm vào cụm; null nếu đứng một mình |
| `name_rep` | string | tên đại diện (tên xuất hiện nhiều nhất trong Silver) |
| `name_clean` | string | tên đã chuẩn hoá (`common/product_text.clean_name`) |
| `brand` | string | thương hiệu chuẩn hoá, chữ thường; null nếu không xác định |
| `category` | string | `phone`, `tablet`, `laptop`, `watch`, `audio`, `accessory`, `other` |
| `ram_gb`, `storage_gb` | int | null nếu tên và thông số không ghi |
| `variants` | array<string> | hậu tố model: pro, max, plus, ultra, fe, 5g... |
| `model_nums` | array<string> | số đời máy: 15, s24, a17, m5... |
| `price_median` | double | trung vị giá 7 ngày gần nhất (VND) |
| `n_obs` | bigint | số lần quan sát trong Silver |
| `computed_at` | timestamp | |

Tỉ lệ sản phẩm khớp với ít nhất một sàn khác: **8,0%** trên toàn bộ, nhưng **55% điện thoại**, 34% máy tính bảng,
25% laptop, 19% đồng hồ; phụ kiện 1,8% (tiki bán phụ kiện khác hẳn các sàn còn lại). 397 cụm từ 2 sàn trở lên.
Cách ghép và các luật: `jobs/entity_resolution.py`, `common/entity_rules.py`. Precision đo bằng tập gán nhãn: `labeling/`.

## 2. `price_baseline`: giá tham chiếu

Mỗi dòng = một `(product_key, sku_id)` có giá trong cửa sổ. 15.519 dòng.

**Join với sự kiện stream:** `sku_id` null (cellphones) được ghi thành chuỗi rỗng `""`. Speed layer làm giống vậy với
sự kiện (`coalesce(sku_id, '')`) rồi join bằng `=`. Join trực tiếp `NULL = NULL` ra false, mất toàn bộ cellphones.

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `product_key`, `sku_id` | string | khoá |
| `platform` | string | |
| `level` | string | `sku`: tính trên chính SKU; `product`: SKU chưa đủ ngày nên dùng số của cả `product_key` |
| `status` | string | `ok` (đủ ≥ 7 ngày) hoặc `insufficient`. **Chỉ chấm luật 1 và 3 khi `ok`** |
| `median_price` | double | trung vị các giá trung vị theo ngày (VND) |
| `mad` | double | trung vị \|giá ngày − median_price\|. **93% SKU có MAD = 0** (giá đứng yên): luật phải có sàn cho MAD |
| `min_price`, `max_price` | double | giá trung vị ngày thấp nhất, cao nhất trong cửa sổ |
| `n_days`, `n_obs` | bigint | số ngày có giá, số quan sát |
| `median_price_original` | double | trung vị giá gốc theo ngày (luật khuyến mãi ảo); null nếu sàn không ghi giá gốc |
| `n_days_original` | bigint | số ngày có giá gốc |
| `first_day`, `last_day` | date | ngày đầu, cuối có dữ liệu |
| `window_start`, `window_end` | date | cửa sổ tính: 30 ngày kết thúc ở ngày Silver mới nhất; Silver chưa đủ 30 ngày thì dùng hết |
| `computed_at` | timestamp | |

Cách tính: bỏ giá null → **gom về trung vị theo ngày trước** (SKU crawl 30–60 phút/lần không nặng ký hơn SKU chỉ có
lượt đêm, ngày máy tắt không bị coi là "giá không đổi") → median, MAD trên các giá trị ngày.

Nhóm theo dõi sát (≥ 6 quan sát/ngày trong 7 ngày cuối): 6.056 SKU, 127 chưa có baseline `ok`, toàn là SKU mới xuất
hiện sau 06/11 (job in danh sách theo ngày xuất hiện). Các SKU này tự đủ 7 ngày theo thời gian.

## 3. `daily_price_stats`, `daily_price_spread`

Partition theo `date`. Job ghi lại đúng những ngày được chạy (Delta `replaceWhere`), giữ ngày khác.

**`daily_price_stats`**: một dòng / (ngày, sàn, danh mục, thương hiệu). Tính trên giá trung vị ngày của từng SKU.

| Cột | Ý nghĩa |
| --- | --- |
| `date`, `platform`, `category`, `brand` | `brand = 'unknown'` khi không xác định |
| `n_skus`, `n_obs` | số SKU, số quan sát |
| `price_avg`, `price_median`, `price_std` | trung bình, trung vị, độ lệch chuẩn của giá SKU |

**`daily_price_spread`**: một dòng / (ngày, `master_id`) có mặt ở ≥ 2 sàn trong ngày.

| Cột | Ý nghĩa |
| --- | --- |
| `date`, `master_id`, `name`, `category`, `brand` | |
| `n_platforms` | số sàn có giá trong ngày |
| `price_min`, `price_max`, `spread_pct` | `spread_pct = (max − min) / min × 100` |
| `cheapest_platform`, `priciest_platform` | |

## 4. `er_candidates`: cặp ứng viên của entity_resolution

Mọi cặp Qdrant tìm ra có cosine ≥ 0,6, kèm kết quả từng luật (`ok_score`, `ok_brand`, `ok_storage`, `ok_ram`,
`ok_variant`, `ok_model`, `ok_chip`, `ok_price`), `matched` (qua mọi luật) và `clustered` (cuối cùng cùng cụm).
Dùng cho bước gán nhãn (`jobs/er_labeling.py`) và để giải thích vì sao hai sản phẩm không được ghép.

## 5. ClickHouse (database `bigdata`)

Tạo bởi `infra/clickhouse/init/01_gold_batch_view.sql`. Ai ghi bảng nào thì người đó tạo bảng đó: các bảng speed layer
(`price_realtime`, `price_window_stats`, `price_alerts`) đặt trong `infra/clickhouse/init/02_*.sql`.

| Bảng | Mỗi dòng | Ghi bởi |
| --- | --- | --- |
| `price_batch_daily` | một SKU trong một ngày UTC: `price_min/max/avg/median/last`, `price_original_median`, `discount_pct_avg`, `n_obs`, `first_event_time`, `last_event_time`, `master_id`, `category`, `brand`, `name` | `daily_stats` |
| `serving_cutoff` | mốc cắt cho mỗi view (`view_name = 'price'`) | `daily_stats` |
| `daily_price_stats`, `daily_price_spread` | bản sao của bảng Gold cùng tên, cho dashboard | `daily_stats` |

**Mốc cắt:** `cutoff` = 00:00 UTC của ngày sau ngày batch mới nhất. Batch view phủ `[đầu dữ liệu, cutoff)`,
real-time view phủ `[cutoff, bây giờ)`.

```sql
SELECT cutoff FROM bigdata.serving_cutoff FINAL WHERE view_name = 'price';
```

Đề xuất cho tuần 4–5 để `price_serving` gộp được: `price_realtime` mỗi dòng là một sự kiện, có ít nhất `event_time`,
`date`, `platform`, `product_key`, `sku_id` (`''` khi null), `price_current`, `price_original`, `discount_pct`,
`is_anomaly`. `price_serving` gom `price_realtime` theo `(date, platform, product_key, sku_id)` ra cùng các cột giá
như `price_batch_daily` rồi `UNION ALL`, mỗi phía lọc theo `cutoff`.

## 6. Redis

| Khoá | Kiểu | Giá trị | Ghi bởi |
| --- | --- | --- | --- |
| `price:last:{master_id}` | hash | trường = tên sàn, giá trị = `"<giá>|<event_time ISO UTC>"`, vd `21290000|2026-11-15T17:13:00Z` | speed layer mỗi sự kiện; `daily_stats --redis-url` nạp sẵn từ Silver để khởi động không trống |

Chỉ có khoá cho sản phẩm đã ghép (`cluster_size ≥ 2`). Định dạng là đề xuất, thành viên 3 chốt.

## 7. Qdrant

Collection `products` (cosine, 384 chiều, model `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`).
Id điểm = `uuid5(NAMESPACE_URL, product_key)`; payload `product_key`, `platform`, `category`, `brand`.
Tạo lại mỗi lần `entity_resolution` chạy.

## 8. Đề xuất công thức 3 luật bất thường (để thành viên 3 chốt)

Kế hoạch ghi "theo công thức trong đề xuất" nhưng tài liệu đề xuất chưa có trong repo. Dưới đây là đề xuất, ngưỡng để trong file cấu hình.
Ký hiệu: `p` giá bán sự kiện, `o` giá gốc, `d` % giảm; `M`, `MAD`, `Mo` từ `price_baseline`.

| Luật | Bất thường khi | Ngưỡng mặc định | Điểm |
| --- | --- | --- | --- |
| **Lệch lịch sử** `HISTORY_DEVIATION_DOWN/UP` | `status = ok`, \|z\| ≥ 3,5 và \|p − M\| / M ≥ 20%, với z = (p − M) / (1,4826 × max(MAD, 1% × M)) | `z_threshold 3.5`, `min_relative_dev 0.20`, `mad_floor_ratio 0.01` | `min(1, |z| / 10)` |
| **Rẻ hơn sàn khác** `CHEAPER_THAN_MARKET` | sản phẩm đã ghép, có ≥ 1 giá sàn khác trong 24 giờ (Redis `price:last`), p / trung vị(giá sàn khác) ≤ 0,7 | `max_ratio 0.70`, `freshness_hours 24` | `min(1, (1 − ratio) / 0.7)` |
| **Khuyến mãi ảo** `FAKE_DISCOUNT` | `status = ok`, `d ≥ 20`, giảm thật (M − p) / M ≤ 5% và o ≥ 1,15 × Mo | `min_claimed_discount_pct 20`, `max_real_discount 0.05`, `original_inflation_ratio 1.15` | `min(1, d/100 − giảm thật)` |

`is_anomaly` = một trong ba luật; `anomaly_score` = điểm lớn nhất; `anomaly_reason` = danh sách mã luật, cách nhau dấu phẩy.

Sự kiện tiêm thử (việc 8 tuần 4): giá giảm 70% → luật 1 (và luật 2 nếu đã ghép); giá gốc bị nâng → luật 3; giá thêm
ba số 0 → `normalize` đưa vào quarantine (`PRICE_OUT_OF_RANGE`, > 2 tỉ VND) **chỉ khi sản phẩm đủ đắt**: tiêm bằng
điện thoại, vì cáp 100.000đ × 1000 vẫn trong khoảng hợp lệ và sẽ thành cảnh báo luật 1.
