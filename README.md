# Big_data_Team6 — Hệ thống phát hiện bất thường giá và hàng giả đa sàn TMĐT

Bài tập lớn môn Lưu trữ và Xử lý Dữ liệu lớn.

```
Crawler  →  Kafka  →  Bronze (HDFS)  →  Silver (Delta)  →  Gold
                                              ↓               ↓
                                      Speed layer      Batch layer
                                              ↓               ↓
                                     ClickHouse + Redis → Dashboard
```

## Cấu trúc repo

| Thư mục | Nội dung | Trạng thái |
| --- | --- | --- |
| [`crawler/`](crawler/) | Thu thập dữ liệu (giai đoạn 1), đưa qua Kafka lên Bronze và làm sạch thành Silver (giai đoạn 2) | Đang làm (giai đoạn 2) |

Mỗi phần có README riêng trong thư mục của nó.

## Tiến độ

| Giai đoạn | Nội dung | Spec | Trạng thái |
| --- | --- | --- | --- |
| 1 | Crawler 5 sàn (tiki, cellphones, hoangha, fptshop, tgdd), ghi JSONL vào `landing/` | [`crawler/README.md`](crawler/README.md) | Xong 08/11/2026, vẫn crawl liên tục từ 01/11 |
| 2 | Kafka, Bronze (HDFS), Silver (Delta) | [`crawler/README-2A.md`](crawler/README-2A.md) | Đang làm |
| 3 | Gold, batch layer | | Chưa bắt đầu |
