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
| [`crawler/`](crawler/) | Thu thập giá và thông tin sản phẩm từ các sàn | Đang làm (giai đoạn 1) |

Mỗi phần có README riêng trong thư mục của nó.
