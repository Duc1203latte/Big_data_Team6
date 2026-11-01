# Fixture Tiki

Mỗi file là một `RawResponse` (xem `common/schema.py`), load bằng
`RawResponse.model_validate_json(path.read_text())`. Gọi tay ngày 2026-11-01
(5 request, User-Agent học thuật).

| File | Nguồn | Nội dung |
| --- | --- | --- |
| `listing_c1789_p1.json` | Thật | `api/personalish/v1/blocks/listings?limit=40&category=1789&page=1`, 40 sản phẩm |
| `detail_normal.json` | Thật | `api/v2/products/278628866?spid=278630782` + review summary |
| `detail_missing_fields.json` | Thật | `api/v2/products/249782771?spid=249782772`: rating 0, không có lượt bán, không có review summary |
| `detail_out_of_stock_derived.json` | **Suy ra** | Từ `detail_normal`, sửa `inventory_status="out_of_stock"`, `stock_item.qty=0` |
| `detail_no_price_derived.json` | **Suy ra** | Từ `detail_missing_fields`, đặt `price`, `original_price`, `list_price` = null |

Body của fixture `detail` có dạng `{"product": <api/v2/products>, "reviews": <api/v2/reviews hoặc null>}`.
Trường `reviews.data` (nội dung review, tên và avatar người mua) đã bị xoá vì là dữ liệu cá nhân;
chỉ giữ `stars`, `rating_average`, `reviews_count`.
