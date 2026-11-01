# Fixture Thế Giới Di Động

Mỗi file là một `RawResponse` (xem `common/schema.py`). Gọi tay ngày 2026-11-01 (5 request, User-Agent học thuật).

| File | Nguồn | Nội dung |
| --- | --- | --- |
| `listing_dtdd.json` | Thật, **trích phần dùng** | `https://www.thegioididong.com/dtdd` (~550 KB). Giữ nguyên văn mảng `"products"` (20 sản phẩm), đặt lại vào 2 đoạn `self.__next_f.push`; thêm một mảng `"products"` giả không có giá để kiểm tra parser chọn đúng danh sách |
| `detail_normal.json` | Thật, **trích phần dùng** | `https://www.thegioididong.com/dtdd/oppo-reno16f-5g-8gb-128gb`: JSON-LD `ProductGroup` (3 biến thể màu + 1 phần tử rỗng như trang thật) và `BreadcrumbList` |
| `detail_out_of_stock_derived.json` | **Suy ra** | Từ `detail_normal`, mọi biến thể `OutOfStock` |
| `detail_missing_fields_derived.json` | **Suy ra** | Từ `detail_normal`, bỏ `brand`, `aggregateRating`, `additionalProperty`, giá của biến thể |
| `detail_single_product_derived.json` | **Suy ra** | Trang không có biến thể: một JSON-LD `Product` lấy từ biến thể đầu |

Trường `review` trong JSON-LD (có tên người viết review) đã bị xoá vì là dữ liệu cá nhân.
