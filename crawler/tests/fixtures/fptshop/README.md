# Fixture FPT Shop

Mỗi file là một `RawResponse` (xem `common/schema.py`). Gọi tay ngày 2026-11-01 (4 request, User-Agent học thuật).

| File | Nguồn | Nội dung |
| --- | --- | --- |
| `listing_dien_thoai.json` | Thật, **trích phần dùng** | `https://fptshop.com.vn/dien-thoai` (2,7 MB). Giữ nguyên văn mảng `"items"` của danh sách chính (24 sản phẩm) và khối "Bán chạy nhất" (2 sản phẩm), đặt lại vào 2 đoạn `self.__next_f.push` để parser đọc như trang thật |
| `detail_normal.json` | Thật, **trích phần dùng** | `https://fptshop.com.vn/dien-thoai/iphone-duo`: chỉ giữ JSON-LD `Product` và `BreadcrumbList` |
| `detail_out_of_stock_derived.json` | **Suy ra** | Từ `detail_normal`, đổi `offers.availability` thành `OutOfStock` |
| `detail_missing_fields_derived.json` | **Suy ra** | Từ `detail_normal`, bỏ `brand`, `aggregateRating`, `additionalProperty`, `offers.price` |

Trường `review` trong JSON-LD (có tên người viết review) đã bị xoá vì là dữ liệu cá nhân.
