# Fixture CellphoneS

Mỗi file là một `RawResponse` (xem `common/schema.py`). Gọi tay ngày 2026-11-01 (5 request, User-Agent học thuật).

| File | Nguồn | Nội dung |
| --- | --- | --- |
| `listing_mobile.json` | Thật, **đã lược** | `https://cellphones.com.vn/mobile.html`, 20 thẻ sản phẩm. Đã bỏ `<script>` (trừ JSON-LD), `<style>`, `<svg>` để giảm từ 1,7 MB xuống ~340 KB; parse không dùng các phần này |
| `detail_normal.json` | Thật, **trích phần dùng** | `https://cellphones.com.vn/iphone-18-pro.html`: chỉ giữ 2 khối JSON-LD (`BreadcrumbList`, `Product`) |
| `detail_out_of_stock_derived.json` | **Suy ra** | Từ `detail_normal`, đổi `offers.availability` thành `OutOfStock` |
| `detail_missing_fields_derived.json` | **Suy ra** | Từ `detail_normal`, bỏ `brand`, `aggregateRating`, `additionalProperty`, `offers.price` |

Trường `review` trong JSON-LD (có tên người viết review) đã bị xoá vì là dữ liệu cá nhân.
