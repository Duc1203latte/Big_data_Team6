# Fixture Hoàng Hà Mobile

Sàn đổi giao diện đêm 2026-11-01 -> 02. Các file không có hậu tố là giao diện cũ (giữ để test phần dự phòng của parser); hậu tố `_v2` là giao diện mới.

Mỗi file là một `RawResponse` (xem `common/schema.py`). Gọi tay ngày 2026-11-01 (3 request, User-Agent học thuật).

| File | Nguồn | Nội dung |
| --- | --- | --- |
| `listing_dien_thoai_p1.json` | Thật, **đã lược** | `https://hoanghamobile.com/dien-thoai-di-dong`, trang 1, 18 thẻ sản phẩm (có 1 sản phẩm đang giảm giá, 1 sản phẩm "Liên hệ"). Đã bỏ `<script>` (trừ JSON-LD), `<style>`, `<svg>` |
| `detail_normal.json` | Thật, **trích phần dùng** | `https://hoanghamobile.com/dien-thoai/iphone-18-pro-max`: giữ JSON-LD `Product`, breadcrumb (microdata) và bảng thông số `ul.specs-content` |
| `detail_out_of_stock_derived.json` | **Suy ra** | Từ `detail_normal`, đổi `offers.availability` thành `OutOfStock` |
| `detail_missing_fields_derived.json` | **Suy ra** | Từ `detail_normal`, bỏ `brand`, `aggregateRating`, `offers.price` và bảng thông số |

| `listing_dien_thoai_p1_v2.json` | Thật, **đã lược** | Giao diện mới, 2026-11-02: `https://hoanghamobile.com/dien-thoai-di-dong`, 20 thẻ `article[data-product-id]`. Đã bỏ `<script>` (trừ JSON-LD), `<style>`, `<svg>` |
| `detail_normal_v2.json` | Thật, **trích phần dùng** | Giao diện mới, 2026-11-02: `https://hoanghamobile.com/dien-thoai/iphone-18-pro-max`: JSON-LD `Product`, `BreadcrumbList` và bảng thông số |

Trường `review` trong JSON-LD (có tên người viết review) đã bị xoá vì là dữ liệu cá nhân.
