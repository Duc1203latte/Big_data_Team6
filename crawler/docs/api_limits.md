# Giới hạn truy cập đo thực tế

Sinh bởi `jobs/measure_limits.py`.

## tiki — 2026-11-01 14:01 UTC

- URL đo: 3 trang danh sách, ví dụ `https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category=1789&page=1`
- User-Agent: `bigdata-crawler/0.1.0 (academic research project; contact: ...)`
- Mỗi mức 120 giây; dừng mức khi gặp 5 lần 429/403

| Mức (req/s) | Request | 2xx | 429 | 403 | Lỗi khác | Độ trễ TB (ms) | p95 (ms) | 429/403 đầu tiên |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.2 | 24 | 24 | 0 | 0 | 0 | 145 | 329 | - |
| 0.5 | 60 | 60 | 0 | 0 | 0 | 138 | 214 | - |
| 1 | 120 | 120 | 0 | 0 | 0 | 131 | 179 | - |
| 2 | 240 | 240 | 0 | 0 | 0 | 51 | 153 | - |

**Kết luận:** không gặp 429/403 tới 2 req/s.

## tiki — đính chính ngày 2026-11-01 (khoảng 14:20 UTC)

Kết luận "không gặp 429/403 tới 2 req/s" ở mục trên **không đáng tin**:

- Khoảng 10 phút sau lần đo, API danh sách `api/personalish/v1/blocks/listings` trả về **mã 200 kèm
  một trang HTML 25.864 byte** (server `Byte-nginx`, nội dung "the system is inspecting your current
  network environment"), tức trang kiểm tra chống bot, thay vì JSON. Mọi URL danh sách đều nhận cùng trang này.
- API danh mục `api/v2/categories` vẫn trả JSON bình thường vài phút trước đó.
- Ở mức 2 req/s, độ trễ trung bình tụt từ ~140 ms xuống 51 ms. Nhiều khả năng từ mức này Tiki đã
  trả trang chặn (nhẹ, nhanh), nhưng phiên bản đo cũ đếm mọi mã 200 là thành công nên không phát hiện.

Kết luận tạm thời: **0,2 đến 1 req/s trong vài phút chưa bị chặn; 2 req/s và/hoặc tổng ~450 request
liên tục có thể đã kích hoạt chặn.** Crawler giữ 0,2 req/s. `measure_limits.py` và `HttpClient` đã được
sửa để coi "200 nhưng không phải JSON" là bị chặn (`expect_json: true` trong `config/platforms.yaml`).
Theo README, nhóm không tìm cách vượt qua trang kiểm tra này.

