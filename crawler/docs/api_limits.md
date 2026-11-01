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

