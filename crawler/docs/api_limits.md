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


## Số liệu quan sát khi chạy thật — cả 5 sàn (02/11 → 08/11/2026)

Bài đo tăng dần tốc độ (`measure_limits.py`, lên tới 2 req/s) **chỉ chạy với Tiki** và chính nó đã làm
Tiki chặn (mục trên). Vì vậy **không chạy bài đo đó cho 4 sàn còn lại**. Thay vào đó, bảng dưới là số liệu
đo được từ một tuần crawler chạy thật ở tốc độ thấp (`crawl_hot` + `crawl_full` song song), lấy từ
`logs/` và bảng `crawl_runs`.

### Tổng hợp

| Sàn | Tốc độ hot / full (req/s) | Request/ngày (ước) | Vòng hot: trung vị / lâu nhất | Lỗi phải thử lại (cả tuần) | Bị chặn? |
| --- | --- | --- | --- | --- | --- |
| tiki | 0,2 / 0,06 | ~3.700 | 4,9 / 38 phút | 158 trang chống bot (HTTP 200), 11 HTTP 502, 6 timeout | Chặn mềm ngắt quãng, 1–3 lần/ngày |
| hoangha | 0,2 / 0,05 | ~5.800 | 9,3 / 9,9 phút | 25 ConnectTimeout, 3 ConnectError, 1 ReadTimeout | Không |
| fptshop | 0,1 / 0,03 | ~1.200 | 6,5 / 7,0 phút | 1 HTTP 503 | Không |
| cellphones | 0,1 / 0,03 | ~500–600 | 3,0 / 3,5 phút | 0 | Không |
| tgdd | 0,1 → **0,05** / 0,03 → **0,02** | ~1.800 | 12,6 / 116 phút | 422 ConnectTimeout, 372 lỗi không ghi tên (chủ yếu timeout), 40 RemoteProtocolError | **Có, ở mức kết nối (04/11)** |

- Request/ngày = số trang hot × số vòng/ngày + `crawl_full` (trang danh sách + trang chi tiết; Tiki mỗi
  trang chi tiết tốn 2 request: sản phẩm + review).
- Không sàn nào trả **429** hay **403** trong cả tuần (kể cả lần đo Tiki 01/11). Các sàn chặn bằng
  cách khác: Tiki trả trang chống bot kèm mã 200, tgdd không nhận kết nối.
- `crawl_full` mỗi đêm: cellphones ~12 phút, fptshop ~142 phút, hoangha ~149 phút, tiki ~218 phút.

### Tiki: chặn mềm ngắt quãng

- Ở 0,2 req/s, Tiki vẫn thỉnh thoảng trả trang chống bot (HTTP 200, HTML) thay vì JSON, khoảng 1–3 đợt
  mỗi ngày, mỗi đợt vài phút (ví dụ 02/11 22h, 03/11 11–12h, 05/11 00–03h UTC).
- `HttpClient` giảm tốc rồi thử lại; hết 6 lần vẫn bị chặn thì bỏ trang đó. Cả tuần bỏ **19 trang**
  (1–5 trang/ngày trên ~2.900 trang hot/ngày, < 0,2%). Vòng sau lấy lại bình thường.
- Chưa lần nào bị chặn liên tục tới mức `PlatformBlocked` dừng sàn.
- Kết luận: **giữ 0,2 req/s; chưa tăng số trang hot từ 60 lên 120** cho tới khi tần suất chặn mềm giảm.

### Thế Giới Di Động: chặn ở mức kết nối

- 02/11–03/11 ở 0,1 req/s (hot) + 0,03 req/s (full): ban ngày chỉ 1–3 lần thử lại mỗi giờ; mỗi khi
  `crawl_full` chạy song song (tổng ~0,13 req/s) thì lên 70–90 lần/giờ, toàn `ConnectTimeout`.
- 03/11: thử giảm tốc mỗi lần timeout -> tốc độ kẹt ở mức sàn, mỗi vòng hot từ ~7 phút thành ~1 giờ 50
  phút. Đã bỏ (timeout giờ chỉ thử lại, không giảm tốc).
- 04/11 ~05:00 UTC: ~1/4 request bị `ConnectTimeout`; thử mở kết nối TCP trực tiếp tới
  222.255.236.148:443 thì **4/5 lần treo 15 giây**, tức tgdd không nhận kết nối từ IP của nhóm.
  Đã **tạm dừng tgdd ~10 giờ**; 15:27 UTC thử lại 5/5 kết nối thành công trong ~0,1 giây.
- Từ 04/11 tối chạy lại ở **0,05 req/s, không chạy `crawl_full` cho tgdd**: 35–41 lần thử lại/ngày,
  mỗi vòng hot ổn định ~12,6 phút (lâu nhất 13,8 phút), không mất trang nào.
- Kết luận: **tgdd chịu tải kém nhất. Tổng tốc độ tới tgdd giữ ≤ 0,05–0,07 req/s**; chỉ đưa tgdd trở lại
  `crawl_full` (0,02 req/s) sau khi `crawl_hot` ổn định, và theo dõi số `ConnectTimeout`.

### CellphoneS, Hoàng Hà, FPT Shop

- Chạy ổn định cả tuần ở tốc độ trong bảng, không 429/403, không trang chống bot.
- Lỗi lẻ tẻ (timeout, 503) đều thử lại thành công. Trang rỗng còn lại là trang sản phẩm không có
  JSON-LD/thông số (sản phẩm mới, trang giới thiệu), không phải bị chặn.
- Chưa có số đo ở tốc độ cao hơn và **không có ý định đo**: tốc độ hiện tại đủ cho mục tiêu của dự án,
  README cấm tăng rate limit.
