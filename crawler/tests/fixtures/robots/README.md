# Fixture robots.txt

Bản `robots.txt` thật của từng sàn, tải ngày 2026-11-04. `tests/test_robots.py` dùng các
file này để kiểm tra offline rằng trang đang bật trong `config/watchlist.csv` không bị cấm.

Đây là ảnh chụp tại một thời điểm, các sàn có thể đổi quy tắc bất cứ lúc nào. Khi chạy thật,
`HttpClient` luôn đọc bản mới từ sàn và đọc lại mỗi 24 giờ.
