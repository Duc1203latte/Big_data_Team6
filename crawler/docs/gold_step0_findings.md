# Bước 0: khảo sát Silver trước khi làm Gold

Bản dữ liệu: `silver_20261116` (01/11 → hết ngày UTC 15/11). Chạy bằng `python -m analysis.explore_silver --silver-root <thư mục>`.

## 1. Dữ liệu

- 3.408.376 dòng price, 11.032 dòng product, khớp `MANIFEST.txt`.
- Ngày hổng hoặc thiếu:
  - 01/11: ngày khởi động, chỉ có cellphones và hoangha, ít dòng.
  - Tiki 14/11: chỉ 2.499 dòng (bình thường khoảng 110.000). 13/11 và 15/11 cũng thấp hơn.
- Baseline gom về trung vị theo ngày, nên ngày hổng chỉ làm giảm `n_days`, không làm lệch giá. Không điền giá cho ngày hổng.

## 2. Đơn vị dữ liệu theo sàn

| Sàn | Một `product_key` là | `sku_id` | Dung lượng lấy từ |
| --- | --- | --- | --- |
| cellphones | một biến thể (slug có `128gb`) | luôn rỗng | tên (58%) |
| tgdd | một model + dung lượng; mỗi sku là một màu, tên có cả màu | màu | tên (dạng `8GB/256GB`) |
| fptshop | một model, **tên không có dung lượng** ("iPhone 16") | 1 sku từ trang danh sách (~660 lần quan sát) và vài sku từ trang chi tiết (1–2 lần) | specs hoặc giá; tên chỉ có 20% |
| hoangha | một sản phẩm; 37% key có nhiều sku | | tên (17%) |
| tiki | một sản phẩm, gần như 1 sku | | tên (3%, phần lớn là phụ kiện) |

Kết luận:

- **Baseline tính theo `(product_key, sku_id)`.** Sku chưa đủ 7 ngày thì dùng baseline của cả `product_key`.
- **Ghép sản phẩm theo `product_key`.** Tên đại diện là tên có nhiều lần quan sát nhất, sau khi bỏ màu sắc.
- Hoangha có **99 dòng `name_raw = "FLASH SALE"`**, tức tên bị nhãn khuyến mãi đè lên. Phải bỏ các dòng này khi chọn tên đại diện. Cần báo người làm crawler, nên sửa ở `platforms/hoangha.py` hoặc `common/normalize.py`.

## 3. Tên sản phẩm

- Chữ quảng cáo cần bỏ: `chính hãng` (43% số tên), `hàng chính hãng`, `hàng nhập khẩu` (7%), `| Chính hãng VN/A`, `Chính hãng Apple Việt Nam`, `- Chính hãng FPT`, `new`, `sale`, `hot`, `flash sale`. Bỏ cả tiền tố loại: `Điện thoại`, `Điện Thoại`, `Tai nghe`, `Laptop`.
- Dung lượng có nhiều kiểu viết: `8GB 128GB`, `8GB/128GB`, `(8GB/128GB)`, `16GB/512GB/8GPU`, `1TB`. Hai số thì số nhỏ là RAM, số lớn là bộ nhớ trong.
- Tên tiki rất dài, có cả mô tả. Tên tgdd có màu ở cuối.

## 4. `silver_product`

- Có `brand_raw` cho gần 100% sản phẩm. Riêng tiki chỉ 54% key có trang chi tiết, phần còn lại phải tách brand từ tên.
- Brand cần quy đổi về một tên: tgdd ghi `iPhone (Apple)`, `Ipad (Apple)`, `Macbook`, đều thành `apple`.
- Category mỗi sàn một kiểu đường dẫn, nên cần bảng quy về vài loại thô: `phone`, `tablet`, `laptop`, `watch`, `audio`, `accessory`.

## 5. Baseline

- 15.519 `(product_key, sku_id)` có giá; 4.257 trong số đó chưa đủ 7 ngày.
- Nhóm hot (≥ 6 lần quan sát mỗi ngày) có 6.339 đơn vị, trong đó **456 chưa đủ 7 ngày**. Đây là sản phẩm mới xuất hiện từ 06/11 đến 15/11, mỗi ngày đều có thêm. 222 đơn vị có sku khác cùng `product_key` đã đủ ngày, nên quy tắc dự phòng ở mục 2 phủ được. Số còn lại sẽ đủ 7 ngày theo thời gian. Muốn đạt tiêu chí "mọi sản phẩm hot có baseline" cần thêm trạng thái `status = insufficient` và định nghĩa rõ "hot" là nhóm đã crawl được ít nhất 7 ngày.
- Giá trong ngày rất ổn định: trung vị và phân vị 90 của dao động trong ngày đều 0%, phân vị 99 là 23% (flash sale). Gom về trung vị ngày là hợp lý.

## 6. Bẫy khi ghép

- **Phụ kiện nhắc tên máy**: "Ốp lưng cho iPhone 16 / 16 Plus / 16 Pro", "Cường lực Samsung A16". Trong các tên có "iphone 15", phần lớn là phụ kiện. **Bắt buộc chỉ ghép khi cùng loại** (category thô), và nên loại cặp có giá chênh nhau quá xa, ví dụ quá 3 lần.
- **Hậu tố model**: `16` / `16e` / `16 Plus` / `16 Pro` / `16 Pro Max`, `Note 15 Pro` / `Pro+`, `S24` / `S24 FE`, `5G` / `LTE`. So khớp hậu tố như một luật cứng.
- **Laptop**: cùng `M5 13 inch 16GB/512GB` nhưng khác GPU (8 hoặc 10) hoặc sạc (30W, 70W). Cần thêm cấu hình CPU/GPU vào khoá so khớp.
- **fptshop thiếu dung lượng trong tên**: chỉ ghép được với biến thể rẻ nhất ở sàn khác, hoặc phải lấy dung lượng từ specs.
