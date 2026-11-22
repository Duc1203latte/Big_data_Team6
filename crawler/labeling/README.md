# Gán nhãn khớp sản phẩm (tuần 3, bước 2)

500 cặp sản phẩm ở hai sàn khác nhau, lấy mẫu phân tầng từ `er_candidates` (bản Gold dựng từ Silver 01–15/11/2026)
bằng `python -m jobs.er_labeling sample`. Mỗi người một file, khoảng 125 cặp riêng + 30 cặp chung cho cả nhóm
(để đo độ đồng thuận).

| File | Người gán |
| --- | --- |
| `labeler_1.csv` | Thành viên 1 |
| `labeler_2.csv` | Thành viên 2 |
| `labeler_3.csv` | Thành viên 3 |
| `labeler_4.csv` | Thành viên 4 |

**Không mở `pairs_key.csv`**: file này có điểm và kết quả của máy, xem trước sẽ bị ảnh hưởng khi gán.

## Cách gán

Mở file của mình bằng Excel/Numbers/Google Sheets, điền cột `label` cho từng dòng:

- `1`: **cùng một sản phẩm**: cùng hãng, cùng model, cùng đời, cùng dung lượng và RAM. Khác màu vẫn là `1`.
  Một bên không ghi dung lượng (fptshop hay bỏ) thì xem giá: giá gần nhau thì coi như cùng bản.
- `0`: khác sản phẩm (khác đời 15/16, khác bản Pro/Pro Max, khác dung lượng, phụ kiện với máy, khác hãng...).
- `?`: không chắc (ghi lý do vào cột `note`).

Không sửa cột khác, không xoá dòng. Lưu lại đúng tên file (CSV, UTF-8) rồi gửi lại hoặc commit.

## Tính kết quả (thành viên 2)

```bash
python -m jobs.er_labeling evaluate --labels labeling --target-precision 0.9
```

Ghi `labeling/report.md` (precision, recall theo ngưỡng, ngưỡng đề xuất, Cohen kappa) và `threshold_curve.csv`.
Sau đó chạy lại `entity_resolution` với `--threshold <ngưỡng đề xuất>` (và sửa ngưỡng mặc định trong DAG).
