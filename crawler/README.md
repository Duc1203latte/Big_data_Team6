# Crawler đa sàn TMĐT — Giai đoạn 1 của dự án Lưu trữ và Xử lý Dữ liệu lớn

Tài liệu này là spec cho phần crawler. Dành cho Claude Code đọc trước khi làm bất cứ việc gì trong repo này.

---

## QUY TẮC LÀM VIỆC (đọc trước, không được bỏ qua)

**KHÔNG ĐƯỢC TỰ Ý VIẾT CODE KHI CHƯA ĐƯỢC YÊU CẦU.**

Cụ thể:

- Mỗi lần chỉ làm đúng một bước mà mình chỉ định. Làm xong thì dừng, báo cáo, chờ lệnh tiếp.
- Không tự nhảy sang bước sau trong mục "Thứ tự thực hiện" dù thấy bước đó hiển nhiên phải làm.
- Không tự tạo file, không tự cài thêm thư viện, không tự refactor code đã chạy được, không tự sửa file ngoài phạm vi bước đang làm.
- Không tự chạy crawler thật khi chưa được cho phép. Test thì dùng fixture offline hoặc tối đa 5 request.
- Trước khi code, nếu thấy spec thiếu hoặc mâu thuẫn thì hỏi lại, đừng tự suy diễn rồi làm.
- Nếu trong lúc làm phát hiện cách tốt hơn spec, nói ra trước, chờ mình đồng ý, không tự đổi.

Cách làm việc mong muốn: mình nói "làm bước N", Claude Code đọc spec bước đó, hỏi lại nếu cần, viết code, chạy test, báo kết quả, dừng.

**Yêu cầu về code:** comment chi tiết bằng tiếng Việt. Tên biến, tên hàm, log message bằng tiếng Anh.

---

## 1. Bối cảnh

Đây là bài tập lớn môn Lưu trữ và Xử lý Dữ liệu lớn, nhóm 4 người, 5 tuần (28/10 đến 28/11/2026). Đề tài: phát hiện bất thường giá và hàng giả trên nhiều sàn thương mại điện tử.

Kiến trúc tổng thể của cả dự án:

```
Crawler  →  Kafka  →  Bronze (HDFS)  →  Silver (Delta)  →  Gold
                                              ↓               ↓
                                      Speed layer      Batch layer
                                              ↓               ↓
                                     ClickHouse + Redis → Dashboard
```

Repo này **chỉ làm phần Crawler**, tức mắt trái ngoài cùng của sơ đồ. Các tầng sau do thành viên khác và các tuần sau làm.

Phạm vi sản phẩm: hàng điện tử và phụ kiện (điện thoại, laptop, tablet, smartwatch, tai nghe, sạc, cáp, pin dự phòng).

Quy mô mục tiêu:

| Nhóm theo dõi | Số SKU | Chu kỳ | Tốc độ |
| --- | --- | --- | --- |
| Theo dõi sát (hot) | 10.000 | 30 phút | ~0,14 request/giây (~250 trang danh sách/chu kỳ) |
| Toàn danh mục (full) | 200.000 | 1 ngày | ~0,06 request/giây (~5.000 trang danh sách/ngày) |

Tổng khoảng 0,2 request/giây. Tốc độ rất thấp và cố tình như vậy, không được tăng.

Để đạt số SKU trên với tốc độ này, **giá lấy từ trang danh sách** (mỗi request ~40 sản phẩm), không gọi trang chi tiết từng sản phẩm. Trang chi tiết chỉ dùng để lấy `ProductEvent` (thông số, phân bố sao), gọi thưa. Nhóm hot được định nghĩa là N trang đầu của các danh mục chọn sẵn, tổng ~250 trang. SKU có thể trôi ra/vào nhóm hot khi thứ tự trang danh sách thay đổi, chấp nhận điều này.

## 2. Nguyên tắc thiết kế (quan trọng nhất)

Bốn nguyên tắc này quyết định toàn bộ cấu trúc code. Vi phạm là phải viết lại.

**1. Crawler ngu, Silver mới thông minh.** Crawler chỉ ép kiểu dữ liệu và đưa thời gian về UTC. Trường nào phải ép kiểu (giá, tồn kho, lượt bán) thì giữ thêm bản gốc ở trường `*_raw`, để khi parser ép sai thì Silver tính lại được từ Bronze mà không phải crawl lại. Không tính `discount_pct`, không match sản phẩm giữa các sàn, không loại giá outlier, không chuẩn hoá tên sản phẩm. Những việc đó thuộc tầng Silver ở tuần sau.

**2. Sink thay được.** Giai đoạn 1 ghi ra file JSONL. Giai đoạn 2A (01/11) đổi sang Kafka. Lúc đó chỉ được thêm một class mới trong `common/sink.py`, không được sửa file nào trong `platforms/`. Nếu phải sửa parser khi đổi sink, tức là thiết kế sai.

**3. `event_id` sinh deterministic.** Tuần 2 sẽ replay toàn bộ file JSONL của giai đoạn 1 vào Kafka. Silver dedup theo `event_id`, nên cùng một lần crawl phải luôn sinh ra cùng `event_id`, chạy lại bao nhiêu lần cũng vậy. Dùng `uuid4` là sai, replay sẽ nhân đôi dữ liệu.

**4. Append-only.** File trong `landing/` không bao giờ được sửa hay xoá. Nó là nguồn sự thật duy nhất cho đến khi Bronze trên HDFS hoạt động.

## 3. Thiết lập môi trường

Môi trường chạy: WSL (Ubuntu) trên Windows, mở bằng VS Code. Máy 16 GB RAM.

### 3.1. Kiểm tra Python

```bash
python3 --version   # cần 3.11 trở lên
```

Nếu chưa có:

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip
```

### 3.2. Tạo virtualenv

```bash
cd ~/projects/bigdata-crawler/crawler     # repo Big_data_Team6, phần crawler nằm trong crawler/
python3 -m venv .venv
source .venv/bin/activate
```

Mỗi terminal mới đều phải `source .venv/bin/activate`. Trong VS Code chọn interpreter `.venv/bin/python` (Ctrl+Shift+P → Python: Select Interpreter).

### 3.3. Thư viện

`requirements.txt`:

```
httpx[http2]==0.27.*
pydantic==2.*
pyyaml==6.*
tenacity==9.*
orjson==3.*
beautifulsoup4==4.*
python-dateutil==2.9.*
rich==13.*
pandas==2.*
pytest==8.*
pytest-asyncio==0.24.*
```

```bash
pip install -r requirements.txt
```

Chưa cài Playwright ở bước này. Chỉ cài khi gặp sàn bắt buộc phải render JavaScript, và phải hỏi mình trước:

```bash
pip install playwright==1.4*
playwright install chromium
playwright install-deps   # WSL cần thêm lib hệ thống
```

Chưa cài thư viện Kafka ở giai đoạn 1. Sang 2A mới thêm `confluent-kafka`.

### 3.4. File cấu hình môi trường

Tạo `.env.example` (commit) và `.env` (không commit):

```
CRAWLER_VERSION=0.1.0
LANDING_DIR=./landing
STATE_DB=./state/crawler.db
LOG_LEVEL=INFO
CONTACT_EMAIL=<email để ghi vào User-Agent>
```

`.gitignore` cần có: `.venv/`, `.env`, `landing/`, `state/`, `logs/`, `__pycache__/`, `.pytest_cache/`.

### 3.5. Chạy dài ngày trên WSL

Crawler phải chạy liên tục nhiều ngày. WSL không có systemd dùng được mặc định, nên dùng `tmux`:

```bash
sudo apt install -y tmux
tmux new -s crawler
source .venv/bin/activate
python -m jobs.crawl_hot
# Ctrl+B rồi D để thoát ra, crawler vẫn chạy
tmux attach -t crawler   # vào lại xem log
```

Lưu ý: Windows sleep hoặc hibernate sẽ treo WSL. Vào Windows Settings → Power → tắt sleep khi đang cắm sạc. Vẫn sẽ có khoảng trống dữ liệu khi tắt máy đi học, chuyện đó đã tính trước, xem mục 7.

## 4. Cây thư mục

```
bigdata-crawler/
├── README.md               # file này
├── requirements.txt
├── .env.example
├── .gitignore
├── config/
│   ├── platforms.yaml      # cấu hình từng sàn
│   └── watchlist.csv       # danh sách trang danh sách nhóm hot (sàn, danh mục, sắp xếp, số trang)
├── common/
│   ├── __init__.py
│   ├── schema.py           # pydantic model cho các loại event
│   ├── ids.py              # sinh event_id, product_key
│   ├── sink.py             # EventSink (abstract), FileSink; 2A thêm KafkaSink
│   ├── ratelimit.py        # token bucket theo từng sàn
│   ├── http.py             # httpx client, retry, backoff
│   ├── state.py            # SQLite: catalog, lần crawl cuối, các đợt chạy
│   ├── config.py           # đọc .env và platforms.yaml
│   └── logging_setup.py    # cấu hình log ra console và file
├── platforms/
│   ├── __init__.py
│   ├── base.py             # lớp trừu tượng Platform
│   ├── parsing.py          # hàm đọc HTML / JSON-LD dùng chung
│   ├── tiki.py
│   ├── cellphones.py
│   ├── hoangha.py
│   └── fptshop.py
├── jobs/
│   ├── __init__.py
│   ├── measure_limits.py   # đo giới hạn truy cập từng sàn
│   ├── crawl_hot.py        # 10.000 SKU mỗi 30 phút
│   └── crawl_full.py       # 200.000 SKU mỗi ngày
├── tests/
│   ├── fixtures/           # payload thật lưu lại, mỗi sàn một thư mục
│   └── test_parse_*.py
├── landing/                # dữ liệu ra, không commit
├── state/                  # SQLite, không commit
└── logs/                   # không commit
```

## 5. Schema sự kiện

Đây là hợp đồng giữa crawler và toàn bộ các tầng phía sau. Chốt rồi không sửa. Nếu buộc phải sửa thì tăng `crawler_version` và báo cả nhóm.

Bốn loại event: `price`, `product`, `review`, `seller`. Giai đoạn 1 chỉ làm `price` và `product`.

### 5.1. PriceEvent — loại chạy liên tục, quan trọng nhất

Quy ước chung cho mọi event:

- Thời điểm quan sát tên là `event_time` (không dùng `crawl_ts`), trùng tên với watermark ở speed layer, Silver và ClickHouse.
- `product_id` là id gốc của sàn, **không thêm tiền tố**. Khóa toàn cục dùng `product_key = "<platform>:<product_id>"`.
- `schema_version` tăng khi schema đổi; `crawler_version` là phiên bản code. Hai số độc lập.

### 5.1. PriceEvent — loại chạy liên tục, quan trọng nhất

```python
class PriceEvent(BaseModel):
    schema_version: int = 1
    event_id: str                     # uuid5, xem mục 5.3
    event_type: Literal["price"] = "price"
    platform: str                     # tiki | cellphones | hoangha | fptshop
    product_id: str                   # id gốc của sàn, KHÔNG tự sinh, KHÔNG thêm tiền tố
    sku_id: str | None                # biến thể màu/dung lượng, nếu sàn có
    url: str
    name_raw: str                     # tên y như trên sàn, chưa chuẩn hoá
    source: Literal["listing", "detail"]  # giá lấy từ trang danh sách hay trang chi tiết

    # Giá: cả dạng số lẫn dạng chữ gốc
    price_current: int | None         # VND, số nguyên; None nếu không đọc được
    price_current_raw: str | None     # "19.990.000đ"; API trả số thì giữ str(số)
    price_original: int | None        # giá niêm yết trước giảm
    price_original_raw: str | None
    currency: str = "VND"

    # Tồn kho
    in_stock: bool | None
    stock_status_raw: str | None      # "available", "Hết hàng"... giữ nguyên

    # Shop (chỉ sàn marketplace mới có)
    seller_id: str | None
    seller_name: str | None

    # Đánh giá và lượt mua của sản phẩm này trong shop này
    rating_avg: float | None          # sao trung bình 0–5
    rating_count: int | None          # số lượt chấm sao
    review_count: int | None          # số đánh giá có nội dung, nếu sàn tách riêng
    sold_count: int | None            # lượt bán đã ép kiểu ("5k+" -> 5000)
    sold_count_raw: str | None        # "Đã bán 5k+", để Silver biết là số ước lượng

    event_time: datetime              # UTC, bắt buộc có timezone, làm tròn xuống giây
    crawler_version: str              # lấy từ biến môi trường
    raw_hash: str                     # sha1 của payload thô
```

### 5.2. ProductEvent — thông tin ít đổi, crawl thưa hơn

Lấy từ trang chi tiết (và API review riêng nếu sàn có). Crawl thưa nhưng định kỳ, nên phân bố sao vẫn có chuỗi thời gian.

```python
class ProductEvent(BaseModel):
    schema_version: int = 1
    event_id: str
    event_type: Literal["product"] = "product"
    platform: str
    product_id: str
    url: str
    name_raw: str
    brand_raw: str | None
    category_raw: str | None          # đường dẫn danh mục của sàn, giữ nguyên
    specs_raw: dict                   # bảng thông số, giữ nguyên key của sàn
    image_url: str | None             # giai đoạn 1 chỉ lưu URL, chưa tải ảnh

    # Phân bố đánh giá sao; None nếu sàn không công bố
    rating_avg: float | None
    rating_count: int | None
    rating_1_count: int | None
    rating_2_count: int | None
    rating_3_count: int | None
    rating_4_count: int | None
    rating_5_count: int | None

    event_time: datetime
    crawler_version: str
    raw_hash: str
```

Nếu sàn chỉ công bố phần trăm từng mức sao thay vì số lượt, không tự nhân ra số lượt: để các cột `rating_N_count` là `None` và giữ phần trăm gốc trong `specs_raw`.

### 5.3. Quy tắc sinh ID

```python
NS = uuid.UUID("<cố định 1 UUID, viết thẳng vào ids.py>")

def make_event_id(platform, product_id, sku_id, event_type, event_time) -> str:
    key = f"{platform}|{product_id}|{sku_id or ''}|{event_type}|{event_time.isoformat()}"
    return str(uuid.uuid5(NS, key))

def make_product_key(platform, product_id) -> str:
    return f"{platform}:{product_id}"
```

`event_time` phải làm tròn xuống giây trước khi đưa vào `make_event_id`, không để microsecond, nếu không replay sẽ không khớp. Có `sku_id` trong key để hai biến thể của cùng sản phẩm lấy trong cùng một giây không bị trùng id.

### 5.4. Quy tắc với dữ liệu thiếu

Trường nào không đọc được thì để `None`, **không được bỏ qua sản phẩm**. Bản ghi thiếu giá vẫn có giá trị: tuần sau nó chứng minh tầng quarantine ở Silver hoạt động. Không được tự điền giá 0, không được đoán.

Không ép kiểu thất bại thành ngoại lệ làm chết cả vòng crawl. Parse lỗi một sản phẩm thì log cảnh báo, ghi `None`, đi tiếp.

## 6. Interface của một sàn

Mỗi sàn chỉ implement 3 hàm trong `platforms/base.py`:

```python
class Platform(ABC):
    name: str

    @abstractmethod
    async def discover(self, category: str) -> AsyncIterator[ProductRef]:
        """Liệt kê sản phẩm trong một danh mục, trả về product_id + url.
        Dùng cho crawl_full để dựng danh sách SKU."""

    @abstractmethod
    async def fetch(self, ref: ProductRef) -> RawResponse:
        """Gọi API hoặc tải trang. Trả về payload THÔ, chưa parse.
        ref có thể trỏ tới trang danh sách (kind="listing") hoặc trang chi tiết (kind="detail").
        Chỉ hàm này được phép đi ra mạng."""

    @abstractmethod
    def parse(self, raw: RawResponse) -> list[Event]:
        """Payload thô -> danh sách event đúng schema.
        raw.kind="listing": một payload sinh ~40 PriceEvent (source="listing").
        raw.kind="detail": sinh ProductEvent (và PriceEvent source="detail" nếu có giá).
        Hàm thuần: không mạng, không I/O, không thời gian hiện tại.
        event_time lấy từ raw.fetched_at."""
```

`RawResponse` có trường `kind: Literal["listing", "detail"]` để `parse` biết đang xử lý loại payload nào.

`parse` bắt buộc là hàm thuần để test offline bằng fixture. Mỗi sàn phải có ít nhất 3 fixture: một sản phẩm bình thường, một sản phẩm hết hàng, một sản phẩm thiếu trường (không giá hoặc không thông số), cộng ít nhất 1 fixture trang danh sách.

### Cách lấy dữ liệu, theo thứ tự ưu tiên

1. **API JSON ẩn** của sàn. Mở trang sản phẩm trên Chrome → DevTools → Network → Fetch/XHR → tìm request trả về JSON chứa giá. Nhanh hơn, nhẹ hơn, không vỡ khi sàn đổi CSS. Tiki có sẵn API kiểu này.
2. **JSON-LD trong HTML** (`<script type="application/ld+json">`). CellphoneS dùng cách này cho giá và tồn kho (cần kiểm chứng ở bước 8).
3. **Parse HTML tĩnh** bằng selector.
4. **Playwright**, chỉ khi ba cách trên không được, và phải hỏi mình trước. Với 10.000 SKU mỗi 30 phút thì Playwright toàn bộ sẽ không kịp.

### Danh sách sàn

| Sàn | Kiểu | Ghi chú |
| --- | --- | --- |
| Tiki | API JSON | Làm trước. Marketplace nên có `seller_id`, cần cho phần phát hiện hàng giả |
| CellphoneS | JSON-LD (kiểm chứng ở bước 8) | Viết mới |
| Hoàng Hà Mobile | HTML (kiểm chứng ở bước 8) | Viết mới |
| FPT Shop | HTML | Làm cuối |

Shopee và Lazada chặn rất gắt (device fingerprint, signed request). **Không đưa vào kế hoạch.** Nếu sau này rảnh thì thử, hỏng thì bỏ, không được để tiến độ phụ thuộc vào hai sàn này.

## 7. Chính sách crawl

### Rate limit

Token bucket riêng cho từng sàn, cấu hình trong `config/platforms.yaml`. Giá trị khởi đầu, chỉnh lại sau khi `measure_limits.py` có số thật:

```yaml
tiki:
  requests_per_second: 0.2
  max_concurrency: 2
  timeout_seconds: 15
```

Mức 0,2 khớp tổng mục tiêu ở mục 1. Các mức cao hơn (0,5 → 2 request/giây) chỉ dùng trong `measure_limits.py` ở bước 5.

- Đọc `robots.txt` của sàn trước khi crawl, tôn trọng `Disallow`.
- User-Agent ghi rõ là crawler học thuật kèm email liên hệ, không giả mạo browser thật.
- Gặp 429 hoặc 403: backoff theo số mũ (5s, 10s, 20s, 40s, tối đa 5 phút), ghi log, giảm rate của sàn đó một nửa.
- Gặp 403 liên tục quá 10 lần: dừng sàn đó, ghi vào bảng giới hạn API, báo mình. **Không tìm cách lách chống bot** (không rotate proxy, không giả fingerprint, không giải captcha). Sàn chặn cũng là một kết quả để viết vào báo cáo.
- Chỉ crawl trang công khai, không đăng nhập, không lấy dữ liệu cá nhân của người mua (tên người review thì hash lại).

### Ghi lại khoảng trống dữ liệu

Máy sẽ tắt khi mình đi học, nên khoảng trống là chắc chắn có. Phải ghi lại để tuần 5 giải thích được lỗ hổng trên dashboard.

Bảng trong SQLite:

```sql
CREATE TABLE crawl_runs (
    run_id       TEXT PRIMARY KEY,
    job          TEXT,      -- crawl_hot | crawl_full
    platform     TEXT,
    started_at   TEXT,      -- UTC ISO8601
    ended_at     TEXT,      -- NULL nếu đang chạy hoặc bị kill
    status       TEXT,      -- running | ok | failed | killed
    events_count INTEGER,
    error        TEXT
);
```

Mỗi lần job bật thì insert `running`, kết thúc bình thường thì update `ok`. Lần chạy sau nếu thấy dòng `running` cũ còn treo thì đánh `killed`. Cuối dự án tính được uptime và danh sách khoảng trống.

### Phân vùng file ra

```
landing/<event_type>/platform=<tên sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-<run_id>.jsonl
```

`date` và `hour` theo UTC, không theo giờ Việt Nam. Partition này phải khớp hệ thống partition của Bronze ở 2A để replay không phải tính lại. Rotate file theo giờ, không theo kích thước. Mỗi dòng là một JSON object, encode bằng `orjson`.

### Không crawl review ở giai đoạn 1

Review có timestamp nên tuần sau backfill lại được. Giá thì không: không có nguồn lịch sử nào, bỏ giờ nào là mất giờ đó. Tuần này dồn toàn bộ cho `price`.

## 8. Thứ tự thực hiện

Làm từng bước, mỗi bước xong thì dừng và báo. **Không tự nhảy bước.**

**Bước 1 — `common/schema.py` + `common/ids.py`**
Model pydantic cho `PriceEvent`, `ProductEvent`, `ProductRef`, `RawResponse`. Hàm sinh id. Test: tạo 2 event cùng input, `event_id` phải giống nhau; đổi `event_time` 1 giây thì phải khác nhau; đổi `sku_id` thì phải khác nhau.

**Bước 2 — `common/sink.py`**
`EventSink` abstract với `write(events: list[Event]) -> None` và `close()`. `FileSink` ghi JSONL theo partition ở mục 7, tự tạo thư mục, flush theo batch. Test: ghi 100 event, đọc lại bằng pandas, đủ 100 dòng, đúng thư mục.

**Bước 3 — `common/logging_setup.py` + `common/state.py`**
Log ra console (rich) và file `logs/crawler-YYYY-MM-DD.log`. SQLite với bảng `crawl_runs` và `catalog(product_key, platform, product_id, url, first_seen, last_seen, is_hot)`. Test: insert, update, query lại.

**Bước 4 — `platforms/base.py` + `platforms/tiki.py`**
Interface và sàn đầu tiên. Trước khi viết `parse`, lưu fixture thật vào `tests/fixtures/tiki/` (gọi tay, tối đa 5 request): 1 trang danh sách + 3 trang chi tiết. Đồng thời kiểm chứng: API danh sách có `seller_id`, `rating_avg`, `rating_count`, `sold_count` không; API nào trả phân bố 1–5 sao. Báo kết quả trước khi viết `parse`. Test `parse` chạy hoàn toàn offline trên fixture.

**Bước 5 — `jobs/measure_limits.py`**
Bắn thử tăng dần 0,2 → 0,5 → 1 → 2 request/giây, mỗi mức 2 phút, ghi lại mốc bắt đầu xuất hiện 429/403 và thời gian phản hồi trung bình. Xuất ra `docs/api_limits.md`. Số liệu này cần cho báo cáo và cho mục 2 của bản đề xuất. **Chỉ chạy khi mình cho phép.**

**Bước 6 — `common/ratelimit.py` + `common/http.py`**
Token bucket theo số đo bước 5. httpx client dùng chung, retry bằng tenacity, backoff theo số mũ, timeout, User-Agent theo mục 7.

**Bước 7 — `jobs/crawl_hot.py`**
Đọc `config/watchlist.csv` (danh sách trang danh sách nhóm hot), crawl vòng tròn mỗi 30 phút, ghi qua `FileSink`, ghi `crawl_runs`, xử lý SIGINT/SIGTERM để đóng file và update trạng thái tử tế. Sau bước này dữ liệu bắt đầu tích lũy, crawler chạy liên tục không dừng nữa.

**Bước 8 — CellphoneS và Hoàng Hà**
Viết mới 2 sàn theo interface `Platform` (không có code cũ). Trước khi viết `parse`, mỗi sàn gọi tay tối đa 5 request để chọn cách lấy dữ liệu theo thứ tự ưu tiên ở mục 6 và lưu fixture. Thông số giữ thô trong `specs_raw`, không gọi LLM.

**Bước 9 — `jobs/crawl_full.py` + FPT Shop**
`discover` toàn danh mục, cập nhật bảng `catalog`, chạy 1 lần mỗi ngày. Thêm sàn thứ tư.

Bước 8 và 9 làm song song trong lúc crawler đã chạy, không cần chờ xong hết mới bật.

## 9. Tiêu chí hoàn thành giai đoạn 1

- [ ] `crawl_hot` chạy một vòng ra JSONL đúng schema, đọc lại bằng pandas không lỗi
- [ ] Đổi sink chỉ cần sửa config, không sửa file nào trong `platforms/`
- [ ] Ctrl+C giữa chừng rồi chạy lại: không sinh `event_id` trùng
- [ ] Mỗi sàn có fixture, `pytest` pass hoàn toàn offline
- [ ] `docs/api_limits.md` có số đo thật của từng sàn
- [ ] Bảng `crawl_runs` ghi đúng mỗi lần bật tắt
- [ ] Crawler chạy liên tục trong tmux, dữ liệu vào `landing/` theo đúng partition

## 10. Những điều không được làm

- Không tự ý code khi chưa được yêu cầu (nhắc lại lần nữa cho chắc)
- Không sửa hay xoá file đã có trong `landing/`
- Không tính toán phái sinh trong crawler (`discount_pct`, chuẩn hoá tên, match sản phẩm giữa sàn)
- Không gọi LLM/OpenAI trong luồng crawl
- Không tăng rate limit để chạy nhanh hơn
- Không lách chống bot
- Không cài thêm thư viện ngoài `requirements.txt` mà chưa hỏi
- Không dùng `uuid4` cho `event_id`
- Không commit `landing/`, `state/`, `logs/`, `.env`