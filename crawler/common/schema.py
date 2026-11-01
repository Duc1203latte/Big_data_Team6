"""Schema sự kiện của crawler: hợp đồng dữ liệu với Kafka, Bronze, Silver"""

from datetime import datetime, timezone
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from common.ids import make_event_id, normalize_event_time

SCHEMA_VERSION = 1

#Loại trang được fetch: trang danh sách (khoảng 40 sản phẩm) hay trang chi tiết
FetchKind = Literal["listing", "detail"]

#Dầu vào/ đầu ra của Platform.fetch
class ProductRef(BaseModel):
    '''Trỏ tói một trang cần fetch: trang danh sách hoạc trang chi tiết'''
    model_config = ConfigDict(extra="forbid")

    platform: str
    kind: FetchKind
    url: str
    product_id: str | None = None
    category: str | None = None
    page: int | None = None

class RawResponse(BaseModel):
    '''Payload thô do fetch() trả về, chưa parse, parse() chỉ đọc từ object này'''
    model_config = ConfigDict(extra="forbid")

    ref: ProductRef
    status_code: int
    body: str
    content_type: str | None = None
    fetched_at: datetime #thời điểm nhận response, UTC; nguồn của event_time

    @field_validator("fetched_at")
    @classmethod
    def _fetched_at_utc(cls, v: datetime) -> datetime:
        #Bắt buộc có timezone rồi đổi về UTC. Giữ nguyên microsecond ở đây
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("fetched_at must be timezone-aware")
        return v.astimezone(timezone.utc)

    @property
    def kind(self) -> FetchKind:
        '''Lối tắt tới ref.kind để parse() rẽ nhánh listing/detail'''
        return self.ref.kind

#Event
class BaseEvent(BaseModel):
    '''Các trường chung của mọi event, không dùng trực tiếp, chỉ để kế thừa'''
    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    event_id: str =""
    platform: str
    product_id: str
    url: str
    name_raw: str
    event_time: datetime
    crawler_version: str
    raw_hash: str

    @field_validator("event_time")
    @classmethod
    def _event_time_utc(cls, v:datetime) -> datetime:
        return normalize_event_time(v)

    def _sku_for_id(self) -> str | None:
        '''sku_id đưa vào event_id, lớp con nào có sku_id thì override'''
        return None

    @model_validator(mode="after")
    def _fill_event_id(self):
        #Sinh event_id sau khi event time đã được chuẩn hóa, để parser
        #Không thể tính sai hoặc quên. event_type do lớp con định nghĩa
        if not self.event_id:
            self.event_id = make_event_id(
                self.platform,
                self.product_id,
                self._sku_for_id(),
                self.event_type,
                self.event_time,
            )
        return self

class PriceEvent(BaseEvent):
    '''Giá, tồn kho, đánh giá, lượt bán tại một thời điểm. Loại event chính'''

    event_type: Literal["price"] = "price"
    sku_id: str | None = None
    source: FetchKind

    #Giá: cả số và chũ
    price_current: int | None = None
    price_current_raw: str | None = None
    price_original: int | None = None
    price_original_raw: str | None = None
    currency: str = "VND"

    #Tồn kho
    in_stock: bool | None = None
    stock_status_raw: str | None = None

    #Shop
    seller_id: str | None = None
    seller_name: str | None = None

    #Đánh giá và lượt mua của sản phẩm này trong shop này
    rating_avg: float | None = None
    rating_count: int | None = None
    review_count: int | None = None
    sold_count: int | None = None
    sold_count_raw: str | None = None

    def _sku_for_id(self) -> str | None:
        return self.sku_id

class ProductEvent(BaseEvent):
    '''Thông tin ít đổi của sản phẩm, lấy từ trang chi tiết, crawl thưa'''
    event_type: Literal["product"] = "product"
    brand_raw: str | None = None
    category_raw: str | None = None
    specs_raw: dict = Field(default_factory=dict)
    image_url: str | None = None

    #Phân bố đánh giá sao; None nếu sàn không công bố số lượt
    rating_avg: float | None = None
    rating_count: int | None = None
    rating_1_count: int | None = None
    rating_2_count: int | None = None
    rating_3_count: int | None = None
    rating_4_count: int | None = None
    rating_5_count: int | None = None

#Kiểu chung cho "một event bất kỳ", discriminator giúp đọc lại JSONL
#tự chọn đúng class theo trường event_type
Event = Annotated[PriceEvent | ProductEvent, Field(discriminator="event_type")]
