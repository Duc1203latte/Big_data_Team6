'''Sinh các loại id cho crawler'''

import hashlib
import uuid
from datetime import datetime, timezone

# Namespace cố định cho uuid5. KHONG ĐƯỢC ĐỔI: đổi giá trị này thì toàn bộ event_id sinh ra sẽ khác, replay sẽ nhân đôi dữ liệu
EVENT_NAMESPACE = uuid.UUID("534c3b5d-c36b-5ae5-a8b6-46a926363fa4")
def normalize_event_time(ts:datetime) -> datetime:
    '''Đưa thời điểm về UC, cắt bỏ phần microsecond (làm tròn xuống giây)'''

    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("event_time must e timezone-aware")
    return ts.astimezone(timezone.utc).replace(microsecond=0)

def make_event_id (
    platform: str,
    product_id: str,
    sku_id: str | None,
    event_type: str,
    event_time: datetime,
) -> str:
    """Sinh event_id = uuid5(namespace, "platform|product_id|sku_id|event_type|event_time").
    - event_time được chuẩn hoá ngay trong hàm này, nên dù truyền vào giờ +07:00
      hay có microsecond thì kết quả vẫn giống nhau.
    - sku_id = None được ghi thành chuỗi rỗng để key luôn có đủ 5 phần.
    """
    ts = normalize_event_time(event_time)
    key = f"{platform}|{product_id}|{sku_id or ''}|{event_type}|{ts.isoformat()}"
    return str(uuid.uuid5(EVENT_NAMESPACE, key))

def make_product_key(platform: str, product_id: str) -> str:
    '''KHóa toàn cục của 1 sản phẩm'''
    return f"{platform}:{product_id}"

def make_raw_hash(payload: str | bytes) -> str:
    '''sha1 của payload thô (body của response), dùng để truy vết và phát hiện payload trùng'''
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    return hashlib.sha1(payload).hexdigest()

