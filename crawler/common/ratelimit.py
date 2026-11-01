"""Token bucket điều tiết tốc độ request của từng sàn (README mục 7).

Mỗi sàn một bucket. Bucket nạp `rate` token mỗi giây, chứa tối đa `capacity`
token; mỗi request tiêu 1 token, hết token thì chờ. capacity = 1 nghĩa là
không cho dồn request thành loạt (burst): các request luôn cách đều nhau.

Gặp 429/403 thì slow_down() giảm tốc một nửa; mỗi request thành công thì
recover() tăng lại 5%, tối đa về tốc độ gốc. Không có recover() thì vài lần
429 là crawler kẹt ở tốc độ sàn mãi mãi.
"""

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class TokenBucket:
    def __init__(self, rate: float, capacity: float = 1.0, min_rate: float | None = None) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.base_rate = rate           # tốc độ cấu hình trong platforms.yaml
        self.rate = rate                # tốc độ hiện tại (token/giây = request/giây)
        self.capacity = capacity
        # Giảm tốc nhiều lần cũng không thấp hơn mức sàn này (mặc định 1/16 tốc độ gốc)
        self.min_rate = min_rate if min_rate is not None else rate / 16
        self._tokens = capacity         # bắt đầu đầy: request đầu tiên đi ngay
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()     # nhiều coroutine cùng xin token thì xếp hàng lần lượt

    def _refill(self) -> None:
        """Cộng thêm token theo thời gian đã trôi qua kể từ lần cập nhật trước."""
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
        self._updated = now

    async def acquire(self) -> None:
        """Chờ tới khi có 1 token rồi lấy nó."""
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                # Thiếu bao nhiêu token thì ngủ đúng chừng ấy thời gian
                await asyncio.sleep((1 - self._tokens) / self.rate)

    def slow_down(self, factor: float = 0.5) -> None:
        """Giảm tốc khi sàn trả 429/403 (mặc định còn một nửa)."""
        old = self.rate
        self.rate = max(self.min_rate, self.rate * factor)
        logger.warning("Rate reduced from %.3f to %.3f req/s", old, self.rate)

    def recover(self, factor: float = 1.05) -> None:
        """Request thành công: tăng tốc dần trở lại, không vượt tốc độ gốc."""
        self.rate = min(self.base_rate, self.rate * factor)
