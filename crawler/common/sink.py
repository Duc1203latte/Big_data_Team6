'''Nơi ghi event ra ngoài (sink)'''

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path 
import orjson
from common.schema import Event 

class EventSink(ABC):
    '''Interface của mọi sinh'''
    @abstractmethod
    def write(self, events: Sequence[Event]) -> None:
        '''Ghi một lô event. Khi hàm trả về, dữ liệu phải nằm an toàn ở đích'''

    @abstractmethod
    def close(self) -> None:
        '''Giải phóng tài nguyên. Gọi nhiều lần không lỗi'''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

class FileSink(EventSink):
    """Ghi event ra JSONL theo partition (README mục 7):

        landing/<event_type>/platform=<sàn>/date=<YYYY-MM-DD>/hour=<HH>/part-<run_id>.jsonl

    - date/hour lấy từ event_time (đã là UTC), nên file tự "rotate" theo giờ.
    - Chỉ mở file ở chế độ append ("ab"): không bao giờ ghi đè hay xoá (append-only).
    - Mỗi lần write() mở file, ghi cả lô, đóng file ngay. Crawl chỉ ~0,2 request/giây
      nên chi phí mở file không đáng kể, đổi lại dữ liệu xuống đĩa ngay sau mỗi lô
      và không phải quản lý file handle khi crawler chạy nhiều ngày.
    """

    def __init__(self, landing_dir: str | Path, run_id: str) -> None:
        #run_id nằm trong tên file nên không được chứa dấu /
        if not run_id or "/" in run_id:
            raise ValueError(f"invalid run_id: {run_id!r}")
        self.landing_dir = Path(landing_dir)
        self.run_id = run_id
        self.events_written = 0 # dùng cho crawl_runs.events_count
        self._closed = False

    def partition_path(self, event: Event) -> Path:
        '''Đường dẫn file JSONL mà event này phải được ghi vào'''
        ts = event.event_time #đã chuẩn hóa UTC trong schema
        return (
            self.landing_dir
            / event.event_type
            / f"platform={event.platform}"
            / f"date={ts:%Y-%m-%d}"
            / f"hour={ts:%H}"
            / f"part-{self.run_id}.jsonl"
        )

    def write(self, events: Sequence[Event]) -> None:
        if self._closed:
            raise RuntimeError("FileSink is cloded")
        if not events:
            return
        
        #B1: serialize toàn bộ 16 bước, gom theo file đích
        #Nếu 1 event lỗi serialize thì chưa có gì bị ghi ra đĩa
        lines_by_path: dict[Path, list[bytes]] = defaultdict(list)
        for event in events:
            #mode="json": datetime -> chuỗi ISO 8601, ssu đó orjson encode ra bytes
            line = orjson.dumps(event.model_dump(mode = "json")) + b"\n"
            lines_by_path[self.partition_path(event)].append(line)
        #B2: ghi từng file, 1 file 1 lần write duy nhất
        for path, lines in lines_by_path.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as f:
                f.write(b"".join(lines))
        self.events_written += len(events)

    def close(self) -> None:
        self._closed = True

# Các loại sink chọn được qua biến SINK trong .env. Giai đoạn 2A thêm "kafka" (KafkaSink)
# ngay tại đây; jobs/ và platforms/ không phải sửa gì.
SINK_KINDS = ("file",)


def make_sink(kind: str, run_id: str, *, landing_dir: str | Path) -> EventSink:
    """Tạo sink cho một run theo cấu hình SINK (README tiêu chí 9: đổi sink chỉ cần sửa config).

    - "file": FileSink ghi JSONL vào landing_dir (giai đoạn 1).
    """
    if kind == "file":
        return FileSink(landing_dir, run_id)
    raise ValueError(f"unknown sink {kind!r}, supported: {', '.join(SINK_KINDS)}")
