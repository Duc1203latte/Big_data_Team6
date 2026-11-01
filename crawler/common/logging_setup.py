"""Cấu hình log cho crawler: ra console (rich, có màu) vf ra file theo ngày
   File log: logs/crawler-YYYY-MM-DD.log, ngày theo UTC giống mọi thời gian khác
trong dự án. Crawler chạy nhiều ngày liền nên handler tự chuyển sang file mới
khi qua ngày."""

import logging
import os 
import time 
from datetime import datetime, timezone
from pathlib import Path 
from rich.logging import RichHandler

#Định dạng dòng log trong file. Giờ ghi theo UTC, có chũ Z ỏ cuối cho rõ
FILE_FORMAT = "%(asctime)sZ %(levelname)-7s %(name)s: %(message)s"
FILE_DATEFMT = "%Y-%m-%dT%H:%M:%S"

class DailyFileHandler(logging.FileHandler):
    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self._current_date = self._today()
        super().__init__(self._path_for(self._current_date), mode="a", encoding="utf-8")

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _path_for(self, date: str) -> str:
        return str(self.log_dir / f"crawler-{date}.log")

    def emit(self, record: logging.LogRecord) -> None:
        today = self._today()
        if today != self._current_date:
            if self.stream is not None:
                self.stream.close()
                self.stream = None
            self._current_date = today
            self.baseFilename = os.path.abspath(self._path_for(today))
        super().emit(record)

def setup_logging(log_dir: str | Path = "logs", level: str | None = None) -> None:
    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    # Console: rich tự thêm giờ và màu theo level
    console = RichHandler(rich_tracebacks=True, show_path=False)
    console.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    root.addHandler(console)

    # File: giờ UTC (converter = time.gmtime) để khớp event_time và crawl_runs
    file_formatter = logging.Formatter(FILE_FORMAT, datefmt=FILE_DATEFMT)
    file_formatter.converter = time.gmtime
    file_handler = DailyFileHandler(log_dir)
    file_handler.setFormatter(file_formatter)
    root.addHandler(file_handler)

    # httpx log mọi request ở mức INFO, quá ồn; chỉ giữ cảnh báo trở lên
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
