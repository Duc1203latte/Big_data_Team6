"""Đọc cấu hình từ file .env và biến môi trường (README mục 3.4).

Không dùng thư viện python-dotenv để khỏi cài thêm: file .env chỉ gồm các dòng
KEY=VALUE nên tự đọc là đủ.
"""

import os
from dataclasses import dataclass, replace
from pathlib import Path


def load_env_file(path: str | Path = ".env") -> None:
    """Nạp KEY=VALUE từ file .env vào os.environ.

    Biến đã có sẵn trong môi trường thì giữ nguyên (không ghi đè), để có thể
    chạy tạm kiểu: LOG_LEVEL=DEBUG python -m jobs.measure_limits
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Settings:
    crawler_version: str
    landing_dir: Path
    state_db: Path
    log_level: str
    contact_email: str
    sink: str = "file"      # nơi ghi event: "file" (JSONL vào landing_dir); 2A thêm "kafka"

    @property
    def user_agent(self) -> str:
        """User-Agent ghi rõ là crawler học thuật kèm email, không giả trình duyệt (README mục 7)."""
        return (
            f"bigdata-crawler/{self.crawler_version} "
            f"(academic research project; contact: {self.contact_email})"
        )


def load_settings(env_file: str | Path = ".env") -> Settings:
    load_env_file(env_file)
    email = os.getenv("CONTACT_EMAIL", "").strip()
    # Chưa điền email (còn placeholder "<...>") thì dừng luôn, không gửi request
    if not email or "<" in email:
        raise RuntimeError("CONTACT_EMAIL is not set in .env")
    # Sai tên sink thì dừng ngay lúc khởi động, không đợi tới lúc ghi event đầu tiên
    from common.sink import SINK_KINDS     # import ở đây để config không kéo theo schema khi không cần
    sink = os.getenv("SINK", "file").strip().lower()
    if sink not in SINK_KINDS:
        raise RuntimeError(f"SINK={sink!r} in .env is not supported (supported: {', '.join(SINK_KINDS)})")
    return Settings(
        crawler_version=os.getenv("CRAWLER_VERSION", "0.1.0"),
        landing_dir=Path(os.getenv("LANDING_DIR", "./landing")),
        state_db=Path(os.getenv("STATE_DB", "./state/crawler.db")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        contact_email=email,
        sink=sink,
    )


# ---------------------------------------------------------------------------
# Cấu hình từng sàn: config/platforms.yaml
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PlatformLimits:
    requests_per_second: float
    max_concurrency: int = 1
    timeout_seconds: float = 15
    # Sàn dùng API JSON: response 2xx mà không phải JSON (ví dụ trang kiểm tra chống bot
    # của tường lửa, trả mã 200) bị coi là BỊ CHẶN, xử lý như 403.
    expect_json: bool = False
    # Dừng sàn ngay ở lần bị chặn đầu tiên (403, 429, chặn mềm), không thử lại hay giảm tốc.
    # Dùng cho sàn dễ chặn, nơi gửi thêm request lúc đang bị chặn có thể làm tình hình nặng hơn.
    stop_on_first_block: bool = False
    # Chu kỳ crawl nhóm hot (phút). Sàn có trang nặng (CellphoneS) để thưa hơn.
    hot_cycle_minutes: int = 30
    # crawl_full chạy song song với crawl_hot nên dùng tốc độ riêng, thấp hơn
    # (None = một nửa requests_per_second), và giới hạn số trang chi tiết mỗi lần chạy.
    full_requests_per_second: float | None = None
    max_details_per_run: int = 300

    def for_full_crawl(self) -> "PlatformLimits":
        """Bản sao cấu hình với tốc độ dành cho crawl_full."""
        rate = self.full_requests_per_second or self.requests_per_second / 2
        return replace(self, requests_per_second=rate)


def load_platform_limits(path: str | Path = "config/platforms.yaml") -> dict[str, PlatformLimits]:
    """Đọc platforms.yaml -> {"tiki": PlatformLimits(...), ...}. Key lạ trong yaml sẽ báo lỗi."""
    import yaml     # import ở đây để các job không cần yaml vẫn import config được

    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return {name: PlatformLimits(**values) for name, values in data.items()}
