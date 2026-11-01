"""Đọc cấu hình từ file .env và biến môi trường (README mục 3.4).

Không dùng thư viện python-dotenv để khỏi cài thêm: file .env chỉ gồm các dòng
KEY=VALUE nên tự đọc là đủ.
"""

import os
from dataclasses import dataclass
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
    return Settings(
        crawler_version=os.getenv("CRAWLER_VERSION", "0.1.0"),
        landing_dir=Path(os.getenv("LANDING_DIR", "./landing")),
        state_db=Path(os.getenv("STATE_DB", "./state/crawler.db")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        contact_email=email,
    )
