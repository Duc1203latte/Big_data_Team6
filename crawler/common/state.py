"""Trạng thái của crawler, lưu trong SQLite
Hai bảng: crawl_runs, catalog: danh sách sản phẩm đã thấy"""

import logging
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from common.ids import make_product_key

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id       TEXT PRIMARY KEY,
    job          TEXT NOT NULL,     -- crawl_hot | crawl_full
    platform     TEXT NOT NULL,
    started_at   TEXT NOT NULL,     -- UTC ISO8601
    ended_at     TEXT,              -- NULL nếu đang chạy hoặc bị kill
    status       TEXT NOT NULL,     -- running | ok | failed | killed
    events_count INTEGER NOT NULL DEFAULT 0,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_crawl_runs_job_status ON crawl_runs (job, platform, status);

CREATE TABLE IF NOT EXISTS catalog (
    product_key TEXT PRIMARY KEY,   -- "<platform>:<product_id>"
    platform    TEXT NOT NULL,
    product_id  TEXT NOT NULL,
    url         TEXT,
    first_seen  TEXT NOT NULL,
    last_seen   TEXT NOT NULL,
    is_hot      INTEGER NOT NULL DEFAULT 0,  -- SQLite không có kiểu bool: 0/1
    detail_at   TEXT                         -- lần cuối lấy trang chi tiết (ProductEvent), NULL = chưa lấy
);
CREATE INDEX IF NOT EXISTS idx_catalog_platform ON catalog (platform);
"""

RUN_STATUSES = {"running", "ok", "failed", "killed"}

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond = 0)

def to_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone aware")
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def make_run_id(job: str, platform: str, started_at: datetime) -> str:
    return f"{job}-{platform}-{started_at.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"

class StateStore:
    def __init__(self, db_path: str | Path) -> None:
        if str(db_path) != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row 
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA_SQL)
        self._migrate()

    def _migrate(self) -> None:
        """DB tạo từ phiên bản cũ (trước bước 9) chưa có cột catalog.detail_at: thêm vào."""
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(catalog)")}
        if "detail_at" not in columns:
            with self.conn:
                self.conn.execute("ALTER TABLE catalog ADD COLUMN detail_at TEXT")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    '''CRAWL RUNS'''
    def start_run(self, job:str, platform:str, started_at: datetime | None = None) -> str:
        started_at = started_at or utc_now()
        run_id = make_run_id(job, platform, started_at)
        with self.conn:
            stale = self.conn.execute(
                "UPDATE crawl_runs SET status = 'killed' "
                "WHERE job = ? AND platform = ? AND status = 'running'",
                (job, platform),
            ).rowcount
            self.conn.execute(
                "INSERT INTO crawl_runs (run_id, job, platform, started_at, status) "
                "VALUES (?, ?, ?, ?, 'running')",
                (run_id, job, platform, to_iso(started_at)),
            )
        if stale:
            logger.warning("Marked %d stale run(s) of %s/%s as killed", stale, job, platform)
        logger.info("Run started: %s",run_id)
        return run_id

    def finish_run(
        self,
        run_id: str,
        status: str,
        events_count: int,
        error: str | None = None,
        ended_at: datetime | None = None,
    ) -> None:
        """Kết thúc run: status là ok hoặc failed (killed chỉ do start_run đánh)."""
        if status not in RUN_STATUSES:
            raise ValueError(f"invalid status: {status!r}")
        ended_at = ended_at or utc_now()
        with self.conn:
            updated = self.conn.execute(
                "UPDATE crawl_runs SET status = ?, ended_at = ?, events_count = ?, error = ? "
                "WHERE run_id = ?",
                (status, to_iso(ended_at), events_count, error, run_id),
            ).rowcount
        if updated == 0:
            raise KeyError(f"unknown run_id: {run_id}")
        logger.info("Run finished: %s status=%s events=%d", run_id, status, events_count)

    def get_run(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM crawl_runs WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None

    def list_runs(self, job: str | None = None) -> list[dict]:
        """Toàn bộ run (lọc theo job nếu có), cũ trước mới sau."""
        if job:
            rows = self.conn.execute(
                "SELECT * FROM crawl_runs WHERE job = ? ORDER BY started_at", (job,)
            )
        else:
            rows = self.conn.execute("SELECT * FROM crawl_runs ORDER BY started_at")
        return [dict(r) for r in rows]

    ''' catalog '''
    
    def upsert_products(
        self,
        platform: str,
        products: Iterable[tuple[str, str | None]],
        seen_at: datetime,
        is_hot: bool | None = None,
    ) -> int:
        """Thêm hoặc cập nhật sản phẩm vào catalog. products là các cặp (product_id, url).

        - Sản phẩm mới: first_seen = last_seen = seen_at.
        - Sản phẩm đã có: giữ first_seen, cập nhật url và last_seen.
        - is_hot = None: giữ nguyên giá trị cũ (sản phẩm mới thì 0).
        Trả về số sản phẩm đã xử lý.
        """
        seen = to_iso(seen_at)
        hot = None if is_hot is None else int(is_hot)
        rows = [
            {
                "product_key": make_product_key(platform, product_id),
                "platform": platform,
                "product_id": product_id,
                "url": url,
                "seen": seen,
                "hot": hot,
            }
            for product_id, url in products
        ]
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO catalog (product_key, platform, product_id, url, first_seen, last_seen, is_hot)
                VALUES (:product_key, :platform, :product_id, :url, :seen, :seen, COALESCE(:hot, 0))
                ON CONFLICT (product_key) DO UPDATE SET
                    url       = COALESCE(excluded.url, catalog.url),
                    last_seen = MAX(catalog.last_seen, excluded.last_seen),
                    is_hot    = COALESCE(:hot, catalog.is_hot)
                """,
                rows,
            )
        return len(rows)

    def get_product(self, platform: str, product_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM catalog WHERE product_key = ?",
            (make_product_key(platform, product_id),),
        ).fetchone()
        return dict(row) if row else None

    def products_needing_detail(self, platform: str, limit: int, stale_before: datetime) -> list[dict]:
        """Sản phẩm cần lấy trang chi tiết: chưa lấy bao giờ, hoặc lấy trước stale_before.
        Ưu tiên: chưa lấy bao giờ -> nhóm hot -> lâu chưa lấy nhất."""
        rows = self.conn.execute(
            """
            SELECT product_id, url FROM catalog
            WHERE platform = ? AND url IS NOT NULL AND (detail_at IS NULL OR detail_at < ?)
            ORDER BY detail_at IS NOT NULL, is_hot DESC, detail_at, last_seen DESC
            LIMIT ?
            """,
            (platform, to_iso(stale_before), limit),
        )
        return [dict(r) for r in rows]

    def mark_detail_done(self, platform: str, product_id: str, at: datetime) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE catalog SET detail_at = ? WHERE product_key = ?",
                (to_iso(at), make_product_key(platform, product_id)),
            )

    def count_products(self, platform: str | None = None) -> int:
        if platform:
            return self.conn.execute(
                "SELECT COUNT(*) FROM catalog WHERE platform = ?", (platform,)
            ).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM catalog").fetchone()[0]
