"""Đo giới hạn truy cập của một sàn (README bước 5). CHỈ CHẠY KHI ĐƯỢC CHO PHÉP.

Bắn request với tốc độ tăng dần (mặc định 0,2 -> 0,5 -> 1 -> 2 request/giây,
mỗi mức 2 phút), ghi lại lúc bắt đầu gặp 429/403 và độ trễ, rồi ghi thêm
một mục vào docs/api_limits.md.

Dừng sớm để không làm phiền sàn:
- Một mức gặp đủ 5 lần 429/403 thì dừng mức đó ngay.
- Mức nào đã gặp 429/403 thì không lên mức cao hơn.

Chạy (trong thư mục crawler/):
    python -m jobs.measure_limits --platform tiki
"""

import argparse
import asyncio
import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx

from common.config import load_settings
from common.logging_setup import setup_logging

logger = logging.getLogger("measure_limits")

# URL dùng để đo: chính loại request crawler sẽ gọi nhiều nhất (trang danh sách)
PROBE_URLS = {
    "tiki": [
        f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category=1789&page={page}"
        for page in (1, 2, 3)
    ],
}
DEFAULT_RATES = [0.2, 0.5, 1.0, 2.0]
BLOCK_STATUSES = {403, 429}
MAX_BLOCKS_PER_LEVEL = 5


@dataclass
class LevelResult:
    """Kết quả đo ở một mức tốc độ."""

    rate: float
    statuses: list[int] = field(default_factory=list)       # 0 = lỗi mạng hoặc timeout
    latencies_ms: list[float] = field(default_factory=list)
    first_block_s: float | None = None                      # giây thứ mấy gặp 429/403 đầu tiên
    aborted: bool = False                                   # dừng sớm vì bị chặn quá nhiều

    def count(self, *statuses: int) -> int:
        return sum(1 for s in self.statuses if s in statuses)

    @property
    def blocked(self) -> int:
        return self.count(*BLOCK_STATUSES)

    @property
    def ok(self) -> int:
        return sum(1 for s in self.statuses if 200 <= s < 300)

    @property
    def other_errors(self) -> int:
        return len(self.statuses) - self.ok - self.blocked

    @property
    def avg_ms(self) -> float | None:
        return statistics.fmean(self.latencies_ms) if self.latencies_ms else None

    @property
    def p95_ms(self) -> float | None:
        if len(self.latencies_ms) < 2:
            return self.latencies_ms[0] if self.latencies_ms else None
        return statistics.quantiles(self.latencies_ms, n=20)[18]


async def run_level(client, urls: list[str], rate: float, duration_s: float) -> LevelResult:
    """Bắn request đều đặn ở tốc độ `rate` trong `duration_s` giây.

    Lịch gửi cố định (request thứ i gửi ở giây i / rate) và mỗi request chạy
    thành task riêng, nên response chậm không làm giảm tốc độ đo.
    """
    result = LevelResult(rate=rate)
    interval = 1.0 / rate
    total = max(1, round(duration_s * rate))
    t0 = time.monotonic()

    async def one_request(i: int) -> None:
        url = urls[i % len(urls)]
        start = time.monotonic()
        try:
            resp = await client.get(url)
            status = resp.status_code
        except httpx.HTTPError as exc:
            logger.warning("Request failed: %s", type(exc).__name__)
            status = 0
        result.statuses.append(status)
        result.latencies_ms.append((time.monotonic() - start) * 1000)
        if status in BLOCK_STATUSES and result.first_block_s is None:
            result.first_block_s = start - t0
            logger.warning("First HTTP %s at %.0fs into %.1f req/s level", status, start - t0, rate)

    tasks = []
    for i in range(total):
        if result.blocked >= MAX_BLOCKS_PER_LEVEL:
            result.aborted = True
            logger.warning("Too many 429/403 at %.1f req/s, aborting this level", rate)
            break
        # Ngủ tới đúng lịch gửi của request thứ i
        await asyncio.sleep(max(0.0, t0 + i * interval - time.monotonic()))
        tasks.append(asyncio.create_task(one_request(i)))
    await asyncio.gather(*tasks)
    return result


def fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}"


def render_report(platform: str, urls: list[str], user_agent: str,
                  started_at: datetime, duration_s: float, results: list[LevelResult]) -> str:
    """Một mục markdown cho docs/api_limits.md."""
    lines = [
        f"## {platform} — {started_at:%Y-%m-%d %H:%M} UTC",
        "",
        f"- URL đo: {len(urls)} trang danh sách, ví dụ `{urls[0]}`",
        f"- User-Agent: `{user_agent.split(' (')[0]} (academic research project; contact: ...)`",
        f"- Mỗi mức {duration_s:.0f} giây; dừng mức khi gặp {MAX_BLOCKS_PER_LEVEL} lần 429/403",
        "",
        "| Mức (req/s) | Request | 2xx | 429 | 403 | Lỗi khác | Độ trễ TB (ms) | p95 (ms) | 429/403 đầu tiên |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        first = "-" if r.first_block_s is None else f"giây {r.first_block_s:.0f}"
        if r.aborted:
            first += " (dừng sớm)"
        lines.append(
            f"| {r.rate:g} | {len(r.statuses)} | {r.ok} | {r.count(429)} | {r.count(403)} "
            f"| {r.other_errors} | {fmt_ms(r.avg_ms)} | {fmt_ms(r.p95_ms)} | {first} |"
        )

    safe = [r.rate for r in results if r.blocked == 0 and r.other_errors == 0]
    blocked = [r.rate for r in results if r.blocked > 0]
    lines.append("")
    if blocked:
        lines.append(f"**Kết luận:** bắt đầu bị chặn ở {blocked[0]:g} req/s; "
                     f"mức an toàn cao nhất đo được: {max(safe):g} req/s." if safe else
                     f"**Kết luận:** bị chặn ngay từ {blocked[0]:g} req/s.")
    elif safe:
        lines.append(f"**Kết luận:** không gặp 429/403 tới {max(safe):g} req/s.")
    else:
        lines.append("**Kết luận:** có lỗi mạng ở mọi mức, cần đo lại.")
    lines.append("")
    return "\n".join(lines)


def append_report(path: Path, section: str) -> None:
    """Ghi thêm vào cuối file (giữ lịch sử các lần đo), tạo file kèm tiêu đề nếu chưa có."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("# Giới hạn truy cập đo thực tế\n\nSinh bởi `jobs/measure_limits.py`.\n\n",
                        encoding="utf-8")
    with path.open("a", encoding="utf-8") as f:
        f.write(section + "\n")


async def measure(platform: str, rates: list[float], duration_s: float,
                  pause_s: float, output: Path) -> list[LevelResult]:
    settings = load_settings()
    setup_logging(level=settings.log_level)
    urls = PROBE_URLS[platform]
    started_at = datetime.now(timezone.utc)
    results: list[LevelResult] = []

    headers = {"User-Agent": settings.user_agent, "Accept": "application/json"}
    async with httpx.AsyncClient(headers=headers, timeout=15) as client:
        for idx, rate in enumerate(rates):
            logger.info("Measuring %s at %.1f req/s for %.0fs", platform, rate, duration_s)
            result = await run_level(client, urls, rate, duration_s)
            results.append(result)
            logger.info("%.1f req/s: %d requests, %d ok, %d blocked, avg %s ms",
                        rate, len(result.statuses), result.ok, result.blocked, fmt_ms(result.avg_ms))
            if result.blocked:
                logger.warning("Blocked at %.1f req/s, not going higher", rate)
                break
            if idx < len(rates) - 1:
                await asyncio.sleep(pause_s)    # nghỉ giữa các mức

    append_report(output, render_report(platform, urls, settings.user_agent,
                                        started_at, duration_s, results))
    logger.info("Report appended to %s", output)
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure access limits of a platform")
    parser.add_argument("--platform", required=True, choices=sorted(PROBE_URLS))
    parser.add_argument("--rates", default=",".join(f"{r:g}" for r in DEFAULT_RATES),
                        help="comma separated req/s levels, e.g. 0.2,0.5,1,2")
    parser.add_argument("--duration", type=float, default=120, help="seconds per level")
    parser.add_argument("--pause", type=float, default=10, help="seconds to rest between levels")
    parser.add_argument("--output", type=Path, default=Path("docs/api_limits.md"))
    args = parser.parse_args(argv)
    args.rates = [float(x) for x in args.rates.split(",")]
    return args


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(measure(args.platform, args.rates, args.duration, args.pause, args.output))
