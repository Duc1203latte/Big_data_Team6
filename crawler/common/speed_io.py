import json
import logging
import socket
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("speed_io")

DEFAULT_RULES_PATH = Path("/app/config/anomaly_rules.yaml")
LOCAL_RULES_PATH = Path(__file__).resolve().parent.parent / "config" / "anomaly_rules.yaml"

DEFAULT_CONFIG = {
    "watermark_minutes": 10,
    "trigger_seconds": 10,
    "rules": {
        "historical_deviation": {
            "enabled": True,
            "min_dev_ratio": 0.30,
            "min_robust_z": 3.5,
            "mad_floor_ratio": 0.02,
            "min_abs_diff_vnd": 100000,
        },
        "cross_platform_cheaper": {
            "enabled": True,
            "min_cheaper_ratio": 0.25,
            "min_abs_diff_vnd": 150000,
        },
        "fake_promotion": {
            "enabled": True,
            "min_discount_pct": 40.0,
            "max_real_drop_ratio": 0.05,
            "inflated_original_ratio": 1.50,
        },
    },
}


def _to_int(val):
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _to_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def load_anomaly_config(path=None):
    """Đọc cấu hình ngưỡng bất thường, nếu lỗi hoặc thiếu file thì dùng mặc định."""
    paths = [Path(path)] if path else [DEFAULT_RULES_PATH, LOCAL_RULES_PATH]
    for p in paths:
        if p and p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception as exc:
                log.warning("Failed to parse config %s, using defaults: %s", p, exc)
    return DEFAULT_CONFIG


def evaluate_anomaly_rules(
    price_current,
    price_original,
    discount_pct,
    baseline_median,
    baseline_mad,
    other_platform_min_price,
    config,
):
    """Kiểm tra 3 luật bất thường giá. Trả về (is_anomaly, score, reasons)."""
    price_current = _to_int(price_current)
    price_original = _to_int(price_original)
    discount_pct = _to_float(discount_pct)
    baseline_median = _to_float(baseline_median)
    baseline_mad = _to_float(baseline_mad)
    other_platform_min_price = _to_int(other_platform_min_price)

    if not price_current or price_current <= 0:
        return 0, 0.0, []

    rules_cfg = config.get("rules", {})
    reasons = []
    scores = []

    # Luật 1: Lệch mạnh so với giá lịch sử (dùng median + MAD)
    hist_cfg = rules_cfg.get("historical_deviation", {})
    if hist_cfg.get("enabled", True) and baseline_median and baseline_median > 0:
        abs_diff = abs(price_current - baseline_median)
        dev_ratio = abs_diff / baseline_median
        mad_val = baseline_mad if (baseline_mad and baseline_mad > 0) else 0.0

        floor_scale = hist_cfg.get("mad_floor_ratio", 0.02) * baseline_median
        scale = max(1.4826 * mad_val, floor_scale, 10000.0)
        robust_z = abs_diff / scale

        if (
            abs_diff >= hist_cfg.get("min_abs_diff_vnd", 100000)
            and dev_ratio >= hist_cfg.get("min_dev_ratio", 0.30)
            and robust_z >= hist_cfg.get("min_robust_z", 3.5)
        ):
            reasons.append("HISTORICAL_DEVIATION")
            scores.append(min(100.0, round(dev_ratio * 100.0, 2)))

    # Luật 2: Rẻ hơn bất thường so với sàn khác
    cross_cfg = rules_cfg.get("cross_platform_cheaper", {})
    if (
        cross_cfg.get("enabled", True)
        and other_platform_min_price
        and other_platform_min_price > 0
        and price_current < other_platform_min_price
    ):
        cheaper_diff = other_platform_min_price - price_current
        cheaper_ratio = cheaper_diff / other_platform_min_price
        if (
            cheaper_diff >= cross_cfg.get("min_abs_diff_vnd", 150000)
            and cheaper_ratio >= cross_cfg.get("min_cheaper_ratio", 0.25)
        ):
            reasons.append("CROSS_PLATFORM_CHEAPER")
            scores.append(min(100.0, round(cheaper_ratio * 100.0, 2)))

    # Luật 3: Khuyến mãi ảo (nâng giá gốc hoặc báo % giảm cao nhưng giá bán vẫn ngang median)
    fake_cfg = rules_cfg.get("fake_promotion", {})
    if fake_cfg.get("enabled", True) and baseline_median and baseline_median > 0:
        min_disc = fake_cfg.get("min_discount_pct", 40.0)
        max_real_drop = fake_cfg.get("max_real_drop_ratio", 0.05)
        inflated_ratio = fake_cfg.get("inflated_original_ratio", 1.50)

        high_claimed = discount_pct is not None and discount_pct >= min_disc
        no_real_drop = price_current >= baseline_median * (1.0 - max_real_drop)
        inflated_orig = (
            price_original is not None
            and price_original >= baseline_median * inflated_ratio
            and price_current >= baseline_median * 0.90
        )

        if (high_claimed and no_real_drop) or inflated_orig:
            reasons.append("FAKE_PROMOTION")
            fake_score = float(discount_pct if discount_pct is not None else 50.0)
            if price_original and baseline_median > 0:
                inflated_pct = (price_original / baseline_median - 1.0) * 100.0
                fake_score = max(fake_score, inflated_pct)
            scores.append(min(100.0, round(fake_score, 2)))

    if not reasons:
        return 0, 0.0, []
    return 1, max(scores), reasons


def fmt_dt(dt):
    """Đưa datetime hoặc chuỗi thời gian về chuẩn UTC cho ClickHouse DateTime64(3)."""
    if dt is None:
        return None
    if isinstance(dt, str):
        s = dt.strip()
        if s.endswith(("Z", "z")):
            s = s[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(s)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc)
            return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            return s.replace("T", " ").replace("Z", "")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class ClickHouseHTTP:
    """Client gọi HTTP interface của ClickHouse qua urllib."""

    def __init__(self, host="clickhouse", port=8123, database="bigdata"):
        self.base_url = f"http://{host}:{port}/"
        self.database = database

    def execute(self, query, data=None, timeout=15.0):
        params = urllib.parse.urlencode({"database": self.database, "query": query})
        req = urllib.request.Request(f"{self.base_url}?{params}", data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            err_msg = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse error ({exc.code}): {err_msg}")

    def insert_json_rows(self, table, rows):
        if not rows:
            return 0
        lines = [json.dumps(r, ensure_ascii=False) for r in rows]
        payload = "\n".join(lines).encode("utf-8")
        query = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow"
        self.execute(query, data=payload)
        return len(rows)

    def query_json_rows(self, sql):
        out = self.execute(f"{sql} FORMAT JSONEachRow")
        return [json.loads(line) for line in out.splitlines() if line.strip()]


class SimpleRedisClient:
    """Client Redis tối giản dùng socket thuần để chạy trên Spark container."""

    def __init__(self, host="redis", port=6379, timeout=5.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    @staticmethod
    def _encode_cmd(*args):
        parts = [f"*{len(args)}\r\n".encode("utf-8")]
        for arg in args:
            b = arg.encode("utf-8") if isinstance(arg, str) else arg
            parts.append(f"${len(b)}\r\n".encode("utf-8"))
            parts.append(b + b"\r\n")
        return b"".join(parts)

    def _read_resp(self, sock_file):
        line = sock_file.readline()
        if not line:
            raise ConnectionError("Redis connection closed unexpectedly")
        line = line[:-2]
        prefix, payload = line[:1], line[1:]

        if prefix == b"+":
            return payload.decode("utf-8", errors="replace")
        if prefix == b"-":
            raise RuntimeError(f"Redis error: {payload.decode('utf-8', errors='replace')}")
        if prefix == b":":
            return int(payload)
        if prefix == b"$":
            length = int(payload)
            if length == -1:
                return None
            data = sock_file.read(length)
            sock_file.read(2)
            return data.decode("utf-8", errors="replace")
        if prefix == b"*":
            count = int(payload)
            if count == -1:
                return None
            return [self._read_resp(sock_file) for _ in range(count)]
        raise RuntimeError(f"Unknown Redis response prefix: {prefix}")

    def pipeline(self, commands):
        if not commands:
            return []
        raw_payload = b"".join(self._encode_cmd(*cmd) for cmd in commands)
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            sock.sendall(raw_payload)
            with sock.makefile("rb") as sock_file:
                return [self._read_resp(sock_file) for _ in commands]

    def hgetall_many(self, keys):
        if not keys:
            return {}
        cmds = [("HGETALL", k) for k in keys]
        results = self.pipeline(cmds)
        out = {}
        for k, flat in zip(keys, results):
            if isinstance(flat, list) and flat:
                out[k] = {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}
            else:
                out[k] = {}
        return out