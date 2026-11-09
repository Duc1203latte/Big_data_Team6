"""Test bước 5: đọc cấu hình .env và logic đo giới hạn (HTTP giả, không gọi mạng)."""

from datetime import datetime, timezone

import httpx
import pytest
from pathlib import Path

from common.config import load_env_file, load_settings
from jobs.measure_limits import LevelResult, append_report, parse_args, render_report, run_level


# ------------------------------ config ------------------------------

def write_env(tmp_path, text):
    path = tmp_path / ".env"
    path.write_text(text, encoding="utf-8")
    return path


ENV_KEYS = ["CRAWLER_VERSION", "LANDING_DIR", "STATE_DB", "LOG_LEVEL", "CONTACT_EMAIL", "SINK",
            "KAFKA_BOOTSTRAP_SERVERS"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    # Mỗi test bắt đầu với môi trường sạch, không dính .env thật của máy
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_load_settings_from_env_file(tmp_path):
    env = write_env(tmp_path, "# comment\nCRAWLER_VERSION=0.2.0\nCONTACT_EMAIL=team@example.com\n")
    s = load_settings(env)
    assert s.crawler_version == "0.2.0"
    assert s.contact_email == "team@example.com"
    assert s.user_agent == "bigdata-crawler/0.2.0 (academic research project; contact: team@example.com)"


def test_existing_env_var_not_overridden(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    env = write_env(tmp_path, "LOG_LEVEL=INFO\nCONTACT_EMAIL=a@b.c\n")
    assert load_settings(env).log_level == "DEBUG"


def test_placeholder_email_rejected(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=<email để ghi vào User-Agent>\n")
    with pytest.raises(RuntimeError):
        load_settings(env)


def test_sink_defaults_to_file(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=a@b.c\n")
    assert load_settings(env).sink == "file"


def test_sink_read_from_env_file(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=a@b.c\nSINK= File \n")
    assert load_settings(env).sink == "file"


@pytest.mark.parametrize("value", ["s3", "file,s3", "file,file", ",", ""])
def test_unknown_sink_rejected_at_startup(tmp_path, value):
    env = write_env(tmp_path, f"CONTACT_EMAIL=a@b.c\nSINK={value}\n")
    with pytest.raises(RuntimeError, match="SINK"):
        load_settings(env)


def test_multi_sink_and_kafka_address(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=a@b.c\nSINK= File , KAFKA \nKAFKA_BOOTSTRAP_SERVERS=broker:9092\n"
                              "STATE_DB=./st/crawler.db\n")
    settings = load_settings(env)
    assert settings.sink == "file,kafka"
    config = settings.sink_config
    assert config.kinds == ("file", "kafka")
    assert config.kafka_bootstrap_servers == "broker:9092"
    assert config.kafka_failed_dir == Path("st/kafka_failed")


def test_kafka_address_defaults_to_localhost(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=a@b.c\nSINK=kafka\n")
    assert load_settings(env).sink_config.kafka_bootstrap_servers == "localhost:9092"


def test_kafka_sink_needs_address(tmp_path):
    env = write_env(tmp_path, "CONTACT_EMAIL=a@b.c\nSINK=file,kafka\nKAFKA_BOOTSTRAP_SERVERS=\n")
    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP_SERVERS"):
        load_settings(env)


def test_missing_env_file_is_ok(tmp_path, monkeypatch):
    load_env_file(tmp_path / "does-not-exist")     # không lỗi
    monkeypatch.setenv("CONTACT_EMAIL", "a@b.c")
    assert load_settings(tmp_path / "does-not-exist").contact_email == "a@b.c"


# --------------------------- measure_limits ---------------------------

class FakeClient:
    """Trả về các status code theo thứ tự cho trước. -1 = trang chặn (200 + HTML)."""

    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = 0

    async def get(self, url):
        status = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        if status == 0:
            raise httpx.ConnectTimeout("timeout")
        if status == -1:
            return httpx.Response(200, html="<html>inspecting your network environment</html>")
        return httpx.Response(status, json={})


@pytest.mark.asyncio
async def test_run_level_sends_expected_number_of_requests():
    client = FakeClient([200])
    result = await run_level(client, ["u1", "u2"], rate=100, duration_s=0.1)   # 10 request trong 0,1 giây
    assert client.calls == 10
    assert result.ok == 10
    assert result.blocked == 0
    assert result.avg_ms is not None


@pytest.mark.asyncio
async def test_run_level_aborts_after_too_many_blocks():
    client = FakeClient([200, 200, 429])
    result = await run_level(client, ["u"], rate=200, duration_s=0.5)          # dự kiến 100 request
    assert result.aborted
    assert result.blocked >= 5
    assert client.calls < 100
    assert result.first_block_s is not None


@pytest.mark.asyncio
async def test_run_level_counts_network_errors():
    result = await run_level(FakeClient([0]), ["u"], rate=100, duration_s=0.05)
    assert result.other_errors == len(result.statuses) > 0


def test_render_report_conclusion():
    ok = LevelResult(rate=0.5, statuses=[200] * 60, latencies_ms=[200.0] * 60)
    blocked = LevelResult(rate=1.0, statuses=[200, 429, 429], latencies_ms=[100.0] * 3, first_block_s=42)
    text = render_report("tiki", ["https://x"], "bigdata-crawler/0.1.0 (academic research project; contact: a@b.c)",
                         datetime(2026, 11, 1, 14, 0, tzinfo=timezone.utc), 120, [ok, blocked])
    assert "## tiki — 2026-11-01 14:00 UTC" in text
    assert "| 0.5 | 60 | 60 | 0 | 0 | 0 | 0 | 200 | 200 | - |" in text
    assert "giây 42" in text
    assert "bắt đầu bị chặn ở 1 req/s" in text
    assert "a@b.c" not in text                     # không lộ email trong báo cáo


def test_append_report_keeps_history(tmp_path):
    out = tmp_path / "docs" / "api_limits.md"
    append_report(out, "## run 1\n")
    append_report(out, "## run 2\n")
    text = out.read_text(encoding="utf-8")
    assert text.startswith("# Giới hạn truy cập đo thực tế")
    assert text.index("## run 1") < text.index("## run 2")


def test_parse_args_defaults():
    args = parse_args(["--platform", "tiki"])
    assert args.rates == [0.2, 0.5, 1.0, 2.0]
    assert args.duration == 120


@pytest.mark.asyncio
async def test_run_level_counts_challenge_page_as_block():
    # Kiểu Tiki: 200 nhưng là trang HTML chặn bot -> phải tính là bị chặn, không phải thành công
    client = FakeClient([200, -1])
    result = await run_level(client, ["u"], rate=200, duration_s=0.5, expect_json=True)
    assert result.ok == 1
    assert result.blocked >= 5
    assert result.aborted


@pytest.mark.asyncio
async def test_run_level_html_ok_when_not_expecting_json():
    result = await run_level(FakeClient([-1]), ["u"], rate=100, duration_s=0.05, expect_json=False)
    assert result.blocked == 0
    assert result.ok == len(result.statuses)
