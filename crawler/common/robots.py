"""Đọc và áp dụng robots.txt theo chuẩn RFC 9309.

Không dùng urllib.robotparser vì thư viện này so khớp theo tiền tố, không hiểu dấu * ở
giữa quy tắc (ví dụ /*muc-gia= của FPT Shop), nên có thể cho phép nhầm URL bị cấm.

Quy tắc so khớp:
- * là chuỗi ký tự bất kỳ, $ ở cuối quy tắc là hết URL.
- Quy tắc khớp dài nhất thắng; dài bằng nhau thì Allow thắng.
- Không có quy tắc nào khớp thì được phép.
- Chọn nhóm User-agent trùng tên crawler, không có thì dùng nhóm *.

Module này không đi ra mạng, HttpClient tải file rồi đưa nội dung vào đây.
"""

import re
from dataclasses import dataclass
from urllib.parse import quote, unquote

# Ký tự giữ nguyên khi chuẩn hoá. Có * và $ để không làm hỏng ký hiệu của quy tắc
_SAFE_CHARS = "/?=&*$:@!,;+~-._()'"


def _normalize(text: str) -> str:
    """Đưa về cùng một dạng percent-encoding để "đ" và "%C4%91" so khớp được với nhau."""
    return quote(unquote(text), safe=_SAFE_CHARS)


def _to_regex(pattern: str) -> re.Pattern[str]:
    """Đổi quy tắc robots thành regex: * là chuỗi bất kỳ, $ ở cuối là hết URL."""
    anchored = pattern.endswith("$")
    body = pattern[:-1] if anchored else pattern
    regex = re.escape(body).replace(r"\*", ".*")
    return re.compile(regex + ("$" if anchored else ""))


@dataclass(frozen=True)
class _Rule:
    is_allow: bool
    length: int             # độ dài quy tắc, dùng để chọn quy tắc khớp dài nhất
    regex: re.Pattern[str]


def _parse_groups(text: str) -> list[tuple[list[str], list[tuple[bool, str]]]]:
    """Tách robots.txt thành các nhóm ([user-agent...], [(is_allow, pattern)...])."""
    groups: list[tuple[list[str], list[tuple[bool, str]]]] = []
    in_agent_lines = False      # đang ở chuỗi dòng User-agent liền nhau của cùng một nhóm
    for raw_line in text.lstrip("﻿").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        field, value = (part.strip() for part in line.split(":", 1))
        field = field.lower()
        if field == "user-agent":
            if not in_agent_lines:
                groups.append(([], []))
                in_agent_lines = True
            groups[-1][0].append(value.lower())
        elif field in ("allow", "disallow"):
            in_agent_lines = False
            # Quy tắc rỗng ("Disallow:") nghĩa là không cấm gì, không cần lưu
            if groups and value:
                groups[-1][1].append((field == "allow", value))
    return groups


class RobotsRules:
    """Các quy tắc áp dụng cho một crawler, đọc từ nội dung một file robots.txt."""

    def __init__(self, text: str, user_agent: str) -> None:
        # Tên so khớp là phần đầu của User-Agent: "bigdata-crawler/0.1.0 (...)" -> "bigdata-crawler"
        token = user_agent.split("/", 1)[0].split(" ", 1)[0].strip().lower()
        groups = _parse_groups(text)
        patterns = [rule for agents, rules in groups if token in agents for rule in rules]
        if not any(token in agents for agents, _ in groups):
            patterns = [rule for agents, rules in groups if "*" in agents for rule in rules]
        self._rules = [
            _Rule(is_allow, len(normalized), _to_regex(normalized))
            for is_allow, normalized in ((allow, _normalize(p)) for allow, p in patterns)
        ]

    @classmethod
    def allow_all(cls) -> "RobotsRules":
        """Không có quy tắc nào, dùng khi sàn không có robots.txt."""
        return cls("", "*")

    def is_allowed(self, path_and_query: str) -> bool:
        """Đường dẫn kèm query (ví dụ /dien-thoai?page=2) có được phép crawl không."""
        target = _normalize(path_and_query)
        best: tuple[int, bool] | None = None    # (độ dài quy tắc, có phải Allow)
        for rule in self._rules:
            if rule.regex.match(target):
                candidate = (rule.length, rule.is_allow)
                if best is None or candidate > best:
                    best = candidate
        return best is None or best[1]
