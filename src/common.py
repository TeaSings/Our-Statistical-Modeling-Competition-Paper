from __future__ import annotations

import hashlib
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Iterable, List

ROOT_DIR = Path(__file__).resolve().parent.parent
HTML_BASE_DIR_CANDIDATES = (
    Path("data/raw/html"),
    Path("data/raw/ncss/html"),
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def configure_utf8_stdio() -> None:
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except ValueError:
            # Some embedded or already-closed streams do not allow reconfigure.
            continue


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def portable_path(path: str | Path) -> str:
    path = Path(path)
    try:
        return str(path.relative_to(ROOT_DIR))
    except ValueError:
        return str(path)


def resolve_html_path(
    local_path: str | Path,
    platform: str,
    page_type: str,
    url: str,
) -> Path | None:
    candidates: list[Path] = []
    seen: set[str] = set()

    if local_path:
        path = Path(local_path)
        candidates.append(path)
        if not path.is_absolute():
            candidates.append(ROOT_DIR / path)
        if path.name:
            for base_dir in HTML_BASE_DIR_CANDIDATES:
                candidates.append(ROOT_DIR / base_dir / platform / page_type / path.name)

    if url and platform and page_type:
        filename = f"{sha1_text(url)}.html"
        for base_dir in HTML_BASE_DIR_CANDIDATES:
            candidates.append(ROOT_DIR / base_dir / platform / page_type / filename)

    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate

    return None


def load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: str | Path) -> List[dict]:
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def append_jsonl(path: str | Path, rows: Iterable[dict]) -> None:
    path = Path(path)
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_jsonl(path: str | Path, rows: Iterable[dict]) -> None:
    path = Path(path)
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def clean_text_list(values: Iterable[str]) -> List[str]:
    cleaned = [clean_text(v) for v in values]
    return [v for v in cleaned if v]


def sleep_with_jitter(base_seconds: float) -> None:
    base_seconds = max(base_seconds, 0)
    jitter = min(max(base_seconds, 0.05), 0.5)
    delay = base_seconds + random.uniform(0, jitter)
    time.sleep(delay)
