from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch 51job campus crawl progress")
    parser.add_argument("--seed-file", default="data/input/51job/campus_seed_urls.csv")
    parser.add_argument("--manifest", default="data/raw/51job/manifests/51job_campus_seed_manifest.jsonl")
    parser.add_argument("--raw-file", default="data/raw/51job/records/51job_campus_jobs_raw.jsonl")
    parser.add_argument("--clean-file", default="data/processed/51job/51job_campus_jobs_clean.csv")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def count_seed_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return sum(1 for row in reader if row.get("seed_url"))


def count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def load_manifest_tail(path: Path, limit: int = 3) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows[-limit:]


def format_bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "[" + "-" * width + "]"
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(width * ratio)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def render(seed_total: int, manifest_total: int, raw_total: int, clean_total: int, tail: list[dict]) -> str:
    lines = [
        "51job 校招专题页进度",
        "",
        f"Seed 进度  {format_bar(manifest_total, seed_total)} {manifest_total}/{seed_total}",
        f"RAW 记录   {raw_total} 条",
        f"Clean 输出 {clean_total} 条",
    ]
    if tail:
        lines.append("")
        lines.append("最近处理")
        for row in tail:
            lines.append(
                f"- {row.get('name') or row.get('seed_url')} | "
                f"{row.get('strategy')} | {row.get('records_emitted')} 条"
            )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    seed_file = ROOT_DIR / args.seed_file
    manifest_file = ROOT_DIR / args.manifest
    raw_file = ROOT_DIR / args.raw_file
    clean_file = ROOT_DIR / args.clean_file

    while True:
        seed_total = count_seed_rows(seed_file)
        manifest_total = count_jsonl(manifest_file)
        raw_total = count_jsonl(raw_file)
        clean_total = count_csv_rows(clean_file)
        tail = load_manifest_tail(manifest_file)
        output = render(seed_total, manifest_total, raw_total, clean_total, tail)
        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(output, flush=True)
        if args.once:
            break
        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
