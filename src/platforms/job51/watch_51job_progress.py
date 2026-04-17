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

from common import ROOT_DIR, configure_utf8_stdio  # noqa: E402


DEFAULT_CAMPUS_SEED = "data/input/51job/campus_seed_urls.csv"
DEFAULT_CAMPUS_MANIFEST = "data/raw/51job/manifests/51job_campus_seed_manifest.jsonl"
DEFAULT_CAMPUS_RAW = "data/raw/51job/records/51job_campus_jobs_raw.jsonl"
DEFAULT_CAMPUS_CLEAN = "data/processed/51job/51job_campus_jobs_clean.csv"
DEFAULT_SOCIAL_PROGRESS = "data/raw/51job/manifests/51job_social_progress.json"
DEFAULT_SOCIAL_MANIFEST = "data/raw/51job/manifests/51job_social_partition_manifest.jsonl"
DEFAULT_SOCIAL_RAW = "data/raw/51job/records/51job_social_jobs_raw.jsonl"
DEFAULT_SOCIAL_CLEAN = "data/processed/51job/51job_social_jobs_clean.csv"
DEFAULT_SOCIAL_CURSOR = "data/raw/51job/manifests/51job_social_cursor.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch 51job crawl progress")
    parser.add_argument(
        "--mode",
        choices=["auto", "campus", "social"],
        default="auto",
        help="Choose campus topic crawl, social search crawl, or auto-detect based on the social progress file",
    )
    parser.add_argument("--seed-file", default="", help="Campus seed CSV; defaults by mode")
    parser.add_argument("--manifest", default="", help="Manifest JSONL; defaults by mode")
    parser.add_argument("--raw-file", default="", help="RAW JSONL; defaults by mode")
    parser.add_argument("--clean-file", default="", help="Clean CSV; defaults by mode")
    parser.add_argument("--progress-file", default="", help="Social progress snapshot JSON; defaults by mode")
    parser.add_argument("--cursor-file", default="", help="Sequential scheduler cursor JSON; defaults by mode")
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


def load_progress_snapshot(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_cursor_snapshot(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def format_bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "[" + "-" * width + "]"
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(width * ratio)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_seconds(seconds: int | None) -> str:
    if seconds is None:
        return "--"
    if seconds <= 0:
        return "0s"
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{sec:02d}s"
    return f"{sec}s"


def format_timestamp(value: float | int | None) -> str:
    if not value:
        return "--"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(value)))


def render_campus(
    seed_total: int,
    manifest_total: int,
    raw_total: int,
    clean_total: int,
    tail: list[dict],
) -> str:
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


def social_primary_progress(snapshot: dict) -> tuple[int, int]:
    function_total = int(snapshot.get("function_total") or 0)
    function_done = int(snapshot.get("function_done") or 0)
    partition_total = int(snapshot.get("partition_total") or 0)
    partition_done = int(snapshot.get("partition_done") or 0)
    page_total = int(snapshot.get("page_total") or 0)
    page_done = int(snapshot.get("page_done") or 0)
    stage = str(snapshot.get("stage") or "")

    if stage == "completed":
        if page_total > 0:
            return page_total, page_total
        if partition_total > 0:
            return partition_total, partition_total
        return function_total, function_total
    if page_total > 0:
        return page_done, page_total
    if partition_total > 0:
        return partition_done, partition_total
    return function_done, function_total


def render_social(snapshot: dict, manifest_total: int, raw_total: int, clean_total: int, cursor: dict | None = None) -> str:
    done, total = social_primary_progress(snapshot)
    started_at = snapshot.get("started_at")
    updated_at = snapshot.get("updated_at")
    elapsed = None
    eta = None
    stale_hint = ""
    status_note = str(snapshot.get("status_note") or "").strip()
    if started_at:
        elapsed_seconds = max(int(time.time() - float(started_at)), 0)
        elapsed = format_seconds(elapsed_seconds)
        if done > 0 and total > done:
            rate = done / max(elapsed_seconds, 1)
            eta = format_seconds(int((total - done) / rate)) if rate > 0 else "--"
    if updated_at:
        stale_seconds = max(int(time.time() - float(updated_at)), 0)
        if stale_seconds >= 60 and str(snapshot.get("stage") or "") != "completed":
            stale_hint = (
                f"进度快照已 {format_seconds(stale_seconds)} 未更新，"
                "可能在等待人工验证、浏览器重试或网络阻塞"
            )

    lines = [
        "51job 社招顺序抓取进度",
        "",
        f"主进度    {format_bar(done, total)} {done}/{total}",
        f"阶段      {snapshot.get('stage') or '--'}",
        f"Functions {snapshot.get('function_done', 0)}/{snapshot.get('function_total', 0)}",
        (
            f"Partitions {snapshot.get('partition_done', 0)}/{snapshot.get('partition_total', 0)}"
            f" | final {snapshot.get('final_partition_total', 0)}"
            f" | capped {snapshot.get('capped_partition_total', 0)}"
            f" | manifest {manifest_total}"
        ),
        f"Pages     {snapshot.get('page_done', 0)}/{snapshot.get('page_total', 0)}",
        f"本轮写入   {snapshot.get('records_written', 0)} 条",
        f"RAW 总量   {raw_total} 条",
        f"Clean 总量 {clean_total} 条",
        f"空 JD 丢弃 {snapshot.get('empty_jd_dropped', 0)} 条",
        f"当前分片   {snapshot.get('current_label') or '--'}",
        f"开始时间   {format_timestamp(started_at)}",
        f"最近更新   {format_timestamp(updated_at)}",
        f"已运行     {elapsed or '--'}",
        f"估计剩余   {eta or '--'}",
    ]
    if status_note:
        lines.append(f"状态说明   {status_note}")
    if stale_hint:
        lines.append(f"状态提示   {stale_hint}")
    if cursor:
        lines.extend(
            [
                "",
                "顺序调度器",
                f"批次队列   {cursor.get('done_batches', 0)}/{cursor.get('total_batches', 0)}",
                f"累计新增   {cursor.get('records_written_total', 0)} 条",
                f"累计失败页 {cursor.get('page_failures_total', 0)}",
                f"调度状态   {cursor.get('status') or '--'}",
            ]
        )
        current_batch = cursor.get("current_batch") or {}
        if current_batch:
            lines.append(
                "当前批次   "
                f"{current_batch.get('batch_number', '--')}/{current_batch.get('total_batches', '--')} | "
                f"{current_batch.get('function_label') or '--'} | "
                f"{current_batch.get('job_area_label') or '--'}"
            )
            if int(current_batch.get("page_total") or 0) > 0:
                lines.append(
                    "批内页进度 "
                    f"{current_batch.get('page_done', 0)}/{current_batch.get('page_total', 0)}"
                )
            next_page = current_batch.get("resume_next_page") or {}
            if next_page:
                lines.append(f"断点续跑   {next_page.get('label') or '--'}")
        last_batch = cursor.get("last_completed_batch") or {}
        if last_batch:
            lines.append(
                "最近完成   "
                f"{last_batch.get('function_label') or '--'} | "
                f"{last_batch.get('job_area_label') or '--'} | "
                f"+{last_batch.get('records_written', 0)} 条"
            )
    return "\n".join(lines)


def resolve_mode(args: argparse.Namespace, progress_file: Path) -> str:
    if args.mode != "auto":
        return args.mode
    return "social" if progress_file.exists() else "campus"


def resolve_path(value: str, default_value: str) -> Path:
    return ROOT_DIR / (value or default_value)


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    social_progress_file = resolve_path(args.progress_file, DEFAULT_SOCIAL_PROGRESS)
    social_cursor_file = resolve_path(args.cursor_file, DEFAULT_SOCIAL_CURSOR)
    mode = resolve_mode(args, social_progress_file)

    if mode == "social":
        seed_file = None
        manifest_file = resolve_path(args.manifest, DEFAULT_SOCIAL_MANIFEST)
        raw_file = resolve_path(args.raw_file, DEFAULT_SOCIAL_RAW)
        clean_file = resolve_path(args.clean_file, DEFAULT_SOCIAL_CLEAN)
        progress_file = social_progress_file
        cursor_file = social_cursor_file
    else:
        seed_file = resolve_path(args.seed_file, DEFAULT_CAMPUS_SEED)
        manifest_file = resolve_path(args.manifest, DEFAULT_CAMPUS_MANIFEST)
        raw_file = resolve_path(args.raw_file, DEFAULT_CAMPUS_RAW)
        clean_file = resolve_path(args.clean_file, DEFAULT_CAMPUS_CLEAN)
        progress_file = social_progress_file
        cursor_file = social_cursor_file

    while True:
        manifest_total = count_jsonl(manifest_file)
        raw_total = count_jsonl(raw_file)
        clean_total = count_csv_rows(clean_file)
        if mode == "social":
            snapshot = load_progress_snapshot(progress_file)
            cursor = load_cursor_snapshot(cursor_file)
            output = render_social(snapshot, manifest_total, raw_total, clean_total, cursor)
        else:
            seed_total = count_seed_rows(seed_file) if seed_file else 0
            tail = load_manifest_tail(manifest_file)
            output = render_campus(seed_total, manifest_total, raw_total, clean_total, tail)
        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(output, flush=True)
        if args.once:
            break
        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
