from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from common import ROOT_DIR


@dataclass
class ShardSnapshot:
    seed_file: Path
    manifest_file: Path
    total_rows: int
    done_rows: int
    delta_rows: int
    updated_seconds_ago: int | None

    @property
    def ratio(self) -> float:
        if self.total_rows <= 0:
            return 0.0
        return min(self.done_rows / self.total_rows, 1.0)

    @property
    def display_name(self) -> str:
        name = self.seed_file.stem
        if "shard_" in name:
            return name.split("shard_")[-1]
        return name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="实时查看 NCSS 详情抓取进度")
    parser.add_argument(
        "--seed-dir",
        default="data/input/ncss/shards",
        help="详情分片种子目录",
    )
    parser.add_argument(
        "--manifest-dir",
        default="data/raw/ncss/manifests/shards",
        help="详情分片 manifest 目录",
    )
    parser.add_argument(
        "--all-seed-file",
        default="data/input/ncss/ncss_detail_urls_all_areas.csv",
        help="全量详情种子文件",
    )
    parser.add_argument(
        "--detail-html-dir",
        default="data/raw/ncss/html/ncss_jobs/detail",
        help="详情页 HTML 目录",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="刷新间隔秒数",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只输出一次快照，不持续刷新",
    )
    return parser.parse_args()


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


def count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def count_detail_html_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for entry in os.scandir(path) if entry.is_file() and entry.name.endswith(".html"))


def build_manifest_path(seed_file: Path, manifest_dir: Path) -> Path:
    manifest_name = seed_file.name.replace("ncss_detail_urls_", "ncss_detail_manifest_").replace(".csv", ".jsonl")
    return manifest_dir / manifest_name


def format_bar(ratio: float, width: int = 28) -> str:
    ratio = max(0.0, min(ratio, 1.0))
    filled = int(width * ratio)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_age(seconds_ago: int | None) -> str:
    if seconds_ago is None:
        return "未开始"
    if seconds_ago < 2:
        return "刚更新"
    if seconds_ago < 60:
        return f"{seconds_ago}s前"
    minutes, seconds = divmod(seconds_ago, 60)
    return f"{minutes}m{seconds:02d}s前"


def format_rate(delta_rows: int, interval: float) -> str:
    if interval <= 0:
        return ""
    rate = delta_rows / interval
    return f"{rate:.1f}条/秒"


def collect_snapshots(
    seed_files: list[Path],
    manifest_dir: Path,
    seed_totals: dict[Path, int],
    previous_counts: dict[Path, int | None],
) -> list[ShardSnapshot]:
    now = time.time()
    snapshots: list[ShardSnapshot] = []
    for seed_file in seed_files:
        manifest_file = build_manifest_path(seed_file, manifest_dir)
        done_rows = count_lines(manifest_file)
        previous_value = previous_counts.get(seed_file)
        delta_rows = 0 if previous_value is None else done_rows - previous_value
        previous_counts[seed_file] = done_rows

        updated_seconds_ago: int | None = None
        if manifest_file.exists():
            updated_seconds_ago = max(0, int(now - manifest_file.stat().st_mtime))

        snapshots.append(
            ShardSnapshot(
                seed_file=seed_file,
                manifest_file=manifest_file,
                total_rows=seed_totals[seed_file],
                done_rows=done_rows,
                delta_rows=delta_rows,
                updated_seconds_ago=updated_seconds_ago,
            )
        )
    return snapshots


def render_output(
    snapshots: list[ShardSnapshot],
    all_seed_total: int,
    html_total: int,
    interval: float,
) -> str:
    total_shard_rows = sum(snapshot.total_rows for snapshot in snapshots)
    total_done_rows = sum(snapshot.done_rows for snapshot in snapshots)
    total_delta_rows = sum(max(snapshot.delta_rows, 0) for snapshot in snapshots)
    total_ratio = 0.0 if total_shard_rows <= 0 else min(total_done_rows / total_shard_rows, 1.0)
    html_ratio = 0.0 if all_seed_total <= 0 else min(html_total / all_seed_total, 1.0)

    lines = [
        "NCSS 详情抓取进度监控",
        f"分片总进度  {format_bar(total_ratio)} {total_done_rows}/{total_shard_rows}  {total_ratio * 100:5.1f}%  {format_rate(total_delta_rows, interval)}",
        f"HTML 覆盖   {format_bar(html_ratio)} {html_total}/{all_seed_total}  {html_ratio * 100:5.1f}%",
        "",
    ]

    for snapshot in snapshots:
        lines.append(
            f"分片 {snapshot.display_name}  {format_bar(snapshot.ratio, width=20)} "
            f"{snapshot.done_rows}/{snapshot.total_rows}  {snapshot.ratio * 100:5.1f}%  "
            f"+{max(snapshot.delta_rows, 0):<3}  最近更新 {format_age(snapshot.updated_seconds_ago)}"
        )

    if total_done_rows >= total_shard_rows > 0:
        lines.extend(["", "全部分片 manifest 已追平种子文件，当前这轮抓取已经结束。"])
    else:
        lines.extend(["", "按 Ctrl+C 退出监控；全部完成后脚本会自动结束。"])

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    seed_dir = ROOT_DIR / args.seed_dir
    manifest_dir = ROOT_DIR / args.manifest_dir
    all_seed_file = ROOT_DIR / args.all_seed_file
    detail_html_dir = ROOT_DIR / args.detail_html_dir

    seed_files = sorted(seed_dir.glob("*.csv"))
    if not seed_files:
        raise SystemExit(f"no shard seed files found in {seed_dir}")

    seed_totals = {seed_file: count_csv_rows(seed_file) for seed_file in seed_files}
    all_seed_total = count_csv_rows(all_seed_file)
    previous_counts = {seed_file: None for seed_file in seed_files}

    while True:
        snapshots = collect_snapshots(seed_files, manifest_dir, seed_totals, previous_counts)
        html_total = count_detail_html_files(detail_html_dir)
        output = render_output(snapshots, all_seed_total, html_total, args.interval)

        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(output, flush=True)

        total_shard_rows = sum(snapshot.total_rows for snapshot in snapshots)
        total_done_rows = sum(snapshot.done_rows for snapshot in snapshots)
        if args.once or (total_shard_rows > 0 and total_done_rows >= total_shard_rows):
            break

        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
