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
class MetricSnapshot:
    label: str
    current: int
    total: int
    delta_rows: int
    updated_seconds_ago: int | None

    @property
    def ratio(self) -> float:
        if self.total <= 0:
            return 0.0
        return min(self.current / self.total, 1.0)


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
        "--all-manifest-file",
        default="data/raw/ncss/manifests/ncss_detail_manifest_all_areas.jsonl",
        help="全量详情 manifest 文件",
    )
    parser.add_argument(
        "--raw-record-file",
        default="data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl",
        help="解析后的原始 jsonl",
    )
    parser.add_argument(
        "--clean-file",
        default="data/processed/ncss/ncss_jobs_all_areas_clean.csv",
        help="清洗后的 csv",
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
    if interval <= 0 or delta_rows <= 0:
        return "--"
    rate = delta_rows / interval
    return f"{rate:.1f}条/秒"


def format_eta(seconds: int | None) -> str:
    if seconds is None:
        return "--"
    if seconds <= 0:
        return "完成"
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def estimate_eta(current: int, total: int, delta_rows: int, interval: float) -> int | None:
    if total <= 0 or current >= total:
        return 0
    if interval <= 0 or delta_rows <= 0:
        return None
    rate = delta_rows / interval
    if rate <= 0:
        return None
    remaining = max(total - current, 0)
    return int(remaining / rate)


def get_updated_seconds_ago(path: Path, now: float) -> int | None:
    if not path.exists():
        return None
    return max(0, int(now - path.stat().st_mtime))


def build_metric_snapshot(
    label: str,
    key: str,
    path: Path,
    current: int,
    total: int,
    previous_counts: dict[str, int | None],
) -> MetricSnapshot:
    now = time.time()
    previous_value = previous_counts.get(key)
    delta_rows = 0 if previous_value is None else current - previous_value
    previous_counts[key] = current
    return MetricSnapshot(
        label=label,
        current=current,
        total=total,
        delta_rows=delta_rows,
        updated_seconds_ago=get_updated_seconds_ago(path, now),
    )


def render_metric(snapshot: MetricSnapshot, interval: float) -> str:
    eta_seconds = estimate_eta(snapshot.current, snapshot.total, max(snapshot.delta_rows, 0), interval)
    display_current = min(snapshot.current, snapshot.total) if snapshot.total > 0 else snapshot.current
    extra_note = ""
    if snapshot.total > 0 and snapshot.current > snapshot.total:
        extra_note = f" (+{snapshot.current - snapshot.total} 额外)"
    return (
        f"{snapshot.label:<10}"
        f"{format_bar(snapshot.ratio)} "
        f"{display_current}/{snapshot.total}{extra_note}  "
        f"{snapshot.ratio * 100:6.2f}%  "
        f"{format_rate(max(snapshot.delta_rows, 0), interval):>8}  "
        f"ETA {format_eta(eta_seconds):>7}  "
        f"最近更新 {format_age(snapshot.updated_seconds_ago)}"
    )


def render_output_metric(label: str, current: int, delta_rows: int, updated_seconds_ago: int | None, interval: float) -> str:
    rate_text = format_rate(max(delta_rows, 0), interval)
    return (
        f"{label:<10}"
        f"{current} 条  "
        f"{rate_text:>8}  "
        f"最近更新 {format_age(updated_seconds_ago)}"
    )


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
    manifest_metric: MetricSnapshot,
    html_metric: MetricSnapshot,
    raw_metric: MetricSnapshot,
    clean_count: int,
    clean_delta: int,
    clean_updated_seconds_ago: int | None,
    interval: float,
) -> str:
    total_shard_rows = sum(snapshot.total_rows for snapshot in snapshots)
    total_done_rows = sum(snapshot.done_rows for snapshot in snapshots)
    total_delta_rows = sum(max(snapshot.delta_rows, 0) for snapshot in snapshots)
    total_ratio = 0.0 if total_shard_rows <= 0 else min(total_done_rows / total_shard_rows, 1.0)

    lines = [
        "NCSS 全量进度监控",
        "",
        f"当前活跃阶段: {detect_active_stage(manifest_metric, html_metric, raw_metric, clean_delta)}",
        "",
        "主流程",
        render_metric(manifest_metric, interval),
        render_metric(html_metric, interval),
        render_metric(raw_metric, interval),
        render_output_metric("Clean 输出", clean_count, clean_delta, clean_updated_seconds_ago, interval),
        "",
        "分片抓取",
        f"分片总进度  {format_bar(total_ratio)} {total_done_rows}/{total_shard_rows}  {total_ratio * 100:5.1f}%  {format_rate(total_delta_rows, interval)}",
    ]

    for snapshot in snapshots:
        eta_seconds = estimate_eta(snapshot.done_rows, snapshot.total_rows, max(snapshot.delta_rows, 0), interval)
        lines.append(
            f"分片 {snapshot.display_name}  {format_bar(snapshot.ratio, width=20)} "
            f"{snapshot.done_rows}/{snapshot.total_rows}  {snapshot.ratio * 100:6.2f}%  "
            f"+{max(snapshot.delta_rows, 0):<3}  "
            f"ETA {format_eta(eta_seconds):>7}  "
            f"最近更新 {format_age(snapshot.updated_seconds_ago)}"
        )

    missing_raw = max(raw_metric.total - raw_metric.current, 0)

    if raw_metric.current >= raw_metric.total > 0:
        lines.extend(["", "全国详情页解析已经追平种子总量；接下来只需要重跑或刷新清洗结果。"])
    elif html_metric.current >= html_metric.total > 0 and missing_raw <= 20:
        lines.extend(
            [
                "",
                f"全国 HTML 已齐；当前 RAW 仍少 {missing_raw} 条，通常是已删除页面或异常详情页，并不一定代表任务卡住。",
            ]
        )
    elif html_metric.current >= html_metric.total > 0:
        lines.extend(["", "全国 HTML 已齐，当前主要等待 RAW 解析推进。"])
    elif total_done_rows >= total_shard_rows > 0:
        lines.extend(["", "全部分片 manifest 已追平种子文件，当前这轮抓取已经结束。"])
    else:
        lines.extend(["", "按 Ctrl+C 退出监控；主流程追平后脚本会自动结束。"])

    return "\n".join(lines)


def detect_active_stage(
    manifest_metric: MetricSnapshot,
    html_metric: MetricSnapshot,
    raw_metric: MetricSnapshot,
    clean_delta: int,
) -> str:
    if max(raw_metric.delta_rows, 0) > 0:
        return "RAW 解析"
    if max(html_metric.delta_rows, 0) > 0 or max(manifest_metric.delta_rows, 0) > 0:
        return "抓取 HTML"
    if max(clean_delta, 0) > 0:
        return "Clean 输出"
    if raw_metric.total > 0 and raw_metric.current >= raw_metric.total:
        return "已完成"
    return "等待中"


def main() -> None:
    args = parse_args()
    seed_dir = ROOT_DIR / args.seed_dir
    manifest_dir = ROOT_DIR / args.manifest_dir
    all_seed_file = ROOT_DIR / args.all_seed_file
    detail_html_dir = ROOT_DIR / args.detail_html_dir
    all_manifest_file = ROOT_DIR / args.all_manifest_file
    raw_record_file = ROOT_DIR / args.raw_record_file
    clean_file = ROOT_DIR / args.clean_file

    seed_files = sorted(seed_dir.glob("*.csv"))
    if not seed_files:
        raise SystemExit(f"no shard seed files found in {seed_dir}")

    seed_totals = {seed_file: count_csv_rows(seed_file) for seed_file in seed_files}
    all_seed_total = count_csv_rows(all_seed_file)
    previous_counts = {seed_file: None for seed_file in seed_files}
    previous_metric_counts: dict[str, int | None] = {
        "manifest": None,
        "html": None,
        "raw": None,
        "clean": None,
    }

    while True:
        snapshots = collect_snapshots(seed_files, manifest_dir, seed_totals, previous_counts)
        manifest_total = count_lines(all_manifest_file)
        html_total = count_detail_html_files(detail_html_dir)
        raw_total = count_lines(raw_record_file)
        clean_total = count_csv_rows(clean_file)

        manifest_metric = build_metric_snapshot(
            label="Manifest",
            key="manifest",
            path=all_manifest_file,
            current=manifest_total,
            total=all_seed_total,
            previous_counts=previous_metric_counts,
        )
        html_metric = build_metric_snapshot(
            label="HTML 覆盖",
            key="html",
            path=detail_html_dir,
            current=html_total,
            total=all_seed_total,
            previous_counts=previous_metric_counts,
        )
        raw_metric = build_metric_snapshot(
            label="RAW 解析",
            key="raw",
            path=raw_record_file,
            current=raw_total,
            total=all_seed_total,
            previous_counts=previous_metric_counts,
        )
        clean_metric = build_metric_snapshot(
            label="Clean 输出",
            key="clean",
            path=clean_file,
            current=clean_total,
            total=max(raw_total, 1),
            previous_counts=previous_metric_counts,
        )

        output = render_output(
            snapshots=snapshots,
            manifest_metric=manifest_metric,
            html_metric=html_metric,
            raw_metric=raw_metric,
            clean_count=clean_metric.current,
            clean_delta=clean_metric.delta_rows,
            clean_updated_seconds_ago=clean_metric.updated_seconds_ago,
            interval=args.interval,
        )

        if sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print(output, flush=True)

        if args.once or (raw_metric.total > 0 and raw_metric.current >= raw_metric.total):
            break

        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
