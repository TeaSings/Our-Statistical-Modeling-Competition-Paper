from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, portable_path


LEGACY_SOCIAL_FILES = [
    Path("data/raw/51job/records/51job_social_jobs_raw.jsonl"),
    Path("data/processed/51job/51job_social_jobs_clean.csv"),
    Path("data/raw/51job/manifests/51job_social_cursor.json"),
    Path("data/raw/51job/manifests/51job_social_progress.json"),
    Path("data/raw/51job/manifests/51job_social_partition_manifest.jsonl"),
    Path("data/raw/51job/manifests/51job_social_scheduler.stdout.log"),
    Path("data/raw/51job/manifests/51job_social_scheduler.stderr.log"),
]
RUNTIME_PROFILE_DIRS = [
    Path("data/runtime/51job/browser_profile_auto"),
    Path("data/runtime/51job/manual_edge_profile"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清理 51job 社招旧快照和浏览器运行态垃圾数据")
    parser.add_argument("--dry-run", action="store_true", help="只打印要删除的内容，不真正删除")
    parser.add_argument(
        "--skip-runtime-profiles",
        action="store_true",
        help="只清理旧 social 数据文件，不删除浏览器 profile 缓存目录",
    )
    return parser.parse_args()


def ensure_within_workspace(path: Path) -> Path:
    resolved = path.resolve()
    root = ROOT_DIR.resolve()
    resolved.relative_to(root)
    return resolved


def remove_path(path: Path, *, dry_run: bool) -> tuple[int, bool]:
    resolved = ensure_within_workspace(path)
    if not resolved.exists():
        return 0, False
    if resolved.is_file():
        size = resolved.stat().st_size
        if not dry_run:
            resolved.unlink()
        return size, True

    total = 0
    for child in resolved.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                pass
    if not dry_run:
        shutil.rmtree(resolved)
    return total, True


def main() -> None:
    args = parse_args()
    targets = [ROOT_DIR / path for path in LEGACY_SOCIAL_FILES]
    if not args.skip_runtime_profiles:
        targets.extend(ROOT_DIR / path for path in RUNTIME_PROFILE_DIRS)

    removed_count = 0
    removed_bytes = 0
    for target in targets:
        try:
            size, existed = remove_path(target, dry_run=args.dry_run)
        except Exception as exc:
            print(f"failed {portable_path(target)}: {exc}")
            continue
        if not existed:
            print(f"skip {portable_path(target)} (missing)")
            continue
        removed_count += 1
        removed_bytes += size
        action = "would remove" if args.dry_run else "removed"
        print(f"{action} {portable_path(target)} ({size} bytes)")

    mode = "dry-run" if args.dry_run else "done"
    print(f"{mode}: targets={removed_count} bytes={removed_bytes}")


if __name__ == "__main__":
    main()
