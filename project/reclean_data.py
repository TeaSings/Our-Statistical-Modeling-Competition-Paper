from __future__ import annotations

import argparse
from pathlib import Path

from clean_data import (
    CLEAN_V2_DIR,
    RAW_DIR,
    CleanRunResult,
    build_reclean_output_path,
    run_cleaning,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Re-clean historical raw job CSV files and backfill publish_time / publish_date "
            "without overwriting the original clean outputs."
        )
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--input",
        help="Path to one raw CSV file, for example project/data/raw/jobs_raw_xxx.csv",
    )
    source_group.add_argument(
        "--all-raw",
        action="store_true",
        help="Re-clean every raw CSV under project/data/raw/.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(CLEAN_V2_DIR),
        help="Directory for the corrected clean outputs. Default: project/data/clean_v2/",
    )
    return parser.parse_args()


def resolve_raw_files(*, input_path: str | None = None, all_raw: bool = False) -> list[Path]:
    if input_path:
        raw_path = Path(input_path).expanduser().resolve()
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_path}")
        return [raw_path]

    if not all_raw:
        raise ValueError("Either --input or --all-raw must be provided.")

    candidates = sorted(RAW_DIR.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No raw CSV files found in {RAW_DIR}")
    return candidates


def reclean_one_file(raw_path: Path, *, output_dir: str | Path) -> CleanRunResult:
    output_path = build_reclean_output_path(raw_path, output_dir=output_dir)
    return run_cleaning(
        input_path=raw_path,
        fallback_keyword="",
        output_path=output_path,
    )


def main() -> int:
    args = parse_args()
    raw_files = resolve_raw_files(input_path=args.input, all_raw=args.all_raw)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Re-clean output directory: {output_dir}")
    print(f"Raw files to process: {len(raw_files)}")

    results: list[CleanRunResult] = []
    failures: list[tuple[Path, str]] = []

    for index, raw_path in enumerate(raw_files, start=1):
        print(f"[{index}/{len(raw_files)}] Re-cleaning raw file: {raw_path}")
        try:
            result = reclean_one_file(raw_path, output_dir=output_dir)
        except Exception as exc:
            failures.append((raw_path, str(exc)))
            print(f"  failed: {exc}")
            continue

        results.append(result)
        print(f"  output clean file: {result.output_path}")
        print(f"  rows: {result.input_rows} -> {result.output_rows}")
        print("  restored fields: crawl_time, crawl_date, publish_time, publish_date")

    print("Re-clean summary:")
    print(f"  succeeded: {len(results)}")
    print(f"  failed: {len(failures)}")
    print(f"  output_dir: {output_dir}")

    if failures:
        print("Failed files:")
        for raw_path, reason in failures:
            print(f"  - {raw_path}: {reason}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
