"""Build GitHub Release data archives from the local data stash.

The script reads large files from _local_archive_not_for_github/ and writes
zip assets under _release_assets/data-v1.0/. Zip entries keep repository
relative paths so users can restore an asset by extracting it at repo root.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_ARCHIVE_ROOT = Path("_local_archive_not_for_github")
DEFAULT_OUTPUT_ROOT = Path("_release_assets") / "data-v1.0"
DEFAULT_MAX_ASSET_MB = 1900


@dataclass(frozen=True)
class Asset:
    name: str
    sources: tuple[str, ...]
    description: str


ASSETS: tuple[Asset, ...] = (
    Asset(
        "processed-51job-main.zip",
        ("data/processed/51job",),
        "51job cleaned social and campus recruitment tables.",
    ),
    Asset(
        "raw-51job-records.zip",
        ("data/raw/51job/records",),
        "51job raw JSONL records.",
    ),
    Asset(
        "raw-51job-manifests.zip",
        ("data/raw/51job/manifests",),
        "51job crawl manifests, cursors, and progress logs.",
    ),
    Asset(
        "analysis-static-full.zip",
        ("data/processed/analysis_static",),
        "Full static extraction intermediates and master tables.",
    ),
    Asset(
        "ncss-raw-and-processed.zip",
        ("data/raw/ncss/records", "data/raw/ncss/manifests", "data/processed/ncss"),
        "NCSS raw records, manifests, and processed tables.",
    ),
    Asset(
        "analysis-local-supplement.zip",
        ("analysis/job_level_scored.csv",),
        "Large local analysis supplements excluded from Git.",
    ),
)


def iter_source_files(archive_root: Path, source: str) -> Iterable[Path]:
    path = archive_root / source
    if not path.exists():
        return
    if path.is_file():
        yield path
        return
    for file_path in sorted(path.rglob("*")):
        if file_path.is_file():
            yield file_path


def collect_files(archive_root: Path, asset: Asset) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for source in asset.sources:
        for file_path in iter_source_files(archive_root, source):
            resolved = file_path.resolve()
            if resolved not in seen:
                files.append(file_path)
                seen.add(resolved)
    return files


def format_size(num_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB")
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} GiB"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_asset(archive_root: Path, output_root: Path, asset: Asset, max_asset_mb: int) -> tuple[Path, str]:
    files = collect_files(archive_root, asset)
    if not files:
        raise FileNotFoundError(f"No files found for {asset.name}")

    output_root.mkdir(parents=True, exist_ok=True)
    archive_path = output_root / asset.name
    with zipfile.ZipFile(
        archive_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        allowZip64=True,
    ) as zf:
        for file_path in files:
            zf.write(file_path, file_path.relative_to(archive_root).as_posix())

    size_mb = archive_path.stat().st_size / (1024 * 1024)
    if size_mb > max_asset_mb:
        print(
            f"WARNING: {archive_path} is {size_mb:.2f} MiB, above the {max_asset_mb} MiB target.",
            file=sys.stderr,
        )
    return archive_path, sha256_file(archive_path)


def dry_run(archive_root: Path) -> int:
    missing = 0
    for asset in ASSETS:
        files = collect_files(archive_root, asset)
        total = sum(path.stat().st_size for path in files)
        if not files:
            missing += 1
            print(f"{asset.name}: MISSING sources={', '.join(asset.sources)}")
            continue
        print(f"{asset.name}: {len(files)} files, {format_size(total)} uncompressed")
    return 1 if missing else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-asset-mb", type=int, default=DEFAULT_MAX_ASSET_MB)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    archive_root = args.archive_root
    if not archive_root.exists():
        print(f"Archive root not found: {archive_root}", file=sys.stderr)
        return 1

    if args.dry_run:
        return dry_run(archive_root)

    checksum_lines: list[str] = []
    for asset in ASSETS:
        archive_path, digest = write_asset(archive_root, args.output_root, asset, args.max_asset_mb)
        checksum_lines.append(f"{digest}  {archive_path.name}")
        print(f"Wrote {archive_path} ({format_size(archive_path.stat().st_size)})")

    checksum_path = args.output_root / "checksums-sha256.txt"
    checksum_path.write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")
    print(f"Wrote {checksum_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
