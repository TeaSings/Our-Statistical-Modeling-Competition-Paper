from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = next((parent for parent in CURRENT_FILE.parents if parent.name == "src"), None)
if SRC_DIR is None:
    raise RuntimeError("could not locate src directory from rebuild_local_manifest.py")
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, append_jsonl, portable_path, sha1_text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="根据详情种子和本地 HTML 重建 manifest")
    parser.add_argument("--seed-file", required=True, help="详情种子 CSV")
    parser.add_argument("--output", required=True, help="输出 manifest jsonl")
    parser.add_argument("--output-dir", default="data/raw/ncss/html", help="HTML 根目录")
    parser.add_argument("--url-column", default="url", help="链接列名")
    parser.add_argument("--platform-column", default="platform", help="平台列名")
    parser.add_argument("--page-type-column", default="page_type", help="页面类型列名")
    parser.add_argument("--overwrite", action="store_true", help="写出前清空旧 manifest")
    return parser.parse_args()


def read_seed_rows(seed_file: Path) -> list[dict]:
    with seed_file.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    args = parse_args()
    seed_file = ROOT_DIR / args.seed_file
    output_path = ROOT_DIR / args.output
    output_dir = ROOT_DIR / args.output_dir

    if args.overwrite and output_path.exists():
        output_path.unlink()

    rows = read_seed_rows(seed_file)
    manifest_rows: list[dict] = []
    now_text = datetime.now().isoformat(timespec="seconds")

    for row in rows:
        url = (row.get(args.url_column) or "").strip()
        if not url:
            continue

        platform = (row.get(args.platform_column) or "unknown").strip() or "unknown"
        page_type = (row.get(args.page_type_column) or "detail").strip() or "detail"
        filename = f"{sha1_text(url)}.html"
        html_path = output_dir / platform / page_type / filename
        html_exists = html_path.exists()

        manifest_row = {
            "fetched_at": now_text,
            "platform": platform,
            "page_type": page_type,
            "url": url,
            "local_path": portable_path(html_path),
            "local_file_exists": html_exists,
            "reused_existing": html_exists,
            "status_code": 200 if html_exists else None,
            "fetched": html_exists,
            "error": "" if html_exists else "local html missing",
            "rebuilt_local_manifest": True,
        }

        for key, value in row.items():
            if key not in manifest_row:
                manifest_row[key] = value

        manifest_rows.append(manifest_row)

        if len(manifest_rows) >= 1000:
            append_jsonl(output_path, manifest_rows)
            manifest_rows = []

    if manifest_rows:
        append_jsonl(output_path, manifest_rows)

    print(f"rebuilt {len(rows)} manifest rows to {output_path}")


if __name__ == "__main__":
    main()
