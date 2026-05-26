from __future__ import annotations

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = next((parent for parent in CURRENT_FILE.parents if parent.name == "src"), None)
if SRC_DIR is None:
    raise RuntimeError("could not locate src directory from fetch_pages.py")
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import (
    DEFAULT_HEADERS,
    ROOT_DIR,
    append_jsonl,
    ensure_parent,
    load_json,
    portable_path,
    sha1_text,
    sleep_with_jitter,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取列表页或详情页 HTML")
    parser.add_argument("--seed-file", required=True, help="CSV 文件，至少包含 url 列")
    parser.add_argument("--manifest", required=True, help="抓取日志 jsonl")
    parser.add_argument("--output-dir", default="data/raw/html", help="HTML 输出目录")
    parser.add_argument("--url-column", default="url", help="链接列名")
    parser.add_argument("--platform-column", default="platform", help="平台列名")
    parser.add_argument("--page-type-column", default="page_type", help="页面类型列名")
    parser.add_argument("--delay", type=float, default=2.0, help="请求间隔秒数")
    parser.add_argument("--timeout", type=float, default=20.0, help="超时时间")
    parser.add_argument("--retries", type=int, default=3, help="失败时最多重试次数")
    parser.add_argument("--skip-existing", action="store_true", help="已存在则跳过")
    parser.add_argument("--overwrite-manifest", action="store_true", help="写出前清空旧 manifest")
    parser.add_argument("--workers", type=int, default=1, help="并发线程数")
    parser.add_argument("--progress-every", type=int, default=500, help="每处理多少条打印一次进度")
    parser.add_argument("--limit", type=int, default=0, help="只抓前 N 条，0 表示不限制")
    parser.add_argument("--config", default="", help="可选的平台配置 json")
    return parser.parse_args()


def read_seed_rows(seed_file: Path) -> list[dict]:
    with open(seed_file, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def fetch_one(task: dict) -> dict:
    row = task["row"]
    output_dir: Path = task["output_dir"]
    headers: dict = task["headers"]
    url_column = task["url_column"]
    platform_column = task["platform_column"]
    page_type_column = task["page_type_column"]
    skip_existing = task["skip_existing"]
    delay = task["delay"]
    timeout = task["timeout"]
    retries = task["retries"]

    url = (row.get(url_column) or "").strip()
    platform = (row.get(platform_column) or "unknown").strip() or "unknown"
    page_type = (row.get(page_type_column) or "detail").strip() or "detail"
    filename = f"{sha1_text(url)}.html"
    html_path = output_dir / platform / page_type / filename
    ensure_parent(html_path)

    status_code = None
    error = ""
    fetched = False
    reused_existing = False

    if skip_existing and html_path.exists():
        reused_existing = True
    else:
        if delay > 0:
            sleep_with_jitter(delay)
        for attempt in range(1, max(retries, 1) + 1):
            try:
                request = Request(url, headers=headers)
                with urlopen(request, timeout=timeout) as response:
                    status_code = getattr(response, "status", None) or response.getcode()
                    body = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    html_path.write_text(body.decode(encoding, errors="ignore"), encoding="utf-8")
                fetched = True
                error = ""
                break
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
                if attempt >= max(retries, 1):
                    break
                sleep_with_jitter(min(delay + 0.5 * attempt, 2.0))

    manifest_row = {
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "platform": platform,
        "page_type": page_type,
        "url": url,
        "local_path": portable_path(html_path),
        "local_file_exists": html_path.exists(),
        "reused_existing": reused_existing,
        "status_code": status_code,
        "fetched": fetched,
        "error": error,
    }

    for key, value in row.items():
        if key not in manifest_row:
            manifest_row[key] = value

    return manifest_row


def main() -> None:
    args = parse_args()
    seed_file = ROOT_DIR / args.seed_file
    manifest_path = ROOT_DIR / args.manifest
    output_dir = ROOT_DIR / args.output_dir

    rows = read_seed_rows(seed_file)
    if args.limit > 0:
        rows = rows[: args.limit]

    if args.overwrite_manifest and manifest_path.exists():
        manifest_path.unlink()

    headers = dict(DEFAULT_HEADERS)
    if args.config:
        config = load_json(ROOT_DIR / args.config)
        headers.update(config.get("request_headers", {}))

    tasks = []
    for row in rows:
        url = (row.get(args.url_column) or "").strip()
        if not url:
            continue
        tasks.append(
            {
                "row": row,
                "output_dir": output_dir,
                "headers": headers,
                "url_column": args.url_column,
                "platform_column": args.platform_column,
                "page_type_column": args.page_type_column,
                "skip_existing": args.skip_existing,
                "delay": args.delay,
                "timeout": args.timeout,
                "retries": args.retries,
            }
        )

    processed = 0
    fetched_count = 0
    reused_count = 0
    error_count = 0
    manifest_buffer: list[dict] = []
    workers = max(args.workers, 1)

    if workers <= 1:
        results_iter = map(fetch_one, tasks)
        for manifest_row in results_iter:
            processed += 1
            if manifest_row.get("fetched"):
                fetched_count += 1
            if manifest_row.get("reused_existing"):
                reused_count += 1
            if manifest_row.get("error"):
                error_count += 1

            manifest_buffer.append(manifest_row)
            if len(manifest_buffer) >= 200:
                append_jsonl(manifest_path, manifest_buffer)
                manifest_buffer = []

            if args.progress_every > 0 and processed % args.progress_every == 0:
                print(
                    f"progress: {processed}/{len(tasks)} "
                    f"fetched={fetched_count} reused={reused_count} errors={error_count}",
                    flush=True,
                )
    else:
        max_workers = min(workers, len(tasks) or 1, (os.cpu_count() or workers) * 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results_iter = executor.map(fetch_one, tasks)

            for manifest_row in results_iter:
                processed += 1
                if manifest_row.get("fetched"):
                    fetched_count += 1
                if manifest_row.get("reused_existing"):
                    reused_count += 1
                if manifest_row.get("error"):
                    error_count += 1

                manifest_buffer.append(manifest_row)
                if len(manifest_buffer) >= 200:
                    append_jsonl(manifest_path, manifest_buffer)
                    manifest_buffer = []

                if args.progress_every > 0 and processed % args.progress_every == 0:
                    print(
                        f"progress: {processed}/{len(tasks)} "
                        f"fetched={fetched_count} reused={reused_count} errors={error_count}",
                        flush=True,
                    )

    if manifest_buffer:
        append_jsonl(manifest_path, manifest_buffer)

    print(
        f"saved {processed} rows to {manifest_path}; "
        f"fetched={fetched_count}; reused_existing={reused_count}; errors={error_count}"
    )


if __name__ == "__main__":
    main()
