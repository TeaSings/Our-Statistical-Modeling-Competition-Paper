from __future__ import annotations

import argparse
import csv
import os
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

from common import (
    ROOT_DIR,
    append_jsonl,
    clean_text,
    clean_text_list,
    load_json,
    load_jsonl,
    resolve_html_path,
)

WORKER_SINGLE_SELECTORS: dict[str, list[str] | str] = {}
WORKER_MULTI_SELECTORS: dict[str, list[str] | str] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="解析详情页 HTML，抽取招聘字段")
    parser.add_argument("--manifest", nargs="+", default=None, help="一个或多个详情页抓取日志 jsonl")
    parser.add_argument("--seed-file", default="", help="可选：直接从详情种子 CSV 解析本地 HTML")
    parser.add_argument("--config", required=True, help="平台配置 json")
    parser.add_argument("--output", required=True, help="输出 jsonl")
    parser.add_argument("--overwrite", action="store_true", help="写出前清空旧输出")
    parser.add_argument("--workers", type=int, default=min(16, os.cpu_count() or 1), help="并行进程数")
    return parser.parse_args()


def as_selector_list(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return value
    return [value]


def extract_single_text(soup, selectors: Iterable[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return ""


def extract_multi_text(soup, selectors: Iterable[str]) -> list[str]:
    values: list[str] = []
    for selector in selectors:
        for node in soup.select(selector):
            values.append(node.get_text(" ", strip=True))
    return clean_text_list(values)


def extract_job_id(detail_url: str) -> str:
    match = re.search(r"/jobs/([^/]+)/detail\.html", detail_url)
    if not match:
        return ""
    return match.group(1)


def read_seed_rows(seed_file: Path) -> list[dict]:
    with seed_file.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def init_worker(single_selectors: dict, multi_selectors: dict) -> None:
    global WORKER_SINGLE_SELECTORS, WORKER_MULTI_SELECTORS
    WORKER_SINGLE_SELECTORS = single_selectors
    WORKER_MULTI_SELECTORS = multi_selectors


def parse_detail_task(task: dict) -> tuple[str, dict | None]:
    from bs4 import BeautifulSoup

    html_path = task["html_path"]
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    detail_url = task.get("url", "")
    page_title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

    if "信息已删除" in page_title:
        return "deleted", None

    output_row = {
        "platform": task.get("platform", ""),
        "job_id": extract_job_id(detail_url),
        "detail_url": detail_url,
        "source_url": task.get("source_url", ""),
        "city_seed": task.get("city", ""),
        "keyword_seed": task.get("keyword", ""),
    }

    for field, selectors in WORKER_SINGLE_SELECTORS.items():
        output_row[field] = extract_single_text(soup, as_selector_list(selectors))

    for field, selectors in WORKER_MULTI_SELECTORS.items():
        values = extract_multi_text(soup, as_selector_list(selectors))
        output_row[field] = " | ".join(values)

    if not output_row.get("job_title_raw") or not output_row.get("jd_text_raw"):
        return "missing_required", None

    return "ok", output_row


def main() -> None:
    args = parse_args()
    if not args.manifest and not args.seed_file:
        raise SystemExit("either --manifest or --seed-file is required")

    config = load_json(ROOT_DIR / args.config)
    detail_cfg = config["detail_page"]
    single_selectors = detail_cfg.get("single_selectors", {})
    multi_selectors = detail_cfg.get("multi_selectors", {})
    output_path = ROOT_DIR / args.output

    if args.overwrite and output_path.exists():
        output_path.unlink()

    seen_local_paths: set[str] = set()
    skipped_missing_html = 0
    task_rows: list[dict] = []

    input_rows: list[dict] = []
    if args.manifest:
        for manifest in args.manifest:
            manifest_rows = load_jsonl(ROOT_DIR / manifest)
            input_rows.extend(manifest_rows)

    if args.seed_file:
        seed_rows = read_seed_rows(ROOT_DIR / args.seed_file)
        input_rows.extend(seed_rows)

    for row in input_rows:
        page_type = (row.get("page_type") or "detail").strip() or "detail"
        if page_type != "detail":
            continue
        if row.get("error"):
            continue

        html_path = resolve_html_path(
            local_path=row.get("local_path", ""),
            platform=row.get("platform", ""),
            page_type=page_type,
            url=row.get("url", ""),
        )
        if html_path is None:
            skipped_missing_html += 1
            continue

        resolved_path = str(html_path)
        if resolved_path in seen_local_paths:
            continue
        seen_local_paths.add(resolved_path)

        task_rows.append(
            {
                "html_path": html_path,
                "platform": row.get("platform", ""),
                "url": row.get("url", ""),
                "source_url": row.get("source_url", ""),
                "city": row.get("city", ""),
                "keyword": row.get("keyword", ""),
            }
        )

    parsed_count = 0
    skipped_deleted = 0
    skipped_missing_required = 0
    output_rows: list[dict] = []

    if args.workers <= 1:
        init_worker(single_selectors, multi_selectors)
        results_iter = map(parse_detail_task, task_rows)
        for index, (status, output_row) in enumerate(results_iter, start=1):
            if status == "deleted":
                skipped_deleted += 1
            elif status == "missing_required":
                skipped_missing_required += 1
            elif status == "ok" and output_row is not None:
                output_rows.append(output_row)
                parsed_count += 1

                if len(output_rows) >= 500:
                    append_jsonl(output_path, output_rows)
                    output_rows = []

            if index % 2000 == 0:
                print(
                    f"progress: {index}/{len(task_rows)} "
                    f"parsed={parsed_count} deleted={skipped_deleted} "
                    f"missing_required={skipped_missing_required}",
                    flush=True,
                )
    else:
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=init_worker,
            initargs=(single_selectors, multi_selectors),
        ) as executor:
            results_iter = executor.map(parse_detail_task, task_rows, chunksize=50)

            for index, (status, output_row) in enumerate(results_iter, start=1):
                if status == "deleted":
                    skipped_deleted += 1
                elif status == "missing_required":
                    skipped_missing_required += 1
                elif status == "ok" and output_row is not None:
                    output_rows.append(output_row)
                    parsed_count += 1

                    if len(output_rows) >= 500:
                        append_jsonl(output_path, output_rows)
                        output_rows = []

                if index % 2000 == 0:
                    print(
                        f"progress: {index}/{len(task_rows)} "
                        f"parsed={parsed_count} deleted={skipped_deleted} "
                        f"missing_required={skipped_missing_required}",
                        flush=True,
                    )

    if output_rows:
        append_jsonl(output_path, output_rows)

    print(
        "parsed "
        f"{parsed_count} detail pages; "
        f"skipped_deleted={skipped_deleted}; "
        f"skipped_missing_html={skipped_missing_html}; "
        f"skipped_missing_required={skipped_missing_required}"
    )


if __name__ == "__main__":
    main()
