from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = next((parent for parent in CURRENT_FILE.parents if parent.name == "src"), None)
if SRC_DIR is None:
    raise RuntimeError("could not locate src directory from fetch_ncss_jobs.py")
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import (
    DEFAULT_HEADERS,
    ROOT_DIR,
    append_jsonl,
    ensure_parent,
    load_json,
    load_jsonl,
    sleep_with_jitter,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量抓取 NCSS 职位列表并导出详情页链接")
    parser.add_argument(
        "--config",
        default="data/input/ncss/ncss_batch_config.json",
        help="批量配置文件",
    )
    parser.add_argument(
        "--list-output",
        default="data/raw/ncss/records/ncss_listings_raw.jsonl",
        help="职位列表原始输出 jsonl",
    )
    parser.add_argument(
        "--detail-seed-output",
        default="data/input/ncss/ncss_detail_urls_generated.csv",
        help="详情页种子输出 csv",
    )
    parser.add_argument(
        "--summary-output",
        default="data/processed/ncss/ncss_list_query_summary.csv",
        help="查询摘要输出 csv",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=600,
        help="最多导出多少个唯一职位详情链接，0 表示不限制",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="从已有原始输出和进度日志继续抓取",
    )
    return parser.parse_args()


def fetch_json(url: str, headers: dict[str, str], max_retries: int = 5) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8", errors="ignore")
            return json.loads(body)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_retries:
                raise
            sleep_with_jitter(min(0.5 * attempt, 2.0))
    raise RuntimeError(str(last_error))


def build_query_url(base_url: str, params: dict[str, str]) -> str:
    return base_url + "?" + urlencode(params)


def main() -> None:
    args = parse_args()
    config = load_json(ROOT_DIR / args.config)
    headers = dict(DEFAULT_HEADERS)
    headers["Referer"] = config.get("referer", "https://www.ncss.cn/student/jobs/index.html")

    list_output_path = ROOT_DIR / args.list_output
    detail_seed_output_path = ROOT_DIR / args.detail_seed_output
    summary_output_path = ROOT_DIR / args.summary_output
    progress_log_path = summary_output_path.with_suffix(".jsonl")

    if "cities_path" in config:
        cities = load_json(ROOT_DIR / config["cities_path"])
    else:
        cities = config["cities"]
    keywords = config["keywords"]
    base_params = config["request_params"]
    base_url = config["list_api"]
    limit = int(config.get("limit", 20))
    max_pages_per_query = int(config.get("max_pages_per_query", 3))
    delay_seconds = float(config.get("delay_seconds", 0.2))

    unique_jobs: dict[str, dict] = {}
    completed_queries: set[tuple[str, str, int]] = set()
    raw_row_count = 0

    if args.resume:
        if list_output_path.exists():
            existing_rows = load_jsonl(list_output_path)
            raw_row_count = len(existing_rows)
            for row in existing_rows:
                job_id = row.get("jobId")
                if job_id and job_id not in unique_jobs:
                    unique_jobs[job_id] = row
        if progress_log_path.exists():
            for row in load_jsonl(progress_log_path):
                completed_queries.add(
                    (
                        str(row.get("areaCode", "")),
                        str(row.get("keyword", "")),
                        int(row.get("page", 0) or 0),
                    )
                )
    else:
        for path in [list_output_path, detail_seed_output_path, summary_output_path, progress_log_path]:
            if path.exists():
                path.unlink()

    for city in cities:
        for keyword in keywords:
            city_name = city["name"]
            area_code = city["areaCode"]
            area_level = city.get("level", "")
            for page in range(1, max_pages_per_query + 1):
                query_key = (str(area_code), str(keyword), int(page))
                if query_key in completed_queries:
                    continue

                params = dict(base_params)
                params.update(
                    {
                        "areaCode": area_code,
                        "jobName": keyword,
                        "offset": str(page),
                        "limit": str(limit),
                    }
                )
                url = build_query_url(base_url, params)
                payload = fetch_json(url, headers)
                data = payload.get("data") or {}
                if not isinstance(data, dict):
                    summary_row = {
                        "city": city_name,
                        "areaCode": area_code,
                        "areaLevel": area_level,
                        "keyword": keyword,
                        "page": page,
                        "returned_count": 0,
                        "total_pages_hint": 0,
                        "query_url": url,
                        "status": "blocked",
                        "message": "; ".join(
                            item.get("des", "")
                            for item in payload.get("global", [])
                            if isinstance(item, dict)
                        ),
                    }
                    append_jsonl(progress_log_path, [summary_row])
                    break
                listing_rows = data.get("list", [])
                pagination = data.get("pagenation", {})
                total_pages = int(pagination.get("total", 0) or 0)

                summary_row = {
                    "city": city_name,
                    "areaCode": area_code,
                    "areaLevel": area_level,
                    "keyword": keyword,
                    "page": page,
                    "returned_count": len(listing_rows),
                    "total_pages_hint": total_pages,
                    "query_url": url,
                    "status": "ok",
                    "message": "",
                }
                append_jsonl(progress_log_path, [summary_row])

                batch_rows = []
                for item in listing_rows:
                    job_id = item.get("jobId")
                    if not job_id:
                        continue

                    record = {
                        "platform": "ncss_jobs",
                        "city_seed": city_name,
                        "keyword_seed": keyword,
                        "area_level_seed": area_level,
                        "areaCode": area_code,
                        "query_page": page,
                        "detail_url": f"https://www.ncss.cn/student/jobs/{job_id}/detail.html",
                        **item,
                    }
                    batch_rows.append(record)
                    if job_id not in unique_jobs:
                        unique_jobs[job_id] = record

                if batch_rows:
                    append_jsonl(list_output_path, batch_rows)
                    raw_row_count += len(batch_rows)

                if not listing_rows:
                    break

                if total_pages and page >= total_pages:
                    break

                sleep_with_jitter(delay_seconds)

    detail_seed_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(detail_seed_output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["platform", "page_type", "url", "source_url", "city", "keyword"],
        )
        writer.writeheader()

        count = 0
        for row in unique_jobs.values():
            if args.max_jobs and count >= args.max_jobs:
                break
            writer.writerow(
                {
                    "platform": "ncss_jobs",
                    "page_type": "detail",
                    "url": row["detail_url"],
                    "source_url": config.get("referer", ""),
                    "city": row["city_seed"],
                    "keyword": row["keyword_seed"],
                }
            )
            count += 1

    ensure_parent(summary_output_path)
    summary_rows = load_jsonl(progress_log_path) if progress_log_path.exists() else []
    with open(summary_output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "city",
                "areaCode",
                "areaLevel",
                "keyword",
                "page",
                "returned_count",
                "total_pages_hint",
                "query_url",
                "status",
                "message",
            ],
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    print(
        f"saved {raw_row_count} raw rows, {len(unique_jobs)} unique jobs, "
        f"and {min(len(unique_jobs), args.max_jobs or len(unique_jobs))} detail seeds"
    )


if __name__ == "__main__":
    main()
