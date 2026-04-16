from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from common import DEFAULT_HEADERS, ROOT_DIR, append_jsonl, ensure_parent, load_json, sleep_with_jitter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量抓取 NCSS 职位列表并导出详情页链接")
    parser.add_argument(
        "--config",
        default="data/input/ncss_batch_config.json",
        help="批量配置文件",
    )
    parser.add_argument(
        "--list-output",
        default="data/raw/records/ncss_listings_raw.jsonl",
        help="职位列表原始输出 jsonl",
    )
    parser.add_argument(
        "--detail-seed-output",
        default="data/input/ncss_detail_urls_generated.csv",
        help="详情页种子输出 csv",
    )
    parser.add_argument(
        "--summary-output",
        default="data/processed/ncss_list_query_summary.csv",
        help="查询摘要输出 csv",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=600,
        help="最多导出多少个唯一职位详情链接，0 表示不限制",
    )
    return parser.parse_args()


def fetch_json(url: str, headers: dict[str, str]) -> dict:
    request = Request(url, headers=headers)
    with urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8", errors="ignore")
    return json.loads(body)


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

    cities = config["cities"]
    keywords = config["keywords"]
    base_params = config["request_params"]
    base_url = config["list_api"]
    limit = int(config.get("limit", 20))
    max_pages_per_query = int(config.get("max_pages_per_query", 3))
    delay_seconds = float(config.get("delay_seconds", 0.2))

    raw_rows: list[dict] = []
    summary_rows: list[dict] = []
    unique_jobs: dict[str, dict] = {}

    for city in cities:
        for keyword in keywords:
            city_name = city["name"]
            area_code = city["areaCode"]
            page = 1
            total_pages = None

            while page <= max_pages_per_query and (total_pages is None or page <= total_pages):
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
                data = payload.get("data", {})
                listing_rows = data.get("list", [])
                pagination = data.get("pagenation", {})
                total_pages = int(pagination.get("total", 0) or 0)

                summary_rows.append(
                    {
                        "city": city_name,
                        "areaCode": area_code,
                        "keyword": keyword,
                        "page": page,
                        "returned_count": len(listing_rows),
                        "total_pages_hint": total_pages,
                        "query_url": url,
                    }
                )

                for item in listing_rows:
                    job_id = item.get("jobId")
                    if not job_id:
                        continue

                    record = {
                        "platform": "ncss_jobs",
                        "city_seed": city_name,
                        "keyword_seed": keyword,
                        "areaCode": area_code,
                        "query_page": page,
                        "detail_url": f"https://www.ncss.cn/student/jobs/{job_id}/detail.html",
                        **item,
                    }
                    raw_rows.append(record)
                    if job_id not in unique_jobs:
                        unique_jobs[job_id] = record

                if not listing_rows:
                    break

                page += 1
                sleep_with_jitter(delay_seconds)

    append_jsonl(list_output_path, raw_rows)

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
    with open(summary_output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "city",
                "areaCode",
                "keyword",
                "page",
                "returned_count",
                "total_pages_hint",
                "query_url",
            ],
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    print(
        f"saved {len(raw_rows)} raw rows, {len(unique_jobs)} unique jobs, "
        f"and {min(len(unique_jobs), args.max_jobs or len(unique_jobs))} detail seeds"
    )


if __name__ == "__main__":
    main()
