from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict, deque
from pathlib import Path

from common import ROOT_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 NCSS 列表结果中均衡抽取详情页种子")
    parser.add_argument(
        "--input",
        default="data/raw/ncss/records/ncss_listings_raw.jsonl",
        help="NCSS 列表原始 jsonl",
    )
    parser.add_argument(
        "--output",
        default="data/input/ncss/ncss_detail_urls_balanced.csv",
        help="输出详情页种子 csv",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=600,
        help="最多抽取的唯一职位数",
    )
    return parser.parse_args()


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def main() -> None:
    args = parse_args()
    rows = load_rows(ROOT_DIR / args.input)

    buckets: dict[tuple[str, str], deque] = defaultdict(deque)
    seen_job_ids = set()

    for row in rows:
        job_id = row.get("jobId")
        if not job_id or job_id in seen_job_ids:
            continue
        seen_job_ids.add(job_id)
        key = (row.get("city_seed", ""), row.get("keyword_seed", ""))
        buckets[key].append(row)

    ordered_keys = sorted(buckets.keys())
    selected_rows = []
    used_job_ids = set()

    while len(selected_rows) < args.max_jobs:
        progressed = False
        for key in ordered_keys:
            bucket = buckets[key]
            while bucket and bucket[0].get("jobId") in used_job_ids:
                bucket.popleft()
            if not bucket:
                continue

            row = bucket.popleft()
            job_id = row.get("jobId")
            if not job_id or job_id in used_job_ids:
                continue

            selected_rows.append(row)
            used_job_ids.add(job_id)
            progressed = True

            if len(selected_rows) >= args.max_jobs:
                break

        if not progressed:
            break

    output_path = ROOT_DIR / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["platform", "page_type", "url", "source_url", "city", "keyword"],
        )
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(
                {
                    "platform": "ncss_jobs",
                    "page_type": "detail",
                    "url": row["detail_url"],
                    "source_url": "https://www.ncss.cn/student/jobs/index.html",
                    "city": row.get("city_seed", ""),
                    "keyword": row.get("keyword_seed", ""),
                }
            )

    print(f"saved {len(selected_rows)} balanced detail seeds to {output_path}")


if __name__ == "__main__":
    main()
