from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import ROOT_DIR, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="为人工搜集招聘种子链接生成任务表")
    parser.add_argument(
        "--registry",
        default="data/input/sources/job_source_registry.json",
        help="数据源注册表 json",
    )
    parser.add_argument(
        "--sources",
        default="mohrss_public,ncss_jobs,zhaopin_detail",
        help="逗号分隔的平台名",
    )
    parser.add_argument(
        "--cities",
        default="北京,上海,广州,深圳,杭州",
        help="逗号分隔的城市名",
    )
    parser.add_argument(
        "--keywords",
        default="Python,数据分析,运营,行政,会计",
        help="逗号分隔的关键词",
    )
    parser.add_argument(
        "--output",
        default="data/input/sources/manual_seed_tasks.csv",
        help="输出 csv",
    )
    return parser.parse_args()


def split_csv_text(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    args = parse_args()
    registry = load_json(ROOT_DIR / args.registry)
    source_names = set(split_csv_text(args.sources))
    cities = split_csv_text(args.cities)
    keywords = split_csv_text(args.keywords)

    selected_sources = [row for row in registry if row["platform"] in source_names]
    if not selected_sources:
        raise SystemExit("no sources selected")

    output_path = ROOT_DIR / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for source in selected_sources:
        for city in cities:
            for keyword in keywords:
                rows.append(
                    {
                        "platform": source["platform"],
                        "page_type": "list",
                        "url": "",
                        "entry_url": source["entry_url"],
                        "city": city if source.get("supports_city") else "",
                        "keyword": keyword if source.get("supports_keyword") else "",
                        "manual_note": source["notes"],
                    }
                )

    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "platform",
                "page_type",
                "url",
                "entry_url",
                "city",
                "keyword",
                "manual_note",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"saved {len(rows)} manual tasks to {output_path}")


if __name__ == "__main__":
    main()
