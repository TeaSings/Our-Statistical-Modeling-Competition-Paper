from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import urljoin

from common import ROOT_DIR, load_json, load_jsonl, resolve_html_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从列表页 HTML 中提取职位详情链接")
    parser.add_argument("--manifest", required=True, help="列表页抓取日志 jsonl")
    parser.add_argument("--config", required=True, help="平台配置 json")
    parser.add_argument("--output", required=True, help="输出的详情页链接 CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    from bs4 import BeautifulSoup

    manifest_rows = load_jsonl(ROOT_DIR / args.manifest)
    config = load_json(ROOT_DIR / args.config)
    list_cfg = config["list_page"]
    output_path = ROOT_DIR / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    item_selector = list_cfg["item_selector"]
    detail_link_selector = list_cfg["detail_link_selector"]
    detail_link_attr = list_cfg.get("detail_link_attr", "href")
    base_url = list_cfg.get("base_url", "")

    seen = set()
    extracted_rows = []

    for row in manifest_rows:
        if row.get("page_type") != "list":
            continue
        if row.get("error"):
            continue

        html_path = resolve_html_path(
            local_path=row.get("local_path", ""),
            platform=row.get("platform", ""),
            page_type=row.get("page_type", ""),
            url=row.get("url", ""),
        )
        if html_path is None:
            continue

        html = html_path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select(item_selector)

        for item in items:
            link_node = item.select_one(detail_link_selector)
            if link_node is None:
                continue
            href = (link_node.get(detail_link_attr) or "").strip()
            if not href:
                continue

            detail_url = urljoin(base_url or row["url"], href)
            if detail_url in seen:
                continue
            seen.add(detail_url)

            extracted_rows.append(
                {
                    "platform": row.get("platform", ""),
                    "page_type": "detail",
                    "url": detail_url,
                    "source_url": row.get("url", ""),
                    "city": row.get("city", ""),
                    "keyword": row.get("keyword", ""),
                }
            )

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["platform", "page_type", "url", "source_url", "city", "keyword"],
        )
        writer.writeheader()
        writer.writerows(extracted_rows)

    print(f"extracted {len(extracted_rows)} detail urls to {output_path}")


if __name__ == "__main__":
    main()
