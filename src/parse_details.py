from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from common import ROOT_DIR, append_jsonl, clean_text, clean_text_list, load_json, load_jsonl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="解析详情页 HTML，抽取招聘字段")
    parser.add_argument("--manifest", required=True, help="详情页抓取日志 jsonl")
    parser.add_argument("--config", required=True, help="平台配置 json")
    parser.add_argument("--output", required=True, help="输出 jsonl")
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


def main() -> None:
    args = parse_args()
    from bs4 import BeautifulSoup

    manifest_rows = load_jsonl(ROOT_DIR / args.manifest)
    config = load_json(ROOT_DIR / args.config)
    detail_cfg = config["detail_page"]
    single_selectors = detail_cfg.get("single_selectors", {})
    multi_selectors = detail_cfg.get("multi_selectors", {})

    output_rows = []
    for row in manifest_rows:
        if row.get("page_type") != "detail":
            continue
        if row.get("error"):
            continue

        html_path = Path(row["local_path"])
        if not html_path.exists():
            continue

        html = html_path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")

        output_row = {
            "platform": row.get("platform", ""),
            "detail_url": row.get("url", ""),
            "source_url": row.get("source_url", ""),
            "city_seed": row.get("city", ""),
            "keyword_seed": row.get("keyword", ""),
        }

        for field, selectors in single_selectors.items():
            output_row[field] = extract_single_text(soup, as_selector_list(selectors))

        for field, selectors in multi_selectors.items():
            values = extract_multi_text(soup, as_selector_list(selectors))
            output_row[field] = " | ".join(values)

        output_rows.append(output_row)

    append_jsonl(ROOT_DIR / args.output, output_rows)
    print(f"parsed {len(output_rows)} detail pages")


if __name__ == "__main__":
    main()
