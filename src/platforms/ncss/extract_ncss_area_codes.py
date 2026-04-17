from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from bs4 import BeautifulSoup

CURRENT_FILE = Path(__file__).resolve()
SRC_DIR = next((parent for parent in CURRENT_FILE.parents if parent.name == "src"), None)
if SRC_DIR is None:
    raise RuntimeError("could not locate src directory from extract_ncss_area_codes.py")
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, clean_text, ensure_parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从 NCSS 列表页 HTML 中提取地区代码")
    parser.add_argument(
        "--input-html",
        default="data/raw/ncss/html/ncss_jobs/list/47358e6f4a77664f9f57198b20032c5b3398a691.html",
        help="NCSS 职位列表页 HTML 路径",
    )
    parser.add_argument(
        "--output-json",
        default="data/input/ncss/ncss_area_codes_all.json",
        help="输出 JSON 路径",
    )
    parser.add_argument(
        "--output-csv",
        default="data/input/ncss/ncss_area_codes_all.csv",
        help="输出 CSV 路径",
    )
    parser.add_argument(
        "--scope",
        choices=["all", "prefecture"],
        default="all",
        help="all 为全部非空地区码，prefecture 仅保留省级/地级市层级",
    )
    return parser.parse_args()


def classify_level(code: str) -> str:
    if len(code) == 2:
        return "province"
    if len(code) == 6 and code.endswith("00"):
        return "prefecture"
    if len(code) == 6:
        return "district"
    return "other"


def should_keep(code: str, scope: str) -> bool:
    if scope == "all":
        return True
    return classify_level(code) in {"province", "prefecture"}


def main() -> None:
    args = parse_args()
    html_path = ROOT_DIR / args.input_html
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    by_code: dict[str, dict] = {}
    for tag in soup.select("li.areacode, span.areacode"):
        code = (tag.get("data-area") or "").strip()
        name = clean_text(tag.get_text(strip=True))
        if not code or not name or not should_keep(code, args.scope):
            continue

        row = {
            "name": name,
            "areaCode": code,
            "level": classify_level(code),
        }
        previous = by_code.get(code)
        if previous is None or len(name) > len(previous["name"]):
            by_code[code] = row

    rows = sorted(by_code.values(), key=lambda item: (item["areaCode"], item["name"]))

    output_json_path = ROOT_DIR / args.output_json
    output_csv_path = ROOT_DIR / args.output_csv
    ensure_parent(output_json_path)
    ensure_parent(output_csv_path)

    with output_json_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    with output_csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "areaCode", "level"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"saved {len(rows)} areas to {output_json_path} and {output_csv_path}")


if __name__ == "__main__":
    main()
