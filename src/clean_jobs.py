from __future__ import annotations

import argparse
import csv
import json
import re

from common import ROOT_DIR, clean_text


CITY_ALIASES = {
    "北京市": "北京",
    "上海市": "上海",
    "广州市": "广州",
    "深圳市": "深圳",
    "杭州市": "杭州",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清洗招聘字段并去重")
    parser.add_argument("--input", required=True, help="原始 jsonl")
    parser.add_argument("--output", required=True, help="输出 csv")
    return parser.parse_args()


def load_jsonl(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def normalize_city(value: str, fallback: str) -> str:
    value = clean_text(value) or clean_text(fallback)
    if not value:
        return ""
    value = re.sub(r"[·•]", "", value)
    return CITY_ALIASES.get(value, value)


def normalize_education(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    mapping = [
        ("博士", "博士"),
        ("硕士", "硕士"),
        ("研究生", "硕士"),
        ("本科", "本科"),
        ("大专", "大专"),
        ("专科", "大专"),
        ("中专", "高中/中专"),
        ("高中", "高中/中专"),
        ("不限", "不限"),
    ]
    for keyword, label in mapping:
        if keyword in value:
            return label
    return value


def normalize_experience(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    mapping = [
        ("不限", "不限"),
        ("应届", "应届"),
        ("无经验", "应届"),
        ("1年以下", "1年以下"),
        ("1-3年", "1-3年"),
        ("2年", "1-3年"),
        ("3-5年", "3-5年"),
        ("5-10年", "5-10年"),
        ("10年以上", "10年以上"),
    ]
    for keyword, label in mapping:
        if keyword in value:
            return label
    return value


def parse_salary(raw: str) -> tuple[float | None, float | None, float | None]:
    text = clean_text(raw).lower()
    if not text or "面议" in text:
        return None, None, None

    text = re.sub(r"[·•]\s*\d+\s*薪", "", text)

    patterns = [
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*k", 1000 / 1),
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*千", 1000 / 1),
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*万/年", 10000 / 12),
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*元/天", 21.75),
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*元/时", 21.75 * 8),
        (r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*元/月", 1),
    ]

    for pattern, multiplier in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        low = float(match.group(1)) * multiplier
        high = float(match.group(2)) * multiplier
        avg = round((low + high) / 2, 2)
        return round(low, 2), round(high, 2), avg

    return None, None, None


def build_clean_row(row: dict, index: int) -> dict:
    city_raw = row.get("city_raw", "")
    city_seed = row.get("city_seed", "")
    salary_min, salary_max, salary_avg = parse_salary(row.get("salary_raw", ""))

    return {
        "job_id": f"job_{index:06d}",
        "source_job_id": row.get("job_id", ""),
        "platform": row.get("platform", ""),
        "detail_url": row.get("detail_url", ""),
        "source_url": row.get("source_url", ""),
        "city_seed": city_seed,
        "keyword_seed": row.get("keyword_seed", ""),
        "job_title_raw": row.get("job_title_raw", ""),
        "job_title_std": clean_text(row.get("job_title_raw", "")),
        "company_name_raw": row.get("company_name_raw", ""),
        "company_name_std": clean_text(row.get("company_name_raw", "")),
        "city_raw": city_raw,
        "city_std": normalize_city(city_raw, city_seed),
        "salary_raw": row.get("salary_raw", ""),
        "salary_min_month": salary_min,
        "salary_max_month": salary_max,
        "salary_avg_month": salary_avg,
        "education_raw": row.get("education_raw", ""),
        "education_std": normalize_education(row.get("education_raw", "")),
        "experience_raw": row.get("experience_raw", ""),
        "experience_std": normalize_experience(row.get("experience_raw", "")),
        "company_industry_raw": row.get("company_industry_raw", ""),
        "company_size_raw": row.get("company_size_raw", ""),
        "job_tags_raw": row.get("job_tags_raw", ""),
        "jd_text_raw": row.get("jd_text_raw", ""),
        "jd_text_clean": clean_text(row.get("jd_text_raw", "")),
    }


def main() -> None:
    args = parse_args()
    rows = load_jsonl(str(ROOT_DIR / args.input))
    if not rows:
        raise SystemExit("input file is empty")

    cleaned_rows = []
    seen = set()
    for index, row in enumerate(rows, start=1):
        clean_row = build_clean_row(row, index)
        dedupe_key = (
            clean_row["job_title_std"],
            clean_row["company_name_std"],
            clean_row["city_std"],
            clean_row["salary_raw"],
            clean_row["jd_text_clean"],
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        cleaned_rows.append(clean_row)

    output_path = ROOT_DIR / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(cleaned_rows[0].keys())
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"saved {len(cleaned_rows)} cleaned jobs to {output_path}")


if __name__ == "__main__":
    main()
