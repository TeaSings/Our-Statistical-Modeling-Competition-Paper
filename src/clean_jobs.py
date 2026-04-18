from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import re
from pathlib import Path
from typing import Iterator

from common import ROOT_DIR, clean_text


CITY_ALIASES = {
    "北京市": "北京",
    "上海市": "上海",
    "天津市": "天津",
    "重庆市": "重庆",
    "广州市": "广州",
    "深圳市": "深圳",
    "杭州市": "杭州",
}
PROVINCE_LEVEL_CITIES = {"北京", "上海", "天津", "重庆"}
FIELDNAMES = [
    "job_id",
    "source_job_id",
    "platform",
    "detail_url",
    "source_url",
    "city_seed",
    "keyword_seed",
    "job_title_raw",
    "job_title_std",
    "company_name_raw",
    "company_name_std",
    "company_type_raw",
    "city_raw",
    "province_std",
    "city_std",
    "district_std",
    "work_area_code",
    "salary_raw",
    "salary_bound_kind",
    "salary_pay_months",
    "salary_min_month",
    "salary_max_month",
    "salary_avg_month",
    "salary_min_annualized",
    "salary_max_annualized",
    "salary_avg_annualized",
    "education_raw",
    "education_std",
    "experience_raw",
    "experience_std",
    "publish_time_raw",
    "publish_time_std",
    "update_time_raw",
    "update_time_std",
    "apply_time_text_raw",
    "company_industry_raw",
    "company_size_raw",
    "job_tags_raw",
    "jd_text_raw",
    "jd_text_clean",
    "jd_char_count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清洗招聘字段并去重")
    parser.add_argument("--input", required=True, help="原始 jsonl")
    parser.add_argument("--output", required=True, help="输出 csv")
    parser.add_argument("--allow-empty-jd", action="store_true", help="保留没有正文的记录")
    return parser.parse_args()


def iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def normalize_plain_text(value: str | None) -> str:
    text = html.unescape(clean_text(value))
    text = text.replace("\u200b", "")
    text = text.replace("\ufeff", "")
    text = text.replace("\r", "\n")
    text = text.replace("\t", " ")
    text = text.replace("•", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_jd_text(value: str | None) -> str:
    text = html.unescape(value or "")
    text = text.replace("\u200b", "")
    text = text.replace("\ufeff", "")
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u3000", " ")
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    lines = [line for line in lines if line]
    return " ".join(lines).strip()


def normalize_region_name(value: str | None) -> str:
    text = normalize_plain_text(value)
    if not text:
        return ""
    return CITY_ALIASES.get(text, text)


def normalize_city_name(value: str | None) -> str:
    text = normalize_region_name(value)
    if not text:
        return ""
    text = re.split(r"[·•/]", text)[0].strip()
    if text.endswith("市") and len(text) <= 4:
        text = text[:-1]
    return CITY_ALIASES.get(text, text)


def normalize_province_name(value: str | None) -> str:
    text = normalize_region_name(value)
    if not text:
        return ""
    if text.endswith("省") or text.endswith("市"):
        text = text[:-1]
    if text.endswith("自治区"):
        text = text[:-3]
    return text


def normalize_district_name(value: str | None) -> str:
    return normalize_region_name(value)


def derive_geo_fields(row: dict) -> tuple[str, str, str]:
    province = normalize_province_name(row.get("job_area_level_province", ""))
    city = normalize_city_name(row.get("job_area_level_city", ""))
    district = normalize_district_name(row.get("job_area_level_district", ""))
    city_raw = normalize_plain_text(row.get("city_raw", ""))

    if not city and city_raw:
        parts = [part.strip() for part in re.split(r"[·•/]", city_raw) if clean_text(part)]
        if parts:
            city = normalize_city_name(parts[0])
        if len(parts) > 1 and not district:
            district = normalize_district_name(parts[1])
    if not city:
        city = normalize_city_name(row.get("city_seed", ""))
    if not province and city in PROVINCE_LEVEL_CITIES:
        province = city
    return province, city, district


def normalize_education(value: str) -> str:
    text = normalize_plain_text(value)
    if not text:
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
        if keyword in text:
            return label
    return text


def normalize_experience(value: str) -> str:
    text = normalize_plain_text(value)
    if not text:
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
        if keyword in text:
            return label
    return text


def normalize_datetime(value: str) -> str:
    text = normalize_plain_text(value)
    if not text:
        return ""
    text = text.replace("/", "-").replace(".", "-")
    text = re.sub(r"\s+", " ", text).strip()
    now = dt.datetime.now()
    candidates = [
        ("%Y-%m-%d %H:%M:%S", lambda parsed: parsed.strftime("%Y-%m-%d %H:%M:%S")),
        ("%Y-%m-%d %H:%M", lambda parsed: parsed.strftime("%Y-%m-%d %H:%M:00")),
        ("%Y-%m-%d", lambda parsed: parsed.strftime("%Y-%m-%d 00:00:00")),
        ("%m-%d %H:%M:%S", lambda parsed: parsed.replace(year=now.year).strftime("%Y-%m-%d %H:%M:%S")),
        ("%m-%d %H:%M", lambda parsed: parsed.replace(year=now.year).strftime("%Y-%m-%d %H:%M:00")),
        ("%m-%d", lambda parsed: parsed.replace(year=now.year).strftime("%Y-%m-%d 00:00:00")),
    ]
    for input_fmt, formatter in candidates:
        try:
            parsed = dt.datetime.strptime(text, input_fmt)
            return formatter(parsed)
        except ValueError:
            continue
    return text


def extract_salary_pay_months(raw: str) -> int | None:
    text = normalize_plain_text(raw).lower()
    if not text:
        return None
    match = re.search(r"(?:[·•]?\s*)(\d{1,2})\s*薪", text)
    if not match:
        return 12
    return max(int(match.group(1)), 1)


def salary_unit_to_monthly_factor(unit: str) -> float:
    mapping = {
        "k": 1000.0,
        "千": 1000.0,
        "万": 10000.0,
        "元/月": 1.0,
        "元/天": 21.75,
        "元/时": 21.75 * 8,
        "元/年": 1.0 / 12.0,
        "万/年": 10000.0 / 12.0,
        "万/月": 10000.0,
        "千/月": 1000.0,
    }
    return mapping[unit]


def parse_salary(raw: str) -> tuple[float | None, float | None, float | None, int | None, str]:
    text = normalize_plain_text(raw).lower()
    pay_months = extract_salary_pay_months(text)
    if not text:
        return None, None, None, pay_months, ""
    if "面议" in text:
        return None, None, None, pay_months, "negotiable"

    text = re.sub(r"(?:[·•]?\s*)\d{1,2}\s*薪", "", text)
    text = text.replace("／", "/")
    text = text.replace("—", "-").replace("–", "-").replace("~", "-").replace("至", "-")
    text = re.sub(r"\s+", "", text)

    units = ["万/年", "万/月", "千/月", "元/月", "元/天", "元/时", "元/年", "k", "千", "万"]
    for unit in units:
        factor = salary_unit_to_monthly_factor(unit)

        range_match = re.search(
            rf"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*{re.escape(unit)}",
            text,
        )
        if range_match:
            low = float(range_match.group(1)) * factor
            high = float(range_match.group(2)) * factor
            avg = round((low + high) / 2.0, 2)
            return round(low, 2), round(high, 2), avg, pay_months, "range"

        upper_match = re.search(
            rf"(\d+(?:\.\d+)?)\s*{re.escape(unit)}\s*(?:及以下|以下|以内)",
            text,
        )
        if upper_match:
            value = round(float(upper_match.group(1)) * factor, 2)
            return value, value, value, pay_months, "upper"

        lower_match = re.search(
            rf"(\d+(?:\.\d+)?)\s*{re.escape(unit)}\s*(?:及以上|以上|起)",
            text,
        )
        if lower_match:
            value = round(float(lower_match.group(1)) * factor, 2)
            return value, value, value, pay_months, "lower"

        exact_match = re.search(rf"^(\d+(?:\.\d+)?)\s*{re.escape(unit)}$", text)
        if exact_match:
            value = round(float(exact_match.group(1)) * factor, 2)
            return value, value, value, pay_months, "exact"

    return None, None, None, pay_months, "unknown"


def annualize_salary(monthly_value: float | None, pay_months: int | None) -> float | None:
    if monthly_value is None or not pay_months:
        return None
    return round(monthly_value * pay_months, 2)


def normalize_tag_field(value: str | None) -> str:
    text = normalize_plain_text(value)
    if not text:
        return ""
    if "|" not in text:
        return text
    tags: list[str] = []
    seen: set[str] = set()
    for part in text.split("|"):
        tag = normalize_plain_text(part)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return " | ".join(tags)


def build_clean_row(row: dict) -> dict:
    source_job_id = normalize_plain_text(row.get("source_job_id", "") or row.get("job_id", ""))
    city_seed = normalize_plain_text(row.get("city_seed", ""))
    keyword_seed = normalize_plain_text(row.get("keyword_seed", ""))
    city_raw = normalize_plain_text(row.get("city_raw", ""))
    province_std, city_std, district_std = derive_geo_fields(row)
    salary_min, salary_max, salary_avg, pay_months, bound_kind = parse_salary(row.get("salary_raw", ""))
    jd_text_raw = normalize_jd_text(row.get("jd_text_raw", ""))
    jd_text_clean = jd_text_raw

    return {
        "job_id": "",
        "source_job_id": source_job_id,
        "platform": normalize_plain_text(row.get("platform", "")),
        "detail_url": normalize_plain_text(row.get("detail_url", "")),
        "source_url": normalize_plain_text(row.get("source_url", "")),
        "city_seed": city_seed,
        "keyword_seed": keyword_seed,
        "job_title_raw": normalize_plain_text(row.get("job_title_raw", "")),
        "job_title_std": normalize_plain_text(row.get("job_title_raw", "")),
        "company_name_raw": normalize_plain_text(row.get("company_name_raw", "")),
        "company_name_std": normalize_plain_text(row.get("company_name_raw", "")),
        "company_type_raw": normalize_plain_text(row.get("company_type_raw", "")),
        "city_raw": city_raw,
        "province_std": province_std,
        "city_std": city_std,
        "district_std": district_std,
        "work_area_code": normalize_plain_text(row.get("work_area_code", "")),
        "salary_raw": normalize_plain_text(row.get("salary_raw", "")),
        "salary_bound_kind": bound_kind,
        "salary_pay_months": pay_months,
        "salary_min_month": salary_min,
        "salary_max_month": salary_max,
        "salary_avg_month": salary_avg,
        "salary_min_annualized": annualize_salary(salary_min, pay_months),
        "salary_max_annualized": annualize_salary(salary_max, pay_months),
        "salary_avg_annualized": annualize_salary(salary_avg, pay_months),
        "education_raw": normalize_plain_text(row.get("education_raw", "")),
        "education_std": normalize_education(row.get("education_raw", "")),
        "experience_raw": normalize_plain_text(row.get("experience_raw", "")),
        "experience_std": normalize_experience(row.get("experience_raw", "")),
        "publish_time_raw": normalize_plain_text(row.get("publish_time_raw", "")),
        "publish_time_std": normalize_datetime(row.get("publish_time_raw", "")),
        "update_time_raw": normalize_plain_text(row.get("update_time_raw", "")),
        "update_time_std": normalize_datetime(row.get("update_time_raw", "")),
        "apply_time_text_raw": normalize_plain_text(row.get("apply_time_text_raw", "")),
        "company_industry_raw": normalize_plain_text(row.get("company_industry_raw", "")),
        "company_size_raw": normalize_plain_text(row.get("company_size_raw", "")),
        "job_tags_raw": normalize_tag_field(row.get("job_tags_raw", "")),
        "jd_text_raw": jd_text_raw,
        "jd_text_clean": jd_text_clean,
        "jd_char_count": len(jd_text_clean),
    }


def dedupe_content_key(clean_row: dict) -> tuple[str, str, str, str, str]:
    return (
        clean_row["job_title_std"],
        clean_row["company_name_std"],
        clean_row["city_std"],
        clean_row["salary_raw"],
        clean_row["jd_text_clean"],
    )


def clean_jsonl_to_csv(
    input_path: Path,
    output_path: Path,
    *,
    allow_empty_jd: bool = False,
) -> tuple[int, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    cleaned_count = 0
    dropped_empty_jd = 0
    seen_source_ids: set[str] = set()
    seen_detail_urls: set[str] = set()
    seen_content_keys: set[tuple[str, str, str, str, str]] = set()

    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for row in iter_jsonl(input_path):
            total_rows += 1
            clean_row = build_clean_row(row)
            if (not clean_row["job_title_std"] or not clean_row["jd_text_clean"]) and not allow_empty_jd:
                dropped_empty_jd += 1
                continue

            source_job_id = clean_row["source_job_id"]
            detail_url = clean_row["detail_url"]
            content_key = dedupe_content_key(clean_row)
            if source_job_id and source_job_id in seen_source_ids:
                continue
            if detail_url and detail_url in seen_detail_urls:
                continue
            if content_key in seen_content_keys:
                continue

            if source_job_id:
                seen_source_ids.add(source_job_id)
            if detail_url:
                seen_detail_urls.add(detail_url)
            seen_content_keys.add(content_key)

            cleaned_count += 1
            clean_row["job_id"] = f"job_{cleaned_count:06d}"
            writer.writerow(clean_row)

    if total_rows == 0:
        raise SystemExit("input file is empty")
    if cleaned_count == 0:
        raise SystemExit("no cleaned rows produced")
    return cleaned_count, dropped_empty_jd


def main() -> None:
    args = parse_args()
    input_path = ROOT_DIR / args.input
    output_path = ROOT_DIR / args.output
    cleaned_count, dropped_empty_jd = clean_jsonl_to_csv(
        input_path,
        output_path,
        allow_empty_jd=args.allow_empty_jd,
    )

    print(
        f"saved {cleaned_count} cleaned jobs to {output_path}; "
        f"dropped_empty_jd={dropped_empty_jd}"
    )


if __name__ == "__main__":
    main()
