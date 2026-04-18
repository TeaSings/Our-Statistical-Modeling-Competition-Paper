from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "clean"
CLEAN_V2_DIR = DATA_DIR / "clean_v2"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"
STORAGE_CSV = DATA_DIR / "jobs.csv"
DEFAULT_OUTPUT_PATH = FINAL_DIR / "master_jobs.csv"

FINAL_HEADERS = [
    "site",
    "keyword",
    "city",
    "job_title",
    "company_name",
    "salary_raw",
    "salary_min",
    "salary_max",
    "salary_times_per_year",
    "education",
    "experience",
    "publish_time_raw",
    "publish_date",
    "job_url",
    "company_url",
    "job_description_raw",
    "skill_requirements_raw",
    "skill_keywords",
    "skill_tags",
    "skill_count",
    "source_file",
]

INTERNAL_FIELDS = FINAL_HEADERS + ["raw_tags", "_source_rank"]

SOURCE_RANKS = {
    "storage": 0,
    "processed": 1,
    "clean": 2,
    "clean_v2": 3,
    "raw": 4,
}

SPLIT_TAGS_RE = re.compile(r"[,，;/、|]+")
RANGE_SEPARATORS_RE = re.compile(r"[-~至]")

SKILL_PATTERNS: dict[str, tuple[str, ...]] = {
    "Python": (r"\bpython\b",),
    "SQL": (r"\bsql\b",),
    "机器学习": (r"机器学习", r"\bmachine learning\b", r"\bml\b"),
    "深度学习": (r"深度学习", r"\bdeep learning\b", r"神经网络"),
    "大模型": (r"大模型", r"\bllm\b", r"large language model", r"foundation model"),
    "AIGC": (r"\baigc\b", r"生成式ai", r"生成式人工智能", r"\bgenai\b", r"generative ai"),
    "RAG": (r"\brag\b", r"检索增强生成"),
    "NLP": (r"\bnlp\b", r"自然语言处理"),
    "数据分析": (r"数据分析", r"data analysis", r"商业分析", r"bi分析"),
    "推荐算法": (r"推荐算法", r"推荐系统", r"recommender"),
    "计算机视觉": (r"计算机视觉", r"computer vision", r"\bcv\b", r"3d视觉", r"点云"),
    "PyTorch": (r"\bpytorch\b",),
    "TensorFlow": (r"\btensorflow\b",),
    "Linux": (r"\blinux\b", r"\bunix\b"),
    "Spark": (r"\bspark\b",),
    "Hadoop": (r"\bhadoop\b",),
    "Tableau": (r"\btableau\b",),
    "Power BI": (r"\bpower\s*bi\b",),
    "Excel": (r"\bexcel\b",),
    "Java": (r"\bjava\b",),
    "C++": (r"\bc\+\+\b", r"\bcpp\b"),
    "R": (r"(?<![a-z0-9])r(?![a-z0-9])", r"\br语言\b"),
    "Git": (r"\bgit\b",),
    "Docker": (r"\bdocker\b",),
    "Kubernetes": (r"\bkubernetes\b", r"\bk8s\b"),
    "ROS": (r"\bros2?\b",),
    "强化学习": (r"强化学习", r"reinforcement learning", r"\brl\b"),
    "多模态": (r"多模态", r"multimodal", r"\bvlm\b"),
    "具身智能": (r"具身智能", r"embodied ai"),
    "Agent": (r"\bagent\b", r"智能体"),
    "世界模型": (r"世界模型", r"world model"),
    "SLAM": (r"\bslam\b",),
    "VIO": (r"\bvio\b",),
    "LIO": (r"\blio\b",),
}

REQUIREMENT_START_MARKERS = (
    "任职资格",
    "任职要求",
    "岗位要求",
    "职位要求",
    "岗位条件",
    "任职条件",
    "技能要求",
    "能力要求",
)

REQUIREMENT_END_MARKERS = (
    "加分项",
    "优先条件",
    "福利待遇",
    "薪资福利",
    "我们提供",
    "岗位职责",
    "工作职责",
    "职位亮点",
    "公司介绍",
)

REQUIREMENT_HINTS = (
    "熟悉",
    "掌握",
    "精通",
    "了解",
    "经验",
    "能力",
    "要求",
    "具备",
    "优先",
    "负责",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge current live crawl outputs into one research-ready master CSV."
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output CSV path. Default: project/data/final/master_jobs.csv",
    )
    return parser.parse_args()


def discover_input_files() -> list[tuple[str, Path]]:
    discovered: list[tuple[str, Path]] = []

    if STORAGE_CSV.exists():
        discovered.append(("storage", STORAGE_CSV))

    for stage, directory in (
        ("processed", PROCESSED_DIR),
        ("clean", CLEAN_DIR),
        ("clean_v2", CLEAN_V2_DIR),
        ("raw", RAW_DIR),
    ):
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.csv")):
            discovered.append((stage, path))

    return discovered


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def build_empty_record() -> dict[str, Any]:
    record = {field: "" for field in INTERNAL_FIELDS}
    record["_source_rank"] = -1
    return record


def first_non_empty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def normalize_text(value: Any) -> str:
    return first_non_empty(value).replace("\u00a0", " ")


def normalize_multiline_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = text.replace("\\n", "\n").replace("\\r", "\n").replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.split("\n")]
    kept = [line for line in lines if line]
    return "\n".join(kept)


def load_json_object(value: Any) -> dict[str, Any]:
    text = normalize_text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def canonicalize_url(value: Any) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""

    if raw.startswith("//"):
        raw = f"https:{raw}"

    try:
        split = urlsplit(raw)
    except ValueError:
        return raw.split("?", 1)[0].split("#", 1)[0]

    if not split.scheme and not split.netloc:
        return raw.split("?", 1)[0].split("#", 1)[0]

    return urlunsplit((split.scheme.lower(), split.netloc.lower(), split.path, "", ""))


def tags_to_text(value: Any) -> str:
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            if isinstance(item, str):
                token = normalize_text(item)
            elif isinstance(item, dict):
                token = first_non_empty(
                    item.get("jobTagName"),
                    item.get("name"),
                    item.get("labelName"),
                    item.get("tag"),
                )
            else:
                token = normalize_text(item)
            if token:
                items.append(token)
        return ",".join(items)
    return ""


def split_tags(text: Any) -> list[str]:
    normalized = normalize_text(text)
    if not normalized:
        return []
    tokens = [token.strip() for token in SPLIT_TAGS_RE.split(normalized) if token.strip()]
    return list(dict.fromkeys(tokens))


def merge_tag_strings(left: str, right: str) -> str:
    return ",".join(dict.fromkeys(split_tags(left) + split_tags(right)))


def parse_date_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    full_match = re.search(r"(\d{4})[./-年](\d{1,2})[./-月](\d{1,2})", text)
    if full_match:
        year, month, day = (int(part) for part in full_match.groups())
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return ""

    iso_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if iso_match:
        year, month, day = (int(part) for part in iso_match.groups())
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return ""

    compact_match = re.search(r"(\d{4})(\d{2})(\d{2})", text)
    if compact_match:
        year, month, day = (int(part) for part in compact_match.groups())
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return ""

    return ""


def convert_salary_number(value: float, unit: str) -> float:
    normalized = unit.lower()
    if normalized == "万":
        return value * 10000
    if normalized in {"千", "k"}:
        return value * 1000
    return value


def normalize_numeric_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        return str(int(round(float(text))))
    except ValueError:
        return ""


def parse_salary_range(salary_raw: str) -> tuple[str, str, str]:
    text = normalize_text(salary_raw).lower().replace(" ", "")
    if not text:
        return "", "", ""

    times_per_year = ""
    annual_match = re.search(r"(\d{1,2})薪", text)
    if annual_match:
        times_per_year = annual_match.group(1)
    elif "年薪" in text or "/年" in text:
        times_per_year = "1"
    elif any(marker in text for marker in ("万", "千", "k", "元")):
        times_per_year = "12"

    period_factor = 1.0
    if "年薪" in text or "/年" in text:
        period_factor = 1 / 12
    elif "/天" in text:
        period_factor = 21.75
    elif "/小时" in text or "/时" in text:
        period_factor = 8 * 21.75

    range_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(万|千|k|K|元)?\s*[-~至]\s*(\d+(?:\.\d+)?)\s*(万|千|k|K|元)?",
        text,
    )
    if range_match:
        low_unit = range_match.group(2) or range_match.group(4) or ""
        high_unit = range_match.group(4) or range_match.group(2) or ""
        low_value = convert_salary_number(float(range_match.group(1)), low_unit)
        high_value = convert_salary_number(float(range_match.group(3)), high_unit)
        return (
            str(int(round(low_value * period_factor))),
            str(int(round(high_value * period_factor))),
            times_per_year,
        )

    single_match = re.search(r"(\d+(?:\.\d+)?)\s*(万|千|k|K|元)?", text)
    if single_match:
        numeric = convert_salary_number(float(single_match.group(1)), single_match.group(2) or "")
        normalized = str(int(round(numeric * period_factor)))
        return normalized, normalized, times_per_year

    return "", "", times_per_year


def extract_payload_keyword(payload: dict[str, Any]) -> str:
    keyword = first_non_empty(payload.get("keyword"))
    if keyword:
        return keyword

    property_payload = load_json_object(payload.get("property"))
    return first_non_empty(property_payload.get("keyword"))


def extract_publish_time(row: dict[str, str], payload: dict[str, Any]) -> str:
    return first_non_empty(
        row.get("publish_time_raw"),
        row.get("publish_time"),
        payload.get("issueDateString"),
        payload.get("publishTime"),
        payload.get("publish_time"),
        payload.get("confirmDateString"),
        payload.get("issueDate"),
    )


def extract_raw_tags(row: dict[str, str], payload: dict[str, Any]) -> str:
    return first_non_empty(
        row.get("raw_tags"),
        tags_to_text(payload.get("jobTags")),
        tags_to_text(payload.get("jobTagsList")),
        tags_to_text(payload.get("raw_tags")),
    )


def build_candidate(row: dict[str, str], *, path: Path, stage: str) -> dict[str, Any]:
    payload = load_json_object(row.get("raw_payload"))
    record = build_empty_record()

    salary_raw = first_non_empty(
        row.get("salary_raw"),
        row.get("salary"),
        payload.get("provideSalaryString"),
        payload.get("salary"),
    )
    parsed_salary_min, parsed_salary_max, parsed_times_per_year = parse_salary_range(salary_raw)

    publish_time_raw = extract_publish_time(row, payload)
    publish_date = first_non_empty(parse_date_text(row.get("publish_date")), parse_date_text(publish_time_raw))

    record.update(
        {
            "site": first_non_empty(row.get("site"), row.get("source_site"), payload.get("site"), "job51"),
            "keyword": first_non_empty(row.get("keyword"), extract_payload_keyword(payload)),
            "city": first_non_empty(row.get("city"), row.get("location"), payload.get("jobAreaString"), payload.get("location")),
            "job_title": first_non_empty(row.get("job_title"), row.get("job_name"), payload.get("jobName"), payload.get("job_name")),
            "company_name": first_non_empty(
                row.get("company_name"),
                payload.get("companyName"),
                payload.get("company_name"),
                payload.get("fullCompanyName"),
            ),
            "salary_raw": salary_raw,
            "salary_min": first_non_empty(
                normalize_numeric_text(row.get("salary_min")),
                normalize_numeric_text(payload.get("jobSalaryMin")),
                parsed_salary_min,
            ),
            "salary_max": first_non_empty(
                normalize_numeric_text(row.get("salary_max")),
                normalize_numeric_text(payload.get("jobSalaryMax")),
                parsed_salary_max,
            ),
            "salary_times_per_year": first_non_empty(
                normalize_numeric_text(row.get("salary_times_per_year")),
                parsed_times_per_year,
            ),
            "education": first_non_empty(row.get("education"), payload.get("degreeString"), payload.get("education")),
            "experience": first_non_empty(
                row.get("experience"),
                payload.get("workYearString"),
                payload.get("experience"),
            ),
            "publish_time_raw": publish_time_raw,
            "publish_date": publish_date,
            "job_url": canonicalize_url(
                first_non_empty(
                    row.get("job_url"),
                    row.get("original_url"),
                    row.get("source_url"),
                    payload.get("jobHref"),
                    payload.get("job_url"),
                )
            ),
            "company_url": canonicalize_url(
                first_non_empty(
                    row.get("company_url"),
                    payload.get("companyHref"),
                    payload.get("company_url"),
                )
            ),
            "job_description_raw": normalize_multiline_text(
                first_non_empty(
                    row.get("job_description_raw"),
                    payload.get("jobDescribe"),
                    payload.get("job_description_raw"),
                    row.get("summary"),
                )
            ),
            "source_file": first_non_empty(row.get("source_file"), path.name),
            "raw_tags": extract_raw_tags(row, payload),
            "_source_rank": SOURCE_RANKS[stage],
        }
    )
    return record


def build_record_key(record: dict[str, Any]) -> tuple[str, str]:
    job_url = canonicalize_url(record.get("job_url"))
    if job_url:
        return "job_url", job_url

    composite = "||".join(
        normalize_text(record.get(field)).casefold()
        for field in ("job_title", "company_name", "city", "publish_date")
    ).strip("|")
    if composite:
        return "job_meta", composite

    fallback = "||".join(
        normalize_text(record.get(field)).casefold()
        for field in ("source_file", "job_title", "company_name")
    )
    return "fallback", fallback


def merge_records(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    merged = dict(current)

    for field in INTERNAL_FIELDS:
        if field == "_source_rank":
            continue
        current_value = normalize_text(merged.get(field))
        candidate_value = normalize_text(candidate.get(field))
        if not current_value and candidate_value:
            merged[field] = candidate_value

    if len(normalize_text(candidate.get("job_description_raw"))) > len(normalize_text(current.get("job_description_raw"))):
        merged["job_description_raw"] = candidate.get("job_description_raw", "")
        if candidate.get("source_file"):
            merged["source_file"] = candidate.get("source_file", merged.get("source_file", ""))

    merged["raw_tags"] = merge_tag_strings(
        normalize_text(current.get("raw_tags")),
        normalize_text(candidate.get("raw_tags")),
    )

    if int(candidate.get("_source_rank", -1)) >= int(current.get("_source_rank", -1)):
        for field in (
            "site",
            "keyword",
            "city",
            "job_title",
            "company_name",
            "salary_raw",
            "salary_min",
            "salary_max",
            "salary_times_per_year",
            "education",
            "experience",
            "publish_time_raw",
            "publish_date",
            "job_url",
            "company_url",
            "source_file",
        ):
            candidate_value = normalize_text(candidate.get(field))
            if candidate_value:
                merged[field] = candidate_value

    merged["_source_rank"] = max(int(current.get("_source_rank", -1)), int(candidate.get("_source_rank", -1)))
    return merged


def extract_requirement_section(description: str) -> str:
    text = normalize_multiline_text(description)
    if not text:
        return ""

    lowered = text.lower()
    for marker in REQUIREMENT_START_MARKERS:
        start = lowered.find(marker.lower())
        if start < 0:
            continue
        end = len(text)
        for end_marker in REQUIREMENT_END_MARKERS:
            candidate_end = lowered.find(end_marker.lower(), start + len(marker))
            if candidate_end > start:
                end = min(end, candidate_end)
        section = text[start:end].strip()
        if section:
            return section

    sentences = re.split(r"[\n。；;]", text)
    selected: list[str] = []
    for sentence in sentences:
        snippet = normalize_text(sentence)
        if not snippet:
            continue
        lowered_snippet = snippet.lower()
        if any(hint in snippet for hint in REQUIREMENT_HINTS) or any(
            re.search(pattern, lowered_snippet, flags=re.IGNORECASE)
            for patterns in SKILL_PATTERNS.values()
            for pattern in patterns
        ):
            selected.append(snippet)
        if len(selected) >= 12:
            break

    return "\n".join(dict.fromkeys(selected))


def extract_skill_fields(*, title: str, raw_tags: str, description: str, requirement_text: str) -> tuple[list[str], list[str]]:
    corpus = "\n".join(
        part
        for part in (
            normalize_text(title),
            normalize_text(raw_tags),
            normalize_multiline_text(requirement_text),
            normalize_multiline_text(description),
        )
        if part
    )

    raw_tag_tokens = split_tags(raw_tags)
    keywords: list[str] = []
    tags: list[str] = []

    for canonical, patterns in SKILL_PATTERNS.items():
        matched = any(re.search(pattern, corpus, flags=re.IGNORECASE) for pattern in patterns)
        if not matched:
            continue

        keywords.append(canonical)
        matched_tokens = [
            token
            for token in raw_tag_tokens
            if any(re.search(pattern, token, flags=re.IGNORECASE) for pattern in patterns)
        ]
        if matched_tokens:
            tags.extend(matched_tokens)
        else:
            tags.append(canonical)

    return keywords, list(dict.fromkeys(tags))


def finalize_record(record: dict[str, Any]) -> dict[str, str]:
    requirement_text = extract_requirement_section(normalize_text(record.get("job_description_raw")))
    if not requirement_text:
        requirement_text = normalize_text(record.get("raw_tags"))

    skill_keywords, skill_tags = extract_skill_fields(
        title=normalize_text(record.get("job_title")),
        raw_tags=normalize_text(record.get("raw_tags")),
        description=normalize_text(record.get("job_description_raw")),
        requirement_text=requirement_text,
    )

    return {
        "site": normalize_text(record.get("site")),
        "keyword": normalize_text(record.get("keyword")),
        "city": normalize_text(record.get("city")),
        "job_title": normalize_text(record.get("job_title")),
        "company_name": normalize_text(record.get("company_name")),
        "salary_raw": normalize_text(record.get("salary_raw")),
        "salary_min": normalize_text(record.get("salary_min")),
        "salary_max": normalize_text(record.get("salary_max")),
        "salary_times_per_year": normalize_text(record.get("salary_times_per_year")),
        "education": normalize_text(record.get("education")),
        "experience": normalize_text(record.get("experience")),
        "publish_time_raw": normalize_text(record.get("publish_time_raw")),
        "publish_date": normalize_text(record.get("publish_date")),
        "job_url": canonicalize_url(record.get("job_url")),
        "company_url": canonicalize_url(record.get("company_url")),
        "job_description_raw": normalize_text(record.get("job_description_raw")),
        "skill_requirements_raw": requirement_text,
        "skill_keywords": ",".join(skill_keywords),
        "skill_tags": ",".join(skill_tags),
        "skill_count": str(len(skill_keywords)),
        "source_file": normalize_text(record.get("source_file")),
    }


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FINAL_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    output_path = Path(args.output).expanduser().resolve()

    input_files = discover_input_files()
    if not input_files:
        print("No live crawl CSV files were found under project/data/.")
        return 1

    stage_file_counts: Counter[str] = Counter()
    stage_row_counts: Counter[str] = Counter()
    merged_records: dict[tuple[str, str], dict[str, Any]] = {}

    for stage, path in input_files:
        rows = read_rows(path)
        stage_file_counts[stage] += 1
        stage_row_counts[stage] += len(rows)

        for row in rows:
            candidate = build_candidate(row, path=path, stage=stage)
            key = build_record_key(candidate)
            existing = merged_records.get(key)
            if existing is None:
                merged_records[key] = candidate
            else:
                merged_records[key] = merge_records(existing, candidate)

    final_rows = [finalize_record(record) for record in merged_records.values()]
    final_rows = [
        row
        for row in final_rows
        if row["job_title"] and row["job_url"]
    ]
    final_rows.sort(
        key=lambda row: (
            row["site"],
            row["keyword"],
            row["publish_date"],
            row["job_title"],
            row["company_name"],
            row["job_url"],
        )
    )
    write_rows(output_path, final_rows)

    print("Merge summary:")
    print(f"  input_files: {len(input_files)}")
    print(f"  input_file_counts_by_stage: {dict(stage_file_counts)}")
    print(f"  input_row_counts_by_stage: {dict(stage_row_counts)}")
    print(f"  unique_rows_written: {len(final_rows)}")
    print(f"  output_file: {output_path}")
    print(f"  output_fields: {', '.join(FINAL_HEADERS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
