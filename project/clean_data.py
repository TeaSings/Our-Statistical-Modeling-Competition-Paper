from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
CLEAN_DIR = PROJECT_DIR / "data" / "clean"
CLEAN_V2_DIR = PROJECT_DIR / "data" / "clean_v2"
OUTPUT_FIELDS = [
    "crawl_time",
    "crawl_date",
    "publish_time",
    "publish_date",
    "source_site",
    "source_file",
    "keyword",
    "city",
    "city_clean",
    "job_title",
    "company_name",
    "salary",
    "salary_avg",
    "experience",
    "education",
    "raw_tags",
    "skills",
    "job_url",
]

SKILL_PATTERNS: dict[str, tuple[str, ...]] = {
    "Python": (r"\bpython\b",),
    "Java": (r"\bjava\b",),
    "SQL": (r"\bsql\b",),
    "C++": (r"\bc\+\+\b", r"\bcpp\b"),
    "C": (r"(?<![a-z0-9])c(?![a-z0-9+#])",),
    "R": (r"(?<![a-z0-9])r(?![a-z0-9])",),
    "MATLAB": (r"\bmatlab\b",),
    "Excel": (r"\bexcel\b",),
    "Tableau": (r"\btableau\b",),
    "Power BI": (r"\bpower\s*bi\b",),
    "Hadoop": (r"\bhadoop\b",),
    "Spark": (r"\bspark\b",),
    "Linux": (r"\blinux\b",),
    "Docker": (r"\bdocker\b",),
    "Kubernetes": (r"\bkubernetes\b", r"\bk8s\b"),
    "Git": (r"\bgit\b",),
    "TensorFlow": (r"\btensorflow\b",),
    "PyTorch": (r"\bpytorch\b",),
    "NLP": (r"\bnlp\b", "自然语言处理"),
    "CV": (r"(?<![a-z0-9])cv(?![a-z0-9])", "计算机视觉"),
    "LLM": (r"\bllm\b",),
    "大模型": ("大模型",),
    "生成式AI": ("生成式ai", "aigc", "生成式人工智能"),
    "机器学习": ("机器学习", r"\bmachine learning\b"),
    "深度学习": ("深度学习", r"\bdeep learning\b"),
    "数据分析": ("数据分析",),
}


@dataclass(frozen=True)
class CleanRunResult:
    input_path: Path
    output_path: Path
    input_rows: int
    output_rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean raw job CSV into a structured dataset for downstream research."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--input",
        help="Path to a raw CSV file, for example project/data/raw/jobs_raw_xxx.csv",
    )
    source_group.add_argument(
        "--latest",
        action="store_true",
        help="Clean the latest raw CSV from project/data/raw/",
    )
    parser.add_argument(
        "--keyword",
        default="",
        help="Optional fallback keyword when raw rows do not contain a keyword.",
    )
    return parser.parse_args()


def resolve_input_path(
    *,
    input_path: str | Path | None = None,
    latest: bool = False,
) -> Path:
    if input_path:
        path = Path(input_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        return path

    if not latest:
        raise ValueError("Either input_path must be provided or latest=True must be used.")

    candidates = sorted(RAW_DIR.glob("jobs_raw_*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No raw CSV found in {RAW_DIR}")
    return candidates[0]


def build_output_path(output_path: str | Path | None = None) -> Path:
    if output_path:
        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return ensure_unique_path(path)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    return ensure_unique_path(CLEAN_DIR / f"jobs_clean_{timestamp}.csv")


def build_reclean_output_path(input_path: str | Path, output_dir: str | Path | None = None) -> Path:
    raw_path = Path(input_path).expanduser().resolve()
    target_dir = Path(output_dir).expanduser().resolve() if output_dir else CLEAN_V2_DIR.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    stem = raw_path.stem
    if stem.startswith("jobs_raw_"):
        base_name = f"jobs_clean_{stem[len('jobs_raw_'):]}_v2.csv"
    else:
        base_name = f"{stem}_clean_v2.csv"
    return ensure_unique_path(target_dir / base_name)


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    counter = 1
    while True:
        candidate = path.with_name(f"{path.stem}_{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def normalize_crawl_time(value: str) -> str:
    raw_value = (value or "").strip()
    if not raw_value:
        return datetime.now().isoformat(timespec="seconds")
    return raw_value


def extract_crawl_date(crawl_time: str) -> str:
    normalized = crawl_time.strip()
    if len(normalized) >= 10:
        return normalized[:10]
    return datetime.now().date().isoformat()


def extract_publish_date(publish_time: str, *, crawl_time: str) -> str:
    text = (publish_time or "").strip()
    if not text:
        return ""

    full_match = re.search(r"(\d{4})[./-年](\d{1,2})[./-月](\d{1,2})", text)
    if full_match:
        year, month, day = full_match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    month_day_match = re.search(r"(\d{1,2})[./-月](\d{1,2})", text)
    if month_day_match:
        crawl_date = extract_crawl_date(crawl_time)
        year = crawl_date[:4] if len(crawl_date) >= 4 else ""
        month, day = month_day_match.groups()
        if year:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    return ""


def load_raw_payload(raw_payload: str) -> dict[str, Any]:
    text = (raw_payload or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_publish_time(row: dict[str, str]) -> str:
    direct_candidates = (
        "publish_time_raw",
        "publish_time",
        "issueDateString",
        "publishTime",
        "issueDate",
        "issue_date",
    )
    for key in direct_candidates:
        value = (row.get(key) or "").strip()
        if value:
            return value

    payload = load_raw_payload(row.get("raw_payload", ""))
    payload_candidates = (
        "publish_time_raw",
        "publish_time",
        "publishTime",
        "issueDateString",
        "issueDate",
        "issue_date",
    )
    for key in payload_candidates:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value).strip()

    return ""


def normalize_city(raw_city: str) -> str:
    city = (raw_city or "").strip()
    if not city:
        return ""

    for separator in ("·", "-", "—", "/", "|", ",", "，", " "):
        if separator in city:
            city = city.split(separator, 1)[0].strip()

    municipalities = ("北京", "上海", "天津", "重庆")
    for name in municipalities:
        if city.startswith(name):
            return name

    if city.endswith("市"):
        city = city[:-1]

    return city.strip()


def infer_keyword(row: dict[str, str], *, source_file: str, fallback_keyword: str) -> str:
    keyword = (row.get("keyword") or "").strip()
    if keyword:
        return keyword
    if fallback_keyword:
        return fallback_keyword.strip()
    # TODO: source_file currently does not encode keyword information reliably.
    return ""


def extract_skills(job_title: str, raw_tags: str) -> str:
    text = f"{job_title}\n{raw_tags}".lower()
    matched: list[str] = []

    for canonical, patterns in SKILL_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                matched.append(canonical)
                break

    unique_skills = list(dict.fromkeys(matched))
    return ",".join(unique_skills)


def parse_salary_avg(raw_salary: str) -> str:
    text = (raw_salary or "").strip().lower().replace(" ", "")
    if not text:
        return ""

    period_multiplier = 1.0
    if "/天" in text or "元/天" in text:
        period_multiplier = 21.75
    elif "/小时" in text or "元/小时" in text:
        period_multiplier = 8 * 21.75
    elif "/年" in text or "元/年" in text or "万/年" in text:
        period_multiplier = 1 / 12

    range_match = re.search(r"(\d+(?:\.\d+)?)\s*[-~至]\s*(\d+(?:\.\d+)?)(万|千|k|元)?", text)
    single_match = re.search(r"(\d+(?:\.\d+)?)(万|千|k|元)?", text)

    if range_match:
        low = _convert_salary_number(float(range_match.group(1)), range_match.group(3), text)
        high = _convert_salary_number(float(range_match.group(2)), range_match.group(3), text)
        avg = (low + high) / 2
    elif single_match:
        avg = _convert_salary_number(float(single_match.group(1)), single_match.group(2), text)
    else:
        return ""

    avg *= period_multiplier
    if avg <= 0:
        return ""
    return str(int(round(avg)))


def _convert_salary_number(value: float, unit: str | None, context: str) -> float:
    if unit == "万":
        return value * 10000
    if unit in {"千", "k"}:
        return value * 1000
    if unit == "元":
        return value
    if "万" in context:
        return value * 10000
    if "k" in context:
        return value * 1000
    return value


def clean_rows(rows: Iterable[dict[str, str]], *, source_path: Path, fallback_keyword: str) -> list[dict[str, str]]:
    cleaned_rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for row in rows:
        job_title = (row.get("job_title") or row.get("job_name") or "").strip()
        job_url = (row.get("job_url") or row.get("original_url") or row.get("source_url") or "").strip()
        if not job_title or not job_url:
            continue
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        crawl_time = normalize_crawl_time(row.get("crawl_time", ""))
        publish_time = resolve_publish_time(row)
        source_file = (row.get("source_file") or source_path.name).strip()
        city = (row.get("city") or row.get("location") or "").strip()
        raw_tags = (row.get("raw_tags") or "").strip()
        salary = (row.get("salary") or row.get("salary_raw") or "").strip()
        experience = (row.get("experience") or "").strip() or "未知"
        education = (row.get("education") or "").strip() or "未知"

        cleaned_rows.append(
            {
                "crawl_time": crawl_time,
                "crawl_date": extract_crawl_date(crawl_time),
                "publish_time": publish_time,
                "publish_date": extract_publish_date(publish_time, crawl_time=crawl_time),
                "source_site": (row.get("source_site") or row.get("site") or row.get("archive_source") or "job51").strip(),
                "source_file": source_file,
                "keyword": infer_keyword(row, source_file=source_file, fallback_keyword=fallback_keyword),
                "city": city,
                "city_clean": normalize_city(city),
                "job_title": job_title,
                "company_name": (row.get("company_name") or "").strip(),
                "salary": salary,
                "salary_avg": parse_salary_avg(salary),
                "experience": experience,
                "education": education,
                "raw_tags": raw_tags,
                "skills": extract_skills(job_title, raw_tags),
                "job_url": job_url,
            }
        )

    return cleaned_rows


def write_clean_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def run_cleaning(
    *,
    input_path: str | Path | None = None,
    latest: bool = False,
    fallback_keyword: str = "",
    output_path: str | Path | None = None,
) -> CleanRunResult:
    resolved_input_path = resolve_input_path(input_path=input_path, latest=latest)
    resolved_output_path = build_output_path(output_path)

    rows = read_rows(resolved_input_path)
    cleaned_rows = clean_rows(
        rows,
        source_path=resolved_input_path,
        fallback_keyword=fallback_keyword,
    )
    write_clean_csv(resolved_output_path, cleaned_rows)

    return CleanRunResult(
        input_path=resolved_input_path,
        output_path=resolved_output_path,
        input_rows=len(rows),
        output_rows=len(cleaned_rows),
    )


def main() -> int:
    args = parse_args()
    result = run_cleaning(
        input_path=args.input,
        latest=args.latest,
        fallback_keyword=args.keyword,
    )

    print(f"Input raw file: {result.input_path}")
    print(f"Output clean file: {result.output_path}")
    print(f"Input rows: {result.input_rows}")
    print(f"Output rows: {result.output_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
