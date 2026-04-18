from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from adapters.base import AdapterConfigError, BaseAdapter, BlockedResponseError, CrawlerError, JobRecord
from adapters.job51 import BlockedException
from browser_session import get_cookies
from config import BROWSER_CONFIG, STORAGE_CONFIG, get_site_config
from main import build_adapter
from storage import JobStorage


PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
FINAL_DIR = PROJECT_DIR / "data" / "final"
SEEN_JOBS_PATH = FINAL_DIR / "seen_jobs.txt"
RAW_HEADERS = [
    "crawl_time",
    "crawl_date",
    "source_site",
    "source_file",
    "keyword",
    "city",
    "job_title",
    "company_name",
    "salary",
    "experience",
    "education",
    "publish_time",
    "raw_tags",
    "job_url",
    "page",
    "raw_payload",
]

# Built-in keyword pool for research on AI/data skill diffusion across occupations.
# The pool is intentionally diverse so auto-sampling does not over-concentrate on
# Python / AI / pure technical roles, and can be used to observe whether AI-related
# skills begin to appear in non-technical and traditional jobs.
LEGACY_KEYWORD_GROUPS: dict[str, list[str]] = {
    "AI / 数字技术类": [
        "人工智能",
        "机器学习",
        "深度学习",
        "大模型",
        "NLP",
        "计算机视觉",
        "python",
        "java",
        "sql",
        "数据分析",
    ],
    "工程技术类": [
        "后端开发",
        "算法工程师",
        "数据开发",
        "测试开发",
        "运维开发",
        "机械工程师",
        "电气工程师",
        "工艺工程师",
        "质量工程师",
        "生产管理",
    ],
    "产品 / 运营 / 商业类": [
        "产品经理",
        "运营",
        "商业分析",
        "数据挖掘",
        "市场营销",
        "销售",
        "客服",
    ],
    "职能支持类": [
        "人力资源",
        "财务",
        "行政",
        "采购",
        "供应链",
    ],
}


KEYWORD_GROUPS: dict[str, list[str]] = {
    "\u667a\u80fd\u4e0e\u6570\u5b57\u6280\u672f": [
        "\u4eba\u5de5\u667a\u80fd",
        "\u673a\u5668\u5b66\u4e60",
        "\u6df1\u5ea6\u5b66\u4e60",
        "\u5927\u6a21\u578b",
        "AIGC",
        "RAG",
        "NLP",
        "\u8ba1\u7b97\u673a\u89c6\u89c9",
        "Python",
        "SQL",
        "\u6570\u636e\u5206\u6790",
        "\u7b97\u6cd5\u5de5\u7a0b\u5e08",
    ],
    "\u901a\u7528\u6280\u672f\u5c97": [
        "\u524d\u7aef",
        "\u540e\u7aef",
        "\u6d4b\u8bd5",
        "\u8fd0\u7ef4",
        "\u4ea7\u54c1\u7ecf\u7406",
        "\u6570\u636e\u5206\u6790",
        "\u5f00\u53d1\u5de5\u7a0b\u5e08",
        "\u540e\u7aef\u5f00\u53d1",
        "\u6d4b\u8bd5\u5f00\u53d1",
        "\u81ea\u52a8\u5316\u5de5\u7a0b\u5e08",
    ],
    "\u533b\u7597\u5065\u5eb7": [
        "\u533b\u751f",
        "\u62a4\u58eb",
        "\u836f\u5e08",
        "\u533b\u5b66\u68c0\u9a8c",
        "\u5eb7\u590d\u6cbb\u7597",
        "\u533b\u7597\u5668\u68b0",
        "\u4e34\u5e8a\u533b\u751f",
        "\u53e3\u8154\u533b\u751f",
        "\u8425\u517b\u5e08",
        "\u5065\u5eb7\u7ba1\u7406",
    ],
    "\u6559\u80b2\u57f9\u8bad": [
        "\u6559\u5e08",
        "\u73ed\u4e3b\u4efb",
        "\u6559\u7814",
        "\u8bfe\u7a0b\u987e\u95ee",
        "\u57f9\u8bad\u5e08",
        "\u5b66\u79d1\u6559\u5e08",
        "\u6559\u80b2\u54a8\u8be2",
        "\u8f85\u5bfc\u5458",
        "\u5e7c\u6559",
        "\u7d20\u8d28\u6559\u80b2",
    ],
    "\u91d1\u878d\u8d22\u4f1a": [
        "\u4f1a\u8ba1",
        "\u5ba1\u8ba1",
        "\u51fa\u7eb3",
        "\u91d1\u878d\u5206\u6790",
        "\u98ce\u63a7",
        "\u6295\u8d44\u987e\u95ee",
        "\u8d22\u52a1\u5206\u6790",
        "\u8bc1\u5238\u987e\u95ee",
        "\u57fa\u91d1\u9500\u552e",
        "\u7a0e\u52a1",
    ],
    "\u8fd0\u8425\u4e0e\u5185\u5bb9": [
        "\u8fd0\u8425",
        "\u65b0\u5a92\u4f53\u8fd0\u8425",
        "\u5185\u5bb9\u8fd0\u8425",
        "\u7535\u5546\u8fd0\u8425",
        "\u7528\u6237\u8fd0\u8425",
        "\u4ea7\u54c1\u8fd0\u8425",
        "\u793e\u7fa4\u8fd0\u8425",
        "\u6d3b\u52a8\u8fd0\u8425",
        "\u76f4\u64ad\u8fd0\u8425",
        "\u5546\u5bb6\u8fd0\u8425",
    ],
    "\u5e02\u573a\u4e0e\u9500\u552e": [
        "\u9500\u552e",
        "\u6e20\u9053\u9500\u552e",
        "\u5ba2\u6237\u7ecf\u7406",
        "\u5e02\u573a\u4e13\u5458",
        "\u5546\u52a1\u62d3\u5c55",
        "\u7535\u8bdd\u9500\u552e",
        "\u533a\u57df\u9500\u552e",
        "\u5927\u5ba2\u6237\u9500\u552e",
        "\u54c1\u724c\u8425\u9500",
        "\u5e02\u573a\u63a8\u5e7f",
    ],
    "\u8bbe\u8ba1\u4e0e\u521b\u610f": [
        "\u5e73\u9762\u8bbe\u8ba1",
        "UI\u8bbe\u8ba1",
        "\u5de5\u4e1a\u8bbe\u8ba1",
        "\u89c6\u89c9\u8bbe\u8ba1",
        "\u89c6\u9891\u526a\u8f91",
        "\u4ea4\u4e92\u8bbe\u8ba1",
        "\u5305\u88c5\u8bbe\u8ba1",
        "\u4e09\u7ef4\u8bbe\u8ba1",
        "\u5e7f\u544a\u8bbe\u8ba1",
        "\u6e32\u67d3\u8bbe\u8ba1",
    ],
    "\u5236\u9020\u4e0e\u5de5\u7a0b": [
        "\u673a\u68b0\u5de5\u7a0b\u5e08",
        "\u7535\u6c14\u5de5\u7a0b\u5e08",
        "\u5de5\u827a\u5de5\u7a0b\u5e08",
        "\u8d28\u91cf\u5de5\u7a0b\u5e08",
        "\u81ea\u52a8\u5316\u5de5\u7a0b\u5e08",
        "\u751f\u4ea7\u7ba1\u7406",
        "\u8bbe\u5907\u5de5\u7a0b\u5e08",
        "\u7ed3\u6784\u5de5\u7a0b\u5e08",
        "\u8f66\u95f4\u4e3b\u7ba1",
        "\u5de5\u7a0b\u9879\u76ee\u7ecf\u7406",
    ],
    "\u7269\u6d41\u4e0e\u4f9b\u5e94\u94fe": [
        "\u91c7\u8d2d",
        "\u4f9b\u5e94\u94fe",
        "\u4ed3\u50a8",
        "\u7269\u6d41\u4e13\u5458",
        "\u8ba1\u5212\u8c03\u5ea6",
        "\u7269\u6599\u7ba1\u7406",
        "\u91c7\u8d2d\u4e13\u5458",
        "\u4f9b\u5e94\u5546\u7ba1\u7406",
        "\u8fd0\u8f93\u8c03\u5ea6",
        "\u5e93\u7ba1",
    ],
    "\u884c\u653f\u4eba\u4e8b\u6cd5\u52a1": [
        "\u4eba\u4e8b",
        "\u62db\u8058\u4e13\u5458",
        "\u884c\u653f",
        "\u6cd5\u52a1",
        "\u5408\u89c4",
        "\u4eba\u529b\u8d44\u6e90",
        "HRBP",
        "\u85aa\u916c\u7ee9\u6548",
        "\u884c\u653f\u4e13\u5458",
        "\u52b3\u52a8\u5173\u7cfb",
    ],
    "\u670d\u52a1\u4e0e\u6d88\u8d39\u884c\u4e1a": [
        "\u5ba2\u670d",
        "\u9152\u5e97\u7ba1\u7406",
        "\u9910\u996e\u7ba1\u7406",
        "\u5bfc\u8d2d",
        "\u95e8\u5e97\u5e97\u957f",
        "\u524d\u53f0",
        "\u8c03\u8336\u5e08",
        "\u9910\u996e\u670d\u52a1\u5458",
        "\u65c5\u6e38\u987e\u95ee",
        "\u5ba2\u6237\u670d\u52a1",
    ],
}


@dataclass(frozen=True)
class BatchRunResult:
    site: str
    city: str
    start_page: int
    pages: int
    keywords: list[str]
    raw_path: Path
    total_rows: int
    total_saved: int
    total_skipped_seen: int
    seen_jobs_path: Path


@dataclass(frozen=True)
class KeywordSelection:
    keywords: list[str]
    categories: dict[str, str]
    auto_selected: bool


def parse_args() -> argparse.Namespace:
    raw_argv = sys.argv[1:]
    examples = """Examples:
  AI / 生成式AI:
    python project/run_batch.py --site job51 --city 000000 --start-page 1 --pages 2 --keywords 人工智能 机器学习 深度学习 大模型 --sleep-between-keywords 10 --sleep-between-pages 4

  通用数字技能:
    python project/run_batch.py --site job51 --city 000000 --start-page 3 --pages 2 --keywords python java sql 数据分析 excel --sleep-between-keywords 10 --sleep-between-pages 4

  非技术岗位:
    python project/run_batch.py --site job51 --city 000000 --start-page 5 --pages 2 --keywords 产品经理 运营 市场营销 人力资源 财务 行政 --sleep-between-keywords 12 --sleep-between-pages 5

Suggested keyword groups:
  通用数字技能: python java sql 数据分析 excel
  AI / 生成式AI: 人工智能 机器学习 深度学习 大模型 生成式AI NLP 计算机视觉
  工程技术: 后端开发 算法工程师 数据开发 测试开发 运维开发
  商业/职能/非技术: 产品经理 运营 市场营销 销售 人力资源 财务 行政 客服 供应链 采购
  制造/传统行业: 机械工程师 电气工程师 生产管理 质量工程师 工艺工程师

Randomized sampling example:
  python project/run_batch.py --site job51 --city 000000 --random-start-page --max-start-page 8 --pages 2 --keywords python java 数据分析 --sleep-page-min 3 --sleep-page-max 7 --sleep-keyword-min 10 --sleep-keyword-max 15 --sample-jobs 5

Auto keyword example:
  python project/run_batch.py --site job51 --city 000000 --auto-keywords --sample-keywords 4 --random-start-page --max-start-page 8 --pages 2 --sleep-page-min 3 --sleep-page-max 7 --sleep-keyword-min 10 --sleep-keyword-max 15
"""
    parser = argparse.ArgumentParser(
        description=(
            "Batch crawl job list pages and export raw CSV for downstream cleaning. "
            "Designed for broader occupation coverage so you can observe whether AI-related "
            "skills begin to appear across technical and non-technical roles."
        ),
        epilog=examples,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--site",
        choices=["job51"],
        default="job51",
        help="Which site adapter to use. Currently only job51 is supported.",
    )
    parser.add_argument(
        "--city",
        required=True,
        help="City code used by the list API, for example 000000.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="How many pages to crawl for each keyword.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="First result page to crawl. Example: --start-page 4 --pages 2 crawls pages 4 and 5.",
    )
    parser.add_argument(
        "--random-start-page",
        action="store_true",
        help="Choose a random start page for each keyword instead of using --start-page.",
    )
    parser.add_argument(
        "--max-start-page",
        type=int,
        default=8,
        help="Upper bound used with --random-start-page. Default: 8",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=None,
        help=(
            "Multiple keywords to crawl sequentially. Use broad occupation groups, not only AI roles, "
            "for example: python java sql 数据分析 excel 产品经理 运营 财务."
        ),
    )
    parser.add_argument(
        "--auto-keywords",
        action="store_true",
        help="Automatically sample keywords from the built-in multi-category occupation pool.",
    )
    parser.add_argument(
        "--sample-keywords",
        type=int,
        default=4,
        help="How many keywords to sample when --auto-keywords is enabled. Default: 4",
    )
    parser.add_argument(
        "--sleep-between-keywords",
        type=float,
        default=10.0,
        help="Sleep seconds between keywords. Default: 10",
    )
    parser.add_argument(
        "--sleep-between-pages",
        type=float,
        default=4.0,
        help="Sleep seconds before each page request within the same keyword. Default: 4",
    )
    parser.add_argument(
        "--sleep-page-min",
        type=float,
        default=3.0,
        help="Minimum randomized sleep seconds before each page when random page sleep is enabled. Default: 3",
    )
    parser.add_argument(
        "--sleep-page-max",
        type=float,
        default=7.0,
        help="Maximum randomized sleep seconds before each page when random page sleep is enabled. Default: 7",
    )
    parser.add_argument(
        "--sleep-keyword-min",
        type=float,
        default=8.0,
        help="Minimum randomized sleep seconds between keywords when random keyword sleep is enabled. Default: 8",
    )
    parser.add_argument(
        "--sleep-keyword-max",
        type=float,
        default=15.0,
        help="Maximum randomized sleep seconds between keywords when random keyword sleep is enabled. Default: 15",
    )
    parser.add_argument(
        "--sample-jobs",
        type=int,
        default=None,
        help="Optional random sample size per page after parsing jobs, for example --sample-jobs 5.",
    )
    parser.add_argument(
        "--cdp-url",
        default=BROWSER_CONFIG.cdp_url,
        help="Edge remote debugging endpoint.",
    )
    parser.add_argument(
        "--list-url-template",
        default=None,
        help="Unused for job51, kept for compatibility with the shared adapter builder.",
    )
    args = parser.parse_args()
    if not args.keywords and not args.auto_keywords:
        parser.error("Either provide --keywords or enable --auto-keywords.")
    if args.sample_keywords < 1:
        parser.error("--sample-keywords must be at least 1.")
    args.randomize_page_sleep = any(flag in raw_argv for flag in ("--sleep-page-min", "--sleep-page-max"))
    args.randomize_keyword_sleep = any(
        flag in raw_argv for flag in ("--sleep-keyword-min", "--sleep-keyword-max")
    )
    return args


def build_raw_output_path(raw_output: str | Path | None = None) -> Path:
    if raw_output is not None:
        output_path = Path(raw_output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return ensure_unique_path(output_path)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    return ensure_unique_path(RAW_DIR / f"jobs_raw_{timestamp}.csv")


def init_raw_csv(path: Path) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RAW_HEADERS)
        writer.writeheader()


def append_raw_rows(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RAW_HEADERS)
        writer.writerows(rows)


def build_raw_rows(
    *,
    jobs: list[dict[str, str]],
    keyword: str,
    site: str,
    source_file: str,
    page: int,
) -> list[dict[str, str]]:
    crawl_dt = datetime.now()
    crawl_time = crawl_dt.isoformat(timespec="seconds")
    crawl_date = crawl_dt.date().isoformat()
    rows: list[dict[str, str]] = []

    for job in jobs:
        rows.append(
            {
                "crawl_time": crawl_time,
                "crawl_date": crawl_date,
                "source_site": site,
                "source_file": source_file,
                "keyword": keyword,
                "city": job.get("location", ""),
                "job_title": job.get("job_name", ""),
                "company_name": job.get("company_name", ""),
                "salary": job.get("salary", ""),
                "experience": job.get("experience", ""),
                "education": job.get("education", ""),
                "publish_time": job.get("publish_time", ""),
                "raw_tags": job.get("raw_tags", ""),
                "job_url": job.get("job_url", ""),
                "page": str(page),
                "raw_payload": job.get("raw_payload", ""),
            }
        )

    return rows


def build_job_records(*, jobs: list[dict[str, str]], site: str, page: int, base_url: str) -> list[JobRecord]:
    records: list[JobRecord] = []
    for job in jobs:
        job_url = job.get("job_url", "")
        job_title = job.get("job_name", "")
        if not job_url or not job_title:
            continue
        records.append(
            JobRecord(
                site=site,
                page=page,
                job_url=urljoin(base_url, job_url),
                job_title=job_title,
                company_name=job.get("company_name", ""),
                city=job.get("location", ""),
                salary=job.get("salary", ""),
                summary="",
                raw_payload=job.get("raw_payload", ""),
            )
        )
    return records


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    counter = 1
    while True:
        candidate = path.with_name(f"{path.stem}_{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def resolve_seen_jobs_path(seen_jobs_path: str | Path | None = None) -> Path:
    if seen_jobs_path is not None:
        path = Path(seen_jobs_path).expanduser().resolve()
    else:
        path = SEEN_JOBS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def seed_seen_jobs_from_storage(seen_jobs_path: Path) -> set[str]:
    storage_urls: set[str] = set()
    storage_csv = STORAGE_CONFIG.csv_path
    if storage_csv.exists():
        with storage_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                job_url = (row.get("job_url") or "").strip()
                if job_url:
                    storage_urls.add(job_url)

    if storage_urls and not seen_jobs_path.exists():
        append_seen_jobs(seen_jobs_path, storage_urls)

    return storage_urls


def load_seen_jobs(seen_jobs_path: str | Path | None = None) -> tuple[set[str], Path]:
    resolved_path = resolve_seen_jobs_path(seen_jobs_path)
    seen_urls: set[str] = set()

    if resolved_path.exists():
        with resolved_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                job_url = line.strip()
                if job_url:
                    seen_urls.add(job_url)
    else:
        seen_urls.update(seed_seen_jobs_from_storage(resolved_path))

    return seen_urls, resolved_path


def append_seen_jobs(seen_jobs_path: Path, job_urls: set[str] | list[str]) -> None:
    if not job_urls:
        return
    with seen_jobs_path.open("a", encoding="utf-8", newline="") as handle:
        for job_url in job_urls:
            text = str(job_url).strip()
            if text:
                handle.write(f"{text}\n")


def filter_new_jobs(
    *,
    jobs: list[dict[str, str]],
    seen_urls: set[str],
    sample_jobs: int | None = None,
) -> tuple[list[dict[str, str]], int]:
    unseen_candidates: list[dict[str, str]] = []
    skipped_seen = 0
    page_seen: set[str] = set()

    for job in jobs:
        job_url = (job.get("job_url") or "").strip()
        if job_url and (job_url in seen_urls or job_url in page_seen):
            skipped_seen += 1
            continue
        if job_url:
            page_seen.add(job_url)
        unseen_candidates.append(job)

    if sample_jobs is not None and len(unseen_candidates) > sample_jobs:
        return random.sample(unseen_candidates, sample_jobs), skipped_seen

    return unseen_candidates, skipped_seen


def compute_sleep_duration(*, fixed: float, randomize: bool, minimum: float, maximum: float) -> float:
    if randomize:
        return random.uniform(minimum, maximum)
    return fixed


def prompt_for_manual_verification(*, site_display_name: str, keyword: str, page: int) -> None:
    print(f"[{site_display_name}] keyword={keyword} page={page} blocked.")
    print("Blocked by site. Please switch to Edge and complete the verification manually.")
    input("After you finish verification in Edge, press ENTER to continue...")


def infer_keyword_category(keyword: str) -> str:
    for category, pool in KEYWORD_GROUPS.items():
        if keyword in pool:
            return category
    return "手动关键词"


def sample_diverse_keywords(sample_keywords: int) -> KeywordSelection:
    categories = list(KEYWORD_GROUPS.keys())
    random.shuffle(categories)

    target_size = min(sample_keywords, len({keyword for pool in KEYWORD_GROUPS.values() for keyword in pool}))
    chosen_keywords: list[str] = []
    chosen_categories: dict[str, str] = {}

    primary_categories = categories[: min(target_size, len(categories))]
    for category in primary_categories:
        keyword = random.choice(KEYWORD_GROUPS[category])
        if keyword not in chosen_categories:
            chosen_keywords.append(keyword)
            chosen_categories[keyword] = category

    if len(chosen_keywords) < target_size:
        remaining: list[tuple[str, str]] = []
        for category, pool in KEYWORD_GROUPS.items():
            for keyword in pool:
                if keyword not in chosen_categories:
                    remaining.append((keyword, category))
        random.shuffle(remaining)
        for keyword, category in remaining:
            if len(chosen_keywords) >= target_size:
                break
            chosen_keywords.append(keyword)
            chosen_categories[keyword] = category

    return KeywordSelection(
        keywords=chosen_keywords,
        categories=chosen_categories,
        auto_selected=True,
    )


def resolve_keywords(*, keywords: list[str] | None, auto_keywords: bool, sample_keywords: int) -> KeywordSelection:
    if keywords:
        unique_keywords = list(dict.fromkeys(keywords))
        return KeywordSelection(
            keywords=unique_keywords,
            categories={keyword: infer_keyword_category(keyword) for keyword in unique_keywords},
            auto_selected=False,
        )
    if auto_keywords:
        return sample_diverse_keywords(sample_keywords)
    raise ValueError("Either provide keywords or enable auto_keywords.")


def log_keyword_selection(selection: KeywordSelection) -> None:
    mode = "auto" if selection.auto_selected else "manual"
    print(f"Keyword selection mode: {mode}")
    for keyword in selection.keywords:
        category = selection.categories.get(keyword, "未分类")
        print(f"  - {keyword} [{category}]")


def run_batch_collection(
    *,
    site: str,
    city: str,
    pages: int,
    start_page: int = 1,
    random_start_page: bool = False,
    max_start_page: int = 8,
    keywords: list[str],
    sleep_between_keywords: float = 10.0,
    sleep_between_pages: float = 4.0,
    sleep_page_min: float = 3.0,
    sleep_page_max: float = 7.0,
    sleep_keyword_min: float = 8.0,
    sleep_keyword_max: float = 15.0,
    randomize_page_sleep: bool = False,
    randomize_keyword_sleep: bool = False,
    sample_jobs: int | None = None,
    seen_job_urls: set[str] | None = None,
    seen_jobs_path: str | Path | None = None,
    cdp_url: str = BROWSER_CONFIG.cdp_url,
    list_url_template: str | None = None,
    raw_output: str | Path | None = None,
) -> BatchRunResult:
    if start_page < 1:
        raise ValueError("start_page must be at least 1.")
    if max_start_page < 1:
        raise ValueError("max_start_page must be at least 1.")
    if pages < 1:
        raise ValueError("pages must be at least 1.")
    if sample_jobs is not None and sample_jobs < 1:
        raise ValueError("sample_jobs must be at least 1 when provided.")
    if sleep_page_min > sleep_page_max:
        raise ValueError("sleep_page_min cannot be greater than sleep_page_max.")
    if sleep_keyword_min > sleep_keyword_max:
        raise ValueError("sleep_keyword_min cannot be greater than sleep_keyword_max.")

    site_config = get_site_config(site)
    raw_path = build_raw_output_path(raw_output)
    seen_urls, resolved_seen_jobs_path = (
        (seen_job_urls, resolve_seen_jobs_path(seen_jobs_path))
        if seen_job_urls is not None
        else load_seen_jobs(seen_jobs_path)
    )
    init_raw_csv(raw_path)
    print(
        f"[{site_config.display_name}] loaded seen jobs={len(seen_urls)} "
        f"from {resolved_seen_jobs_path}"
    )

    storage = JobStorage(STORAGE_CONFIG)

    total_rows = 0
    total_saved = 0
    total_skipped_seen = 0

    def create_adapter() -> BaseAdapter:
        fresh_cookies = get_cookies(
            cdp_url=cdp_url,
            target_url=site_config.cookie_url,
        )
        return build_adapter(
            site,
            list_url_template=list_url_template,
            cookies=fresh_cookies,
        )

    adapter = create_adapter()
    try:
        for index, keyword in enumerate(keywords, start=1):
            keyword_start_page = (
                random.randint(1, max_start_page) if random_start_page else start_page
            )
            print(
                f"[{site_config.display_name}] keyword {index}/{len(keywords)} start: "
                f"keyword={keyword} city={city} start_page={keyword_start_page} pages={pages}"
            )
            keyword_rows = 0

            for offset in range(pages):
                page = keyword_start_page + offset
                page_sleep = compute_sleep_duration(
                    fixed=sleep_between_pages,
                    randomize=randomize_page_sleep,
                    minimum=sleep_page_min,
                    maximum=sleep_page_max,
                )
                if page_sleep > 0:
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page={page} sleep {page_sleep:.1f}s before next page"
                    )
                    time.sleep(page_sleep)
                else:
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page={page} sleep 0.0s before next page"
                    )
                while True:
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page={page} fetching"
                    )
                    try:
                        response = adapter.fetch_page(page=page, keyword=keyword, city=city)
                        jobs = adapter.parse_jobs(response)
                        break
                    except (BlockedException, BlockedResponseError):
                        prompt_for_manual_verification(
                            site_display_name=site_config.display_name,
                            keyword=keyword,
                            page=page,
                        )
                        adapter.close()
                        adapter = create_adapter()
                        print(
                            f"[{site_config.display_name}] keyword={keyword} "
                            f"page={page} retrying after manual verification"
                        )
                original_count = len(jobs)
                jobs, skipped_seen = filter_new_jobs(
                    jobs=jobs,
                    seen_urls=seen_urls,
                    sample_jobs=sample_jobs,
                )
                total_skipped_seen += skipped_seen
                if sample_jobs is not None and original_count:
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page={page} sampled_jobs={len(jobs)}/{max(original_count - skipped_seen, 0)} "
                        f"after skipping_seen={skipped_seen}"
                    )
                elif skipped_seen:
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page={page} skipped_seen={skipped_seen}"
                    )
                raw_rows = build_raw_rows(
                    jobs=jobs,
                    keyword=keyword,
                    site=site,
                    source_file=raw_path.name,
                    page=page,
                )
                append_raw_rows(raw_path, raw_rows)

                job_records = build_job_records(
                    jobs=jobs,
                    site=site,
                    page=page,
                    base_url=site_config.base_url,
                )
                saved_count = storage.save_jobs(job_records)
                new_urls = {
                    (job.get("job_url") or "").strip()
                    for job in jobs
                    if (job.get("job_url") or "").strip()
                }
                if new_urls:
                    seen_urls.update(new_urls)
                    append_seen_jobs(resolved_seen_jobs_path, sorted(new_urls))

                keyword_rows += len(raw_rows)
                total_rows += len(raw_rows)
                total_saved += saved_count
                print(
                    f"[{site_config.display_name}] keyword={keyword} "
                    f"page={page} fetched_rows={len(raw_rows)} "
                    f"saved_to_storage={saved_count} skipped_seen={skipped_seen}"
                )

            print(
                f"[{site_config.display_name}] keyword={keyword} completed "
                f"start_page={keyword_start_page} pages={pages} rows={keyword_rows} raw_file={raw_path}"
            )

            keyword_sleep = compute_sleep_duration(
                fixed=sleep_between_keywords,
                randomize=randomize_keyword_sleep,
                minimum=sleep_keyword_min,
                maximum=sleep_keyword_max,
            )
            if index < len(keywords) and keyword_sleep > 0:
                print(
                    f"[{site_config.display_name}] keyword switch sleep "
                    f"{keyword_sleep:.1f}s before next keyword"
                )
                time.sleep(keyword_sleep)
            elif index < len(keywords):
                print(f"[{site_config.display_name}] keyword switch sleep 0.0s before next keyword")
    finally:
        adapter.close()

    print(
        f"[{site_config.display_name}] batch completed keywords={len(keywords)} "
        f"start_page={start_page} pages={pages} total_rows={total_rows} "
        f"storage_new_saved={total_saved} skipped_seen={total_skipped_seen} raw_file={raw_path}"
    )
    return BatchRunResult(
        site=site,
        city=city,
        start_page=start_page,
        pages=pages,
        keywords=list(keywords),
        raw_path=raw_path,
        total_rows=total_rows,
        total_saved=total_saved,
        total_skipped_seen=total_skipped_seen,
        seen_jobs_path=resolved_seen_jobs_path,
    )


def main() -> int:
    args = parse_args()
    raw_path = build_raw_output_path()
    selection = resolve_keywords(
        keywords=args.keywords,
        auto_keywords=args.auto_keywords,
        sample_keywords=args.sample_keywords,
    )
    log_keyword_selection(selection)
    try:
        run_batch_collection(
            site=args.site,
            city=args.city,
            pages=args.pages,
            start_page=args.start_page,
            random_start_page=args.random_start_page,
            max_start_page=args.max_start_page,
            keywords=selection.keywords,
            sleep_between_keywords=args.sleep_between_keywords,
            sleep_between_pages=args.sleep_between_pages,
            sleep_page_min=args.sleep_page_min,
            sleep_page_max=args.sleep_page_max,
            sleep_keyword_min=args.sleep_keyword_min,
            sleep_keyword_max=args.sleep_keyword_max,
            randomize_page_sleep=args.randomize_page_sleep,
            randomize_keyword_sleep=args.randomize_keyword_sleep,
            sample_jobs=args.sample_jobs,
            cdp_url=args.cdp_url,
            list_url_template=args.list_url_template,
            raw_output=raw_path,
        )
    except (BlockedException, BlockedResponseError) as exc:
        site_config = get_site_config(args.site)
        print(
            f"[{site_config.display_name}] blocked while crawling: {exc}"
        )
        print("Blocked by site. Please switch to Edge, handle verification manually, then rerun.")
        print(f"Partial raw data has been written to: {raw_path}")
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Runtime error: {exc}")
        raise SystemExit(5)
    except AdapterConfigError as exc:  # type: ignore[name-defined]
        print(f"Config error: {exc}")
        raise SystemExit(3)
    except CrawlerError as exc:
        print(f"Crawler error: {exc}")
        raise SystemExit(4)
