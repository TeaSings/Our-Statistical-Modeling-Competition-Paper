from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from adapters.base import AdapterConfigError, BlockedResponseError, CrawlerError
from adapters.job51 import BlockedException
from clean_data import CleanRunResult, run_cleaning
from run_batch import (
    BatchRunResult,
    load_seen_jobs,
    log_keyword_selection,
    resolve_keywords,
    run_batch_collection,
)


def parse_args() -> argparse.Namespace:
    raw_argv = sys.argv[1:]
    examples = """Examples:
  示例 1：抓取 AI 相关岗位
    python project/run_pipeline.py --site job51 --city 000000 --start-page 1 --pages 2 --keywords 人工智能 机器学习 深度学习 大模型 --sleep-between-keywords 10 --sleep-between-pages 4

  示例 2：抓取普通数字岗位
    python project/run_pipeline.py --site job51 --city 000000 --start-page 3 --pages 2 --keywords python java sql 数据分析 excel --sleep-between-keywords 10 --sleep-between-pages 4

  示例 3：抓取非技术岗位，观察是否出现 AI 相关技能
    python project/run_pipeline.py --site job51 --city 000000 --start-page 5 --pages 2 --keywords 产品经理 运营 市场营销 人力资源 财务 行政 --sleep-between-keywords 12 --sleep-between-pages 5

Suggested keyword groups:
  通用数字技能组: python java sql 数据分析 excel
  AI / 生成式AI 组: 人工智能 机器学习 深度学习 大模型 生成式AI NLP 计算机视觉
  工程技术组: 后端开发 算法工程师 数据开发 测试开发 运维开发
  商业 / 职能 / 非技术岗位组: 产品经理 运营 市场营销 销售 人力资源 财务 行政 客服 供应链 采购
  制造 / 传统行业岗位组: 机械工程师 电气工程师 生产管理 质量工程师 工艺工程师

Randomized sampling example:
  python project/run_pipeline.py --site job51 --city 000000 --random-start-page --max-start-page 8 --pages 2 --keywords python java 数据分析 --sleep-page-min 3 --sleep-page-max 7 --sleep-keyword-min 10 --sleep-keyword-max 15 --sample-jobs 5

Auto keyword example:
  python project/run_pipeline.py --site job51 --city 000000 --auto-keywords --sample-keywords 4 --random-start-page --max-start-page 8 --pages 2 --sleep-page-min 3 --sleep-page-max 7 --sleep-keyword-min 10 --sleep-keyword-max 15

Continuous multi-round example:
  python project/run_pipeline.py --site job51 --city 000000 --auto-keywords --sample-keywords 4 --random-start-page --max-start-page 8 --pages 2 --rounds 3 --sleep-between-rounds 1800 --sleep-page-min 3 --sleep-page-max 7 --sleep-keyword-min 10 --sleep-keyword-max 15
"""
    parser = argparse.ArgumentParser(
        description=(
            "Run the local crawl -> clean pipeline for research-ready job data. "
            "Use broad keyword sets so you can compare whether AI-related skills appear "
            "inside technical, business, and traditional occupation postings."
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
            "Multiple keywords to crawl sequentially. Use broader occupation categories, "
            "for example: python java sql 数据分析 excel 产品经理 运营 财务 行政."
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
        "--rounds",
        type=int,
        default=1,
        help="How many collection rounds to run sequentially. Default: 1",
    )
    parser.add_argument(
        "--sleep-between-rounds",
        type=float,
        default=1800.0,
        help="Sleep seconds between rounds when --rounds > 1. Default: 1800",
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Skip the cleaning phase and only export raw data.",
    )
    parser.add_argument(
        "--raw-output",
        default=None,
        help="Optional custom raw CSV output path. Multi-round runs append _roundN automatically.",
    )
    parser.add_argument(
        "--clean-output",
        default=None,
        help="Optional custom clean CSV output path. Multi-round runs append _roundN automatically.",
    )
    args = parser.parse_args()
    if not args.keywords and not args.auto_keywords:
        parser.error("Either provide --keywords or enable --auto-keywords.")
    if args.sample_keywords < 1:
        parser.error("--sample-keywords must be at least 1.")
    if args.rounds < 1:
        parser.error("--rounds must be at least 1.")
    if args.sleep_between_rounds < 0:
        parser.error("--sleep-between-rounds cannot be negative.")
    args.randomize_page_sleep = any(flag in raw_argv for flag in ("--sleep-page-min", "--sleep-page-max"))
    args.randomize_keyword_sleep = any(
        flag in raw_argv for flag in ("--sleep-keyword-min", "--sleep-keyword-max")
    )
    return args


def build_round_output_path(
    output_path: str | Path | None,
    *,
    round_index: int,
    total_rounds: int,
) -> str | None:
    if output_path is None:
        return None

    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if total_rounds <= 1:
        return str(path)
    return str(path.with_name(f"{path.stem}_round{round_index}{path.suffix}"))


def print_round_summary(
    *,
    round_index: int,
    total_rounds: int,
    keywords: list[str],
    batch_result: BatchRunResult,
    clean_path: Path | None,
    elapsed_seconds: float,
) -> None:
    print(f"Round {round_index}/{total_rounds} summary:")
    print(f"  keywords: {', '.join(keywords)}")
    print(f"  new_rows: {batch_result.total_rows}")
    print(f"  skipped_seen: {batch_result.total_skipped_seen}")
    print(f"  raw_file: {batch_result.raw_path}")
    print(f"  clean_file: {clean_path if clean_path else 'N/A'}")
    print(f"  seen_jobs_file: {batch_result.seen_jobs_path}")
    print(f"  elapsed_seconds: {elapsed_seconds:.2f}")


def print_pipeline_summary(
    *,
    site: str,
    city: str,
    pages: int,
    rounds: int,
    completed_rounds: int,
    all_keywords: list[list[str]],
    raw_paths: list[Path],
    clean_paths: list[Path],
    seen_jobs_path: Path,
    total_new_rows: int,
    total_skipped_seen: int,
    elapsed_seconds: float,
    success: bool,
) -> None:
    print("Pipeline summary:")
    print(f"  site: {site}")
    print(f"  city: {city}")
    print(f"  pages_per_keyword: {pages}")
    print(f"  rounds_completed: {completed_rounds}/{rounds}")
    for index, keywords in enumerate(all_keywords, start=1):
        print(f"  round_{index}_keywords: {', '.join(keywords)}")
    print(f"  total_new_rows: {total_new_rows}")
    print(f"  total_skipped_seen: {total_skipped_seen}")
    print(f"  raw_files: {', '.join(str(path) for path in raw_paths) if raw_paths else 'N/A'}")
    print(f"  clean_files: {', '.join(str(path) for path in clean_paths) if clean_paths else 'N/A'}")
    print(f"  seen_jobs_file: {seen_jobs_path}")
    print(f"  elapsed_seconds: {elapsed_seconds:.2f}")
    print(f"  success: {success}")


def main() -> int:
    args = parse_args()
    start_time = time.perf_counter()
    seen_urls, seen_jobs_path = load_seen_jobs()
    print(f"Incremental collection will use seen jobs file: {seen_jobs_path}")
    print(f"Loaded historical seen jobs: {len(seen_urls)}")

    completed_rounds = 0
    total_new_rows = 0
    total_skipped_seen = 0
    all_keywords: list[list[str]] = []
    raw_paths: list[Path] = []
    clean_paths: list[Path] = []

    try:
        for round_index in range(1, args.rounds + 1):
            round_start = time.perf_counter()
            print(f"=== Round {round_index}/{args.rounds} ===")

            selection = resolve_keywords(
                keywords=args.keywords,
                auto_keywords=args.auto_keywords,
                sample_keywords=args.sample_keywords,
            )
            all_keywords.append(list(selection.keywords))
            log_keyword_selection(selection)

            round_raw_output = build_round_output_path(
                args.raw_output,
                round_index=round_index,
                total_rounds=args.rounds,
            )
            round_clean_output = build_round_output_path(
                args.clean_output,
                round_index=round_index,
                total_rounds=args.rounds,
            )

            batch_result: BatchRunResult = run_batch_collection(
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
                seen_job_urls=seen_urls,
                seen_jobs_path=seen_jobs_path,
                raw_output=round_raw_output,
            )
            raw_paths.append(batch_result.raw_path)
            total_new_rows += batch_result.total_rows
            total_skipped_seen += batch_result.total_skipped_seen

            round_clean_path: Path | None = None
            if not args.skip_clean:
                clean_result: CleanRunResult = run_cleaning(
                    input_path=batch_result.raw_path,
                    fallback_keyword="",
                    output_path=round_clean_output,
                )
                round_clean_path = clean_result.output_path
                clean_paths.append(round_clean_path)

            completed_rounds = round_index
            round_elapsed = time.perf_counter() - round_start
            print_round_summary(
                round_index=round_index,
                total_rounds=args.rounds,
                keywords=selection.keywords,
                batch_result=batch_result,
                clean_path=round_clean_path,
                elapsed_seconds=round_elapsed,
            )

            if round_index < args.rounds:
                if args.sleep_between_rounds > 0:
                    print(
                        f"Sleeping {args.sleep_between_rounds:.1f}s before round "
                        f"{round_index + 1}/{args.rounds}"
                    )
                    time.sleep(args.sleep_between_rounds)
                else:
                    print(f"Sleeping 0.0s before round {round_index + 1}/{args.rounds}")

        elapsed = time.perf_counter() - start_time
        print_pipeline_summary(
            site=args.site,
            city=args.city,
            pages=args.pages,
            rounds=args.rounds,
            completed_rounds=completed_rounds,
            all_keywords=all_keywords,
            raw_paths=raw_paths,
            clean_paths=clean_paths,
            seen_jobs_path=seen_jobs_path,
            total_new_rows=total_new_rows,
            total_skipped_seen=total_skipped_seen,
            elapsed_seconds=elapsed,
            success=True,
        )
        return 0
    except (BlockedException, BlockedResponseError) as exc:
        elapsed = time.perf_counter() - start_time
        print(f"[{args.site}] blocked during pipeline: {exc}")
        print("Blocked by site. Please switch to Edge, handle verification manually, then rerun.")
        print_pipeline_summary(
            site=args.site,
            city=args.city,
            pages=args.pages,
            rounds=args.rounds,
            completed_rounds=completed_rounds,
            all_keywords=all_keywords,
            raw_paths=raw_paths,
            clean_paths=clean_paths,
            seen_jobs_path=seen_jobs_path,
            total_new_rows=total_new_rows,
            total_skipped_seen=total_skipped_seen,
            elapsed_seconds=elapsed,
            success=False,
        )
        return 2
    except (RuntimeError, AdapterConfigError, CrawlerError) as exc:
        elapsed = time.perf_counter() - start_time
        print(f"Pipeline failed during crawl stage: {exc}")
        print_pipeline_summary(
            site=args.site,
            city=args.city,
            pages=args.pages,
            rounds=args.rounds,
            completed_rounds=completed_rounds,
            all_keywords=all_keywords,
            raw_paths=raw_paths,
            clean_paths=clean_paths,
            seen_jobs_path=seen_jobs_path,
            total_new_rows=total_new_rows,
            total_skipped_seen=total_skipped_seen,
            elapsed_seconds=elapsed,
            success=False,
        )
        return 4
    except Exception as exc:
        elapsed = time.perf_counter() - start_time
        print(f"Pipeline failed during clean stage: {exc}")
        print("Raw file has been kept for manual inspection.")
        print_pipeline_summary(
            site=args.site,
            city=args.city,
            pages=args.pages,
            rounds=args.rounds,
            completed_rounds=completed_rounds,
            all_keywords=all_keywords,
            raw_paths=raw_paths,
            clean_paths=clean_paths,
            seen_jobs_path=seen_jobs_path,
            total_new_rows=total_new_rows,
            total_skipped_seen=total_skipped_seen,
            elapsed_seconds=elapsed,
            success=False,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
