from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from clean_jobs import clean_jsonl_to_csv  # noqa: E402
from common import ROOT_DIR, clean_text, configure_utf8_stdio, ensure_parent  # noqa: E402
from platforms.job51.fetch_social_jobs import (  # noqa: E402
    CAP_THRESHOLD,
    PAGE_SIZE,
    FunctionCode,
    build_client,
    close_client,
    crawl_scope,
    refresh_taxonomies,
    select_function_codes,
    select_root_children,
)


DEFAULT_CURSOR_FILE = "data/raw/51job/manifests/51job_social_cursor.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a stable, sequential 51job social crawl across functions and areas",
    )
    parser.add_argument(
        "--output-raw",
        default="data/raw/51job/records/51job_social_jobs_raw.jsonl",
        help="Output JSONL with normalized 51job social job rows",
    )
    parser.add_argument(
        "--partition-manifest",
        default="data/raw/51job/manifests/51job_social_partition_manifest.jsonl",
        help="Partition planning manifest JSONL",
    )
    parser.add_argument(
        "--progress-file",
        default="data/raw/51job/manifests/51job_social_progress.json",
        help="Per-batch progress snapshot JSON file",
    )
    parser.add_argument(
        "--cursor-file",
        default=DEFAULT_CURSOR_FILE,
        help="Sequential scheduler cursor JSON file",
    )
    parser.add_argument(
        "--clean-output",
        default="data/processed/51job/51job_social_jobs_clean.csv",
        help="Optional clean CSV refreshed after each batch when --refresh-clean is enabled",
    )
    parser.add_argument(
        "--refresh-clean",
        action="store_true",
        help="Refresh the clean CSV after each completed batch",
    )
    parser.add_argument(
        "--function-file",
        default="data/input/51job/51job_search_function_codes.json",
        help="Cached function code JSON extracted from the live search bundle",
    )
    parser.add_argument(
        "--area-file",
        default="data/input/51job/51job_search_area_tree.json",
        help="Cached area tree JSON extracted from the live search bundle",
    )
    parser.add_argument(
        "--specific-only",
        action="store_true",
        help="Only keep more specific function codes and skip broad group-style codes",
    )
    parser.add_argument(
        "--function-code",
        default="",
        help="Only crawl one specific 51job function code",
    )
    parser.add_argument(
        "--max-functions",
        type=int,
        default=0,
        help="Only keep the first N selected function codes when > 0",
    )
    parser.add_argument(
        "--job-area",
        default="",
        help="Optional explicit jobArea code to use as the only sequential area batch",
    )
    parser.add_argument(
        "--top-level-area-offset",
        type=int,
        default=0,
        help="Skip the first N top-level areas in nationwide order",
    )
    parser.add_argument(
        "--top-level-area-limit",
        type=int,
        default=0,
        help="Only keep the next N top-level areas in nationwide order",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Only process the next N sequential batches in this run; 0 means keep going until finished",
    )
    parser.add_argument(
        "--reset-cursor",
        action="store_true",
        help="Ignore any existing cursor file and rebuild the sequential queue from scratch",
    )
    parser.add_argument(
        "--batch-retries",
        type=int,
        default=1,
        help="How many times to rebuild the client and retry the same batch after a fatal batch-level error",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Concurrent workers for page fetching; default 1 for stability",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Search API page size; smaller pages are slower but more stable",
    )
    parser.add_argument(
        "--cap-threshold",
        type=int,
        default=CAP_THRESHOLD,
        help="Treat partitions at or above this count as capped and split further",
    )
    parser.add_argument(
        "--keep-empty-jd",
        action="store_true",
        help="Keep rows whose JD text is empty",
    )
    parser.add_argument(
        "--transport",
        choices=["browser", "requests"],
        default="browser",
        help="Use browser automation or plain requests for search-page API calls",
    )
    parser.add_argument(
        "--browser-visible",
        action="store_true",
        help="Run 51job browser transport with a visible window instead of headless mode",
    )
    parser.add_argument(
        "--browser-profile-dir",
        default="data/runtime/51job/browser_profile",
        help="Persistent browser profile directory used by the browser transport",
    )
    parser.add_argument(
        "--browser-cdp-url",
        default="",
        help="Attach browser transport to an already-running real browser via CDP",
    )
    parser.add_argument(
        "--browser-min-interval",
        type=float,
        default=0.6,
        help="Minimum seconds between browser-side 51job API requests",
    )
    parser.add_argument(
        "--browser-max-retries",
        type=int,
        default=4,
        help="Maximum retries for one browser-side 51job API request",
    )
    parser.add_argument(
        "--manual-verify",
        action="store_true",
        help="Allow manual slider verification in the attached browser when needed",
    )
    parser.add_argument(
        "--manual-verify-wait",
        type=int,
        default=180,
        help="How many seconds to wait for manual browser verification",
    )
    return parser.parse_args()


def serialize_function(fn: FunctionCode) -> dict[str, str]:
    return {
        "function_code": fn.function_code,
        "function_label": fn.function_label,
    }


def serialize_area(area: Any) -> dict[str, str]:
    return {
        "area_id": clean_text(getattr(area, "area_id", "")),
        "label": clean_text(getattr(area, "label", "")),
    }


def load_selected_scope(
    args: argparse.Namespace,
) -> tuple[list[FunctionCode], list[Any], list[Any], dict[str, Any]]:
    function_path = ROOT_DIR / args.function_file
    area_path = ROOT_DIR / args.area_file
    raw_function_codes, area_tree, area_index = refresh_taxonomies(
        function_path=function_path,
        area_path=area_path,
        verbose=True,
    )
    function_codes = select_function_codes(raw_function_codes, args.specific_only)
    if clean_text(args.function_code):
        function_codes = [
            row for row in function_codes if row.function_code == clean_text(args.function_code)
        ]
    if args.max_functions > 0:
        function_codes = function_codes[: args.max_functions]
    if not function_codes:
        raise SystemExit("no function codes selected for the sequential crawl")

    if clean_text(args.job_area):
        area_code = clean_text(args.job_area)
        area = area_index.get(area_code)
        if area is None:
            raise SystemExit(f"jobArea not found in cached area tree: {area_code}")
        selected_areas = [area]
    else:
        selected_areas = select_root_children(
            area_tree,
            offset=max(args.top_level_area_offset, 0),
            limit=max(args.top_level_area_limit, 0),
        )
    if not selected_areas:
        raise SystemExit("no top-level areas selected for the sequential crawl")
    return function_codes, selected_areas, area_tree, area_index


def build_cursor(function_codes: list[FunctionCode], areas: list[Any]) -> dict[str, Any]:
    now = time.time()
    return {
        "mode": "51job_social_sequential",
        "status": "running",
        "started_at": now,
        "updated_at": now,
        "function_codes": [serialize_function(fn) for fn in function_codes],
        "areas": [serialize_area(area) for area in areas],
        "function_index": 0,
        "area_index": 0,
        "done_batches": 0,
        "total_batches": len(function_codes) * len(areas),
        "records_written_total": 0,
        "page_failures_total": 0,
        "recent_batches": [],
        "current_batch": None,
        "last_completed_batch": None,
    }


def validate_cursor(cursor: dict[str, Any], function_codes: list[FunctionCode], areas: list[Any]) -> None:
    expected_functions = [serialize_function(fn) for fn in function_codes]
    expected_areas = [serialize_area(area) for area in areas]
    if cursor.get("function_codes") != expected_functions or cursor.get("areas") != expected_areas:
        raise SystemExit(
            "existing cursor does not match the current selected functions/areas; "
            "use --reset-cursor to rebuild it"
        )


def load_cursor(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_progress_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_cursor(path: Path, cursor: dict[str, Any]) -> None:
    ensure_parent(path)
    cursor["updated_at"] = time.time()
    path.write_text(json.dumps(cursor, ensure_ascii=False, indent=2), encoding="utf-8")


def append_recent_batch(cursor: dict[str, Any], batch_summary: dict[str, Any], limit: int = 50) -> None:
    recent = list(cursor.get("recent_batches") or [])
    recent.append(batch_summary)
    if len(recent) > limit:
        recent = recent[-limit:]
    cursor["recent_batches"] = recent


def advance_cursor(cursor: dict[str, Any], area_total: int) -> None:
    area_index = int(cursor.get("area_index", 0)) + 1
    function_index = int(cursor.get("function_index", 0))
    if area_index >= area_total:
        area_index = 0
        function_index += 1
    cursor["area_index"] = area_index
    cursor["function_index"] = function_index


def current_batch_number(cursor: dict[str, Any], area_total: int) -> int:
    return int(cursor.get("function_index", 0)) * area_total + int(cursor.get("area_index", 0)) + 1


def current_batch_matches(
    batch: dict[str, Any],
    *,
    batch_number: int,
    function_code: str,
    job_area: str,
) -> bool:
    if not batch:
        return False
    if int(batch.get("batch_number") or 0) not in {0, batch_number}:
        return False
    if clean_text(batch.get("function_code", "")) != clean_text(function_code):
        return False
    if clean_text(batch.get("job_area", "")) != clean_text(job_area):
        return False
    return True


def extract_resume_state(
    cursor: dict[str, Any],
    *,
    batch_number: int,
    function_code: str,
    job_area: str,
    progress_snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    current_batch = cursor.get("current_batch") or {}
    if not current_batch_matches(
        current_batch,
        batch_number=batch_number,
        function_code=function_code,
        job_area=job_area,
    ):
        return None

    has_cursor_page_checkpoint = any(
        [
            int(current_batch.get("page_total") or 0) > 0,
            int(current_batch.get("page_done") or 0) > 0,
            bool(current_batch.get("resume_next_page")),
        ]
    )
    if has_cursor_page_checkpoint:
        return {
            "source": "cursor",
            "started_at": current_batch.get("started_at"),
            "page_done": int(current_batch.get("page_done") or 0),
            "page_total": int(current_batch.get("page_total") or 0),
            "records_written": int(current_batch.get("records_written") or 0),
            "empty_jd_dropped": int(current_batch.get("empty_jd_dropped") or 0),
            "status_note": clean_text(current_batch.get("status_note", "")),
            "current_label": clean_text(current_batch.get("current_label", "")),
            "next_page": current_batch.get("resume_next_page") or None,
        }

    if clean_text(current_batch.get("status", "")) == "failed" and int(current_batch.get("page_failures") or 0) > 0:
        return None

    has_legacy_progress = any(
        [
            int(progress_snapshot.get("page_total") or 0) > 0,
            int(progress_snapshot.get("page_done") or 0) > 0,
        ]
    )
    if has_legacy_progress:
        return {
            "source": "legacy_progress_snapshot",
            "started_at": current_batch.get("started_at") or progress_snapshot.get("started_at"),
            "page_done": int(progress_snapshot.get("page_done") or 0),
            "page_total": int(progress_snapshot.get("page_total") or 0),
            "records_written": int(progress_snapshot.get("records_written") or 0),
            "empty_jd_dropped": int(progress_snapshot.get("empty_jd_dropped") or 0),
            "status_note": clean_text(progress_snapshot.get("status_note", "")),
            "current_label": clean_text(progress_snapshot.get("current_label", "")),
            "next_page": None,
        }
    return None


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    function_codes, selected_areas, area_tree, area_index = load_selected_scope(args)
    output_raw = ROOT_DIR / args.output_raw
    manifest_path = ROOT_DIR / args.partition_manifest
    progress_path = ROOT_DIR / args.progress_file
    clean_output = ROOT_DIR / args.clean_output
    cursor_path = ROOT_DIR / args.cursor_file

    if args.reset_cursor or not cursor_path.exists():
        cursor = build_cursor(function_codes, selected_areas)
        write_cursor(cursor_path, cursor)
    else:
        cursor = load_cursor(cursor_path)
        validate_cursor(cursor, function_codes, selected_areas)

    if int(cursor.get("function_index", 0)) >= len(function_codes):
        cursor["status"] = "completed"
        cursor["current_batch"] = None
        write_cursor(cursor_path, cursor)
        print("Sequential crawl cursor is already completed; nothing to do.", flush=True)
        return

    print(
        f"Loaded sequential queue with {len(function_codes)} functions and {len(selected_areas)} areas; "
        f"total_batches={cursor.get('total_batches')}",
        flush=True,
    )
    print(
        f"Resuming from batch {current_batch_number(cursor, len(selected_areas))}/{cursor.get('total_batches')}: "
        f"function_index={cursor.get('function_index')} area_index={cursor.get('area_index')}",
        flush=True,
    )

    client = build_client(
        transport=args.transport,
        browser_visible=args.browser_visible,
        browser_profile_dir=args.browser_profile_dir,
        manual_verify=args.manual_verify,
        manual_verify_wait=args.manual_verify_wait,
        browser_cdp_url=args.browser_cdp_url,
        browser_min_interval=args.browser_min_interval,
        browser_max_retries=args.browser_max_retries,
    )
    batches_processed = 0
    try:
        while int(cursor.get("function_index", 0)) < len(function_codes):
            if args.max_batches > 0 and batches_processed >= args.max_batches:
                cursor["status"] = "paused"
                cursor["current_batch"] = None
                write_cursor(cursor_path, cursor)
                print(
                    f"Reached --max-batches={args.max_batches}; cursor saved to {cursor_path}",
                    flush=True,
                )
                break

            function_index = int(cursor.get("function_index", 0))
            area_index_value = int(cursor.get("area_index", 0))
            fn = function_codes[function_index]
            area = selected_areas[area_index_value]
            batch_number = current_batch_number(cursor, len(selected_areas))
            progress_snapshot = load_progress_snapshot(progress_path)
            batch_job_area = clean_text(getattr(area, "area_id", ""))
            resume_state = extract_resume_state(
                cursor,
                batch_number=batch_number,
                function_code=fn.function_code,
                job_area=batch_job_area,
                progress_snapshot=progress_snapshot,
            )
            batch_started_at = time.time()
            if resume_state:
                try:
                    batch_started_at = float(resume_state.get("started_at") or batch_started_at)
                except (TypeError, ValueError):
                    batch_started_at = time.time()

            current_batch = {
                "batch_number": batch_number,
                "total_batches": cursor.get("total_batches"),
                "function_index": function_index,
                "area_index": area_index_value,
                "function_code": fn.function_code,
                "function_label": fn.function_label,
                "job_area": batch_job_area,
                "job_area_label": clean_text(getattr(area, "label", "")),
                "started_at": batch_started_at,
            }
            if resume_state:
                current_batch["page_resume_source"] = resume_state.get("source")
                current_batch["page_done"] = int(resume_state.get("page_done") or 0)
                current_batch["page_total"] = int(resume_state.get("page_total") or 0)
                current_batch["records_written"] = int(resume_state.get("records_written") or 0)
                current_batch["empty_jd_dropped"] = int(resume_state.get("empty_jd_dropped") or 0)
                current_batch["current_label"] = clean_text(resume_state.get("current_label", ""))
                current_batch["status_note"] = clean_text(resume_state.get("status_note", ""))
                if resume_state.get("next_page"):
                    current_batch["resume_next_page"] = resume_state.get("next_page")
            cursor["status"] = "running"
            cursor["current_batch"] = current_batch
            write_cursor(cursor_path, cursor)

            print(
                f"[batch {batch_number}/{cursor.get('total_batches')}] "
                f"{fn.function_label} | {current_batch['job_area_label']} ({current_batch['job_area']})",
                flush=True,
            )
            if resume_state:
                print(
                    "Page-level resume checkpoint detected: "
                    f"source={resume_state.get('source')} "
                    f"page_done={resume_state.get('page_done', 0)}/{resume_state.get('page_total', 0)}",
                    flush=True,
                )

            def sync_current_batch(checkpoint: dict[str, Any]) -> None:
                current_batch.update(
                    {
                        "stage": checkpoint.get("stage"),
                        "partition_total": checkpoint.get("partition_total"),
                        "partition_done": checkpoint.get("partition_done"),
                        "final_partition_total": checkpoint.get("final_partition_total"),
                        "capped_partition_total": checkpoint.get("capped_partition_total"),
                        "page_total": checkpoint.get("page_total"),
                        "page_done": checkpoint.get("page_done"),
                        "page_failures": checkpoint.get("page_failures"),
                        "records_written": checkpoint.get("records_written"),
                        "empty_jd_dropped": checkpoint.get("empty_jd_dropped"),
                        "current_label": checkpoint.get("current_label"),
                        "status_note": checkpoint.get("status_note"),
                        "resume_page_index": checkpoint.get("resume_page_index"),
                        "resume_next_page": checkpoint.get("resume_next_page"),
                        "checkpoint_updated_at": time.time(),
                    }
                )
                cursor["current_batch"] = dict(current_batch)
                write_cursor(cursor_path, cursor)

            attempt = 0
            while True:
                try:
                    summary = crawl_scope(
                        client=client,
                        function_codes=[fn],
                        area_tree=area_tree,
                        area_index=area_index,
                        output_raw=output_raw,
                        manifest_path=manifest_path,
                        progress_path=progress_path,
                        workers=max(args.workers, 1),
                        page_size=max(args.page_size, 1),
                        cap_threshold=max(args.cap_threshold, 1),
                        keep_empty_jd=args.keep_empty_jd,
                        start_job_area=current_batch["job_area"],
                        append_output=True,
                        append_manifest=True,
                        write_manifest=resume_state is None,
                        resume_state=resume_state,
                        progress_callback=sync_current_batch,
                    )
                    break
                except Exception as exc:
                    if attempt >= max(args.batch_retries, 0):
                        raise
                    attempt += 1
                    failed_batch = dict(cursor.get("current_batch") or current_batch)
                    failed_batch.update(
                        {
                            "retry_attempt": attempt,
                            "last_error": str(exc),
                        }
                    )
                    cursor["current_batch"] = failed_batch
                    write_cursor(cursor_path, cursor)
                    resume_state = extract_resume_state(
                        cursor,
                        batch_number=batch_number,
                        function_code=fn.function_code,
                        job_area=batch_job_area,
                        progress_snapshot=load_progress_snapshot(progress_path),
                    )
                    print(
                        f"Batch {batch_number} failed with a fatal error; rebuilding the client and retrying "
                        f"({attempt}/{args.batch_retries}): {exc}",
                        flush=True,
                    )
                    close_client(client)
                    client = build_client(
                        transport=args.transport,
                        browser_visible=args.browser_visible,
                        browser_profile_dir=args.browser_profile_dir,
                        manual_verify=args.manual_verify,
                        manual_verify_wait=args.manual_verify_wait,
                        browser_cdp_url=args.browser_cdp_url,
                        browser_min_interval=args.browser_min_interval,
                        browser_max_retries=args.browser_max_retries,
                    )
                    time.sleep(2.0)
            state = summary["state"]
            latest_batch_state = dict(cursor.get("current_batch") or current_batch)

            batch_summary = {
                **latest_batch_state,
                "started_at": batch_started_at,
                "finished_at": time.time(),
                "records_written": state.records_written,
                "page_failures": state.page_failures,
                "final_partitions": state.final_partition_total,
                "capped_leaf_partitions": state.capped_partition_total,
                "empty_jd_dropped": state.empty_jd_dropped,
            }

            cursor["records_written_total"] = int(cursor.get("records_written_total", 0)) + int(state.records_written)
            cursor["page_failures_total"] = int(cursor.get("page_failures_total", 0)) + int(state.page_failures)
            cursor["last_completed_batch"] = batch_summary
            append_recent_batch(cursor, batch_summary)

            if args.refresh_clean:
                cleaned_count, dropped_empty_jd = clean_jsonl_to_csv(
                    output_raw,
                    clean_output,
                    allow_empty_jd=args.keep_empty_jd,
                )
                batch_summary["clean_rows"] = cleaned_count
                batch_summary["clean_dropped_empty"] = dropped_empty_jd
                cursor["last_completed_batch"] = batch_summary
                print(
                    f"Refreshed clean CSV: rows={cleaned_count} dropped_empty_jd={dropped_empty_jd}",
                    flush=True,
                )

            if state.page_failures > 0:
                failed_batch = dict(cursor.get("current_batch") or current_batch)
                failed_batch.update(
                    {
                        "status": "failed",
                        "page_failures": state.page_failures,
                    }
                )
                cursor["status"] = "paused"
                cursor["current_batch"] = failed_batch
                write_cursor(cursor_path, cursor)
                print(
                    f"Paused because batch {batch_number} still has page_failures={state.page_failures}; "
                    f"fix the browser session, then rerun the same command to resume from the saved page checkpoint.",
                    flush=True,
                )
                return

            cursor["done_batches"] = int(cursor.get("done_batches", 0)) + 1
            cursor["current_batch"] = None
            advance_cursor(cursor, len(selected_areas))
            batches_processed += 1

            if int(cursor.get("function_index", 0)) >= len(function_codes):
                cursor["status"] = "completed"
                write_cursor(cursor_path, cursor)
                print(
                    f"Sequential crawl completed all {cursor.get('total_batches')} batches; "
                    f"records_written_total={cursor.get('records_written_total')}",
                    flush=True,
                )
                break

            write_cursor(cursor_path, cursor)
    finally:
        close_client(client)


if __name__ == "__main__":
    main()
