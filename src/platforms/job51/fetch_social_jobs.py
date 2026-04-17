from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, clean_text, configure_utf8_stdio, ensure_parent  # noqa: E402
from platforms.job51.browser_search_client import BrowserSearchClient  # noqa: E402
from platforms.job51.search_taxonomy import (  # noqa: E402
    AreaNode,
    fetch_search_taxonomies,
    flatten_area_index,
)
from platforms.job51.we_search_client import SearchClient  # noqa: E402


PLATFORM_NAME = "51job_social"
CAP_THRESHOLD = 990
PAGE_SIZE = 100
ROOT_AREA_CODE = "000000"
ROOT_AREA_LABEL = "全国"


@dataclass
class FunctionCode:
    function_code: str
    function_label: str


@dataclass
class Partition:
    function_code: str
    function_label: str
    job_area: str
    job_area_label: str
    depth: int = 0
    total_count: int = 0
    status: str = ""

    @property
    def key(self) -> str:
        return f"{self.function_code}@{self.job_area}"


@dataclass
class ProgressState:
    function_total: int
    started_at: float = field(default_factory=time.time)
    stage: str = "planning"
    function_done: int = 0
    partition_total: int = 0
    partition_done: int = 0
    final_partition_total: int = 0
    capped_partition_total: int = 0
    page_total: int = 0
    page_done: int = 0
    page_failures: int = 0
    records_written: int = 0
    empty_jd_dropped: int = 0
    current_label: str = ""
    status_note: str = ""
    last_render_at: float = 0.0

    def render(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_render_at < 0.2:
            return
        self.last_render_at = now
        elapsed = max(now - self.started_at, 1e-6)
        if self.stage == "planning":
            total = max(self.function_total, 1)
            done = self.function_done
        elif self.page_total > 0:
            total = self.page_total
            done = self.page_done
        elif self.partition_total > 0:
            total = self.partition_total
            done = self.partition_done
        else:
            total = max(self.function_total, 1)
            done = self.function_done

        ratio = min(max(done / max(total, 1), 0.0), 1.0)
        width = 28
        filled = int(width * ratio)
        bar = "[" + "#" * filled + "-" * (width - filled) + "]"
        rate = done / elapsed
        eta_seconds: int | None = None
        if rate > 0 and done < total:
            eta_seconds = int((total - done) / rate)

        message = (
            f"\r{bar} {done}/{total} "
            f"| stage {self.stage} "
            f"| functions {self.function_done}/{self.function_total} "
            f"| partitions {self.partition_done}/{self.partition_total} "
            f"| final {self.final_partition_total} "
            f"| capped {self.capped_partition_total} "
            f"| pages {self.page_done}/{self.page_total} "
            f"| page_fail {self.page_failures} "
            f"| records {self.records_written} "
            f"| empty_jd_drop {self.empty_jd_dropped} "
            f"| note {self.status_note[:24]} "
            f"| current {self.current_label[:36]} "
            f"| elapsed {format_seconds(int(elapsed))} "
            f"| ETA {format_seconds(eta_seconds) if eta_seconds is not None else '--'}"
        )
        print(message[:260], end="", flush=True)

    def newline(self) -> None:
        print("", flush=True)

    def snapshot(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "function_total": self.function_total,
            "function_done": self.function_done,
            "partition_total": self.partition_total,
            "partition_done": self.partition_done,
            "final_partition_total": self.final_partition_total,
            "capped_partition_total": self.capped_partition_total,
            "page_total": self.page_total,
            "page_done": self.page_done,
            "page_failures": self.page_failures,
            "records_written": self.records_written,
            "empty_jd_dropped": self.empty_jd_dropped,
            "current_label": self.current_label,
            "status_note": self.status_note,
            "started_at": self.started_at,
            "updated_at": time.time(),
        }


def format_seconds(seconds: int | None) -> str:
    if seconds is None:
        return "--"
    if seconds <= 0:
        return "0s"
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{sec:02d}s"
    return f"{sec}s"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch full social-recruiting jobs from 51job")
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
        help="Progress snapshot JSON file",
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
        "--workers",
        type=int,
        default=6,
        help="Concurrent workers for page fetching",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=PAGE_SIZE,
        help="Search API page size",
    )
    parser.add_argument(
        "--cap-threshold",
        type=int,
        default=CAP_THRESHOLD,
        help="Treat partitions at or above this count as capped and split further",
    )
    parser.add_argument(
        "--max-functions",
        type=int,
        default=0,
        help="Only plan the first N function codes when > 0",
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
        "--job-area",
        default="",
        help="Only crawl one specific 51job jobArea code",
    )
    parser.add_argument(
        "--top-level-area-offset",
        type=int,
        default=0,
        help="When crawling nationally, skip the first N top-level areas in nationwide order",
    )
    parser.add_argument(
        "--top-level-area-limit",
        type=int,
        default=0,
        help="When crawling nationally, only crawl the next N top-level areas in nationwide order",
    )
    parser.add_argument(
        "--keep-empty-jd",
        action="store_true",
        help="Keep rows whose JD text is empty",
    )
    parser.add_argument(
        "--append-output",
        action="store_true",
        help="Append new raw rows to an existing output JSONL instead of overwriting it",
    )
    parser.add_argument(
        "--append-manifest",
        action="store_true",
        help="Append partition planning rows to an existing manifest instead of overwriting it",
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
        help="Run 51job browser transport with a visible Playwright window when not auto-starting a manual-verification browser",
    )
    parser.add_argument(
        "--browser-profile-dir",
        default="data/runtime/51job/browser_profile",
        help="Persistent browser profile directory used by the browser transport",
    )
    parser.add_argument(
        "--browser-cdp-url",
        default="",
        help="Optional CDP endpoint for an already-running real browser; when omitted with --manual-verify, the script auto-starts a local browser",
    )
    parser.add_argument(
        "--browser-min-interval",
        type=float,
        default=0.35,
        help="Minimum seconds between browser-side 51job API requests",
    )
    parser.add_argument(
        "--browser-max-retries",
        type=int,
        default=3,
        help="Maximum retries for one browser-side 51job API request",
    )
    parser.add_argument(
        "--manual-verify",
        action="store_true",
        help="When 51job shows a slider challenge, pause and allow manual verification in a visible browser window",
    )
    parser.add_argument(
        "--manual-verify-wait",
        type=int,
        default=180,
        help="How many seconds to wait for manual browser verification when --manual-verify is enabled",
    )
    return parser.parse_args()


def write_progress(path: Path, state: ProgressState) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(state.snapshot(), ensure_ascii=False, indent=2), encoding="utf-8")


def refresh_taxonomies(
    *,
    function_path: Path,
    area_path: Path,
    verbose: bool = True,
) -> tuple[list[FunctionCode], list[AreaNode], dict[str, AreaNode]]:
    if verbose:
        print("Refreshing 51job live taxonomies...", flush=True)
    try:
        fetch_search_taxonomies(area_output=area_path, function_output=function_path)
    except Exception as exc:
        if not area_path.exists() or not function_path.exists():
            raise
        if verbose:
            print(
                f"Live taxonomy refresh failed, falling back to cached files: {exc}",
                flush=True,
            )
    function_codes = load_function_codes(function_path)
    area_tree = load_area_tree(area_path)
    area_index = flatten_area_index(area_tree)
    return function_codes, area_tree, area_index


def load_function_codes(path: Path) -> list[FunctionCode]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    return [
        FunctionCode(
            function_code=clean_text(row.get("function_code", "")),
            function_label=clean_text(row.get("function_label", "")),
        )
        for row in rows
        if clean_text(row.get("function_code", ""))
    ]


def load_area_tree(path: Path) -> list[AreaNode]:
    rows = json.loads(path.read_text(encoding="utf-8"))

    def to_node(row: dict[str, Any]) -> AreaNode:
        return AreaNode(
            area_id=clean_text(row.get("area_id", "")),
            name=clean_text(row.get("name", "")),
            label=clean_text(row.get("label", "")),
            children=[to_node(child) for child in row.get("children", []) or []],
        )

    return [to_node(row) for row in rows]


def select_function_codes(function_codes: list[FunctionCode], specific_only: bool) -> list[FunctionCode]:
    if not specific_only:
        return function_codes
    return [row for row in function_codes if not row.function_code.endswith("00")]


def get_root_children(area_tree: list[AreaNode]) -> list[AreaNode]:
    nodes_by_code: dict[str, AreaNode] = {}
    for node in area_tree:
        if not node.area_id.endswith("0000"):
            continue
        current = nodes_by_code.get(node.area_id)
        if current is None or len(node.children) > len(current.children):
            nodes_by_code[node.area_id] = node
    return [nodes_by_code[area_id] for area_id in sorted(nodes_by_code)]


def select_root_children(
    area_tree: list[AreaNode],
    *,
    offset: int = 0,
    limit: int = 0,
) -> list[AreaNode]:
    roots = get_root_children(area_tree)
    if offset > 0:
        roots = roots[offset:]
    if limit > 0:
        roots = roots[:limit]
    return roots


def load_existing_job_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    job_ids: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            job_id = clean_text(row.get("job_id", ""))
            if job_id:
                job_ids.add(job_id)
    return job_ids


def get_child_areas(area_code: str, area_tree: list[AreaNode], area_index: dict[str, AreaNode]) -> list[AreaNode]:
    if area_code == ROOT_AREA_CODE:
        return get_root_children(area_tree)
    node = area_index.get(area_code)
    seen: set[str] = set()
    direct_children: list[AreaNode] = []
    if node:
        for child in node.children:
            if not child.area_id or child.area_id == area_code or child.area_id in seen:
                continue
            seen.add(child.area_id)
            direct_children.append(child)
    if direct_children:
        return direct_children
    if not area_code.endswith("0000"):
        return []

    prefix = area_code[:2]
    fallback_children: list[AreaNode] = []
    for candidate in area_tree:
        if (
            not candidate.area_id
            or candidate.area_id == area_code
            or not candidate.area_id.startswith(prefix)
            or "-" in candidate.label
            or candidate.area_id in seen
        ):
            continue
        seen.add(candidate.area_id)
        fallback_children.append(candidate)
    return fallback_children


def normalize_company_industry(item: dict[str, Any]) -> str:
    values = [
        clean_text(item.get("companyIndustryType1Str", "")),
        clean_text(item.get("companyIndustryType2Str", "")),
        clean_text(item.get("industryType1Str", "")),
        clean_text(item.get("industryType2Str", "")),
    ]
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return " / ".join(output)


def normalize_job_row(item: dict[str, Any], partition: Partition, page_num: int) -> dict[str, Any]:
    area_detail = item.get("jobAreaLevelDetail", {}) or {}
    city_seed = partition.job_area_label
    tags = item.get("jobTags", []) or []
    source_url = (
        "https://we.51job.com/pc/search"
        f"?jobArea={partition.job_area}&function={partition.function_code}"
    )
    return {
        "platform": PLATFORM_NAME,
        "job_id": clean_text(item.get("jobId", "")),
        "detail_url": clean_text(item.get("jobHref", "")),
        "source_url": source_url,
        "city_seed": city_seed,
        "keyword_seed": partition.function_label,
        "job_title_raw": clean_text(item.get("jobName", "")),
        "company_name_raw": clean_text(item.get("fullCompanyName", "") or item.get("companyName", "")),
        "city_raw": clean_text(item.get("jobAreaString", "")),
        "salary_raw": clean_text(item.get("provideSalaryString", "")),
        "education_raw": clean_text(item.get("degreeString", "")),
        "experience_raw": clean_text(item.get("workYearString", "")),
        "company_industry_raw": normalize_company_industry(item),
        "company_size_raw": clean_text(item.get("companySizeString", "")),
        "job_tags_raw": " | ".join(clean_text(tag) for tag in tags if clean_text(tag)),
        "jd_text_raw": clean_text(item.get("jobDescribe", "")),
        "strategy": "search_pc",
        "function_code": partition.function_code,
        "function_label": partition.function_label,
        "job_area_code": partition.job_area,
        "job_area_label": partition.job_area_label,
        "request_page_num": page_num,
        "company_type_raw": clean_text(item.get("companyTypeString", "")),
        "work_area_code": clean_text(item.get("workAreaCode", "")),
        "job_area_level_province": clean_text(area_detail.get("provinceString", "")),
        "job_area_level_city": clean_text(area_detail.get("cityString", "")),
        "job_area_level_district": clean_text(area_detail.get("districtString", "")),
        "address_raw": clean_text(area_detail.get("landMarkString", "")),
    }


def plan_partitions(
    *,
    client: Any,
    function_codes: list[FunctionCode],
    area_tree: list[AreaNode],
    area_index: dict[str, AreaNode],
    manifest_path: Path,
    progress_path: Path,
    state: ProgressState,
    cap_threshold: int,
    start_job_area: str = "",
    root_area_offset: int = 0,
    root_area_limit: int = 0,
    append_manifest: bool = False,
    write_manifest: bool = True,
) -> list[Partition]:
    final_partitions: list[Partition] = []
    manifest_file = None
    if write_manifest:
        ensure_parent(manifest_path)
        manifest_mode = "a" if append_manifest else "w"
        manifest_file = manifest_path.open(manifest_mode, encoding="utf-8")
    try:
        for fn in function_codes:
            if start_job_area:
                start_area = area_index.get(start_job_area)
                queue = [
                    Partition(
                        fn.function_code,
                        fn.function_label,
                        start_job_area,
                        clean_text(start_area.label if start_area else start_job_area),
                        depth=1,
                    )
                ]
            else:
                root_children = select_root_children(
                    area_tree,
                    offset=max(root_area_offset, 0),
                    limit=max(root_area_limit, 0),
                )
                queue = [
                    Partition(
                        fn.function_code,
                        fn.function_label,
                        child.area_id,
                        child.label,
                        depth=1,
                    )
                    for child in root_children
                ]
            if not queue:
                queue = [Partition(fn.function_code, fn.function_label, ROOT_AREA_CODE, ROOT_AREA_LABEL, depth=0)]
            seen_partitions: set[str] = set()
            state.current_label = f"{fn.function_label} ({fn.function_code})"
            state.render(force=True)
            while queue:
                partition = queue.pop(0)
                if partition.key in seen_partitions:
                    continue
                seen_partitions.add(partition.key)
                _, total_count = client.get_job_page(
                    function_code=partition.function_code,
                    job_area=partition.job_area,
                    page_num=1,
                    page_size=1,
                )
                partition.total_count = total_count
                state.partition_total += 1
                state.partition_done += 1

                child_areas = get_child_areas(partition.job_area, area_tree, area_index)
                if total_count <= 0:
                    partition.status = "empty"
                elif total_count >= cap_threshold and child_areas:
                    partition.status = "split"
                    for child in child_areas:
                        queue.append(
                            Partition(
                                function_code=partition.function_code,
                                function_label=partition.function_label,
                                job_area=child.area_id,
                                job_area_label=child.label,
                                depth=partition.depth + 1,
                            )
                        )
                else:
                    partition.status = "final"
                    final_partitions.append(partition)
                    state.final_partition_total += 1
                    if total_count >= cap_threshold and not child_areas:
                        state.capped_partition_total += 1

                if manifest_file is not None:
                    manifest_file.write(json.dumps(
                        {
                            "function_code": partition.function_code,
                            "function_label": partition.function_label,
                            "job_area": partition.job_area,
                            "job_area_label": partition.job_area_label,
                            "depth": partition.depth,
                            "total_count": partition.total_count,
                            "status": partition.status,
                        },
                        ensure_ascii=False,
                    ) + "\n")
                    manifest_file.flush()
                write_progress(progress_path, state)
                state.render()
                time.sleep(0.05 if isinstance(client, SearchClient) else 0.02)

            state.function_done += 1
            write_progress(progress_path, state)
            state.render(force=True)

    finally:
        if manifest_file is not None:
            manifest_file.close()

    return final_partitions


_THREAD_LOCAL = threading.local()


def get_thread_client() -> SearchClient:
    client = getattr(_THREAD_LOCAL, "client", None)
    if client is None:
        client = SearchClient()
        _THREAD_LOCAL.client = client
    return client


def fetch_page_task(partition: Partition, page_num: int, page_size: int) -> tuple[Partition, int, int, list[dict[str, Any]]]:
    client = get_thread_client()
    items, total_count = client.get_job_page(
        function_code=partition.function_code,
        job_area=partition.job_area,
        page_num=page_num,
        page_size=page_size,
    )
    return partition, page_num, total_count, items


def describe_page_task(partition: Partition, page_num: int) -> dict[str, Any]:
    return {
        "partition_key": partition.key,
        "function_code": partition.function_code,
        "function_label": partition.function_label,
        "job_area": partition.job_area,
        "job_area_label": partition.job_area_label,
        "page_num": int(page_num),
        "label": f"{partition.function_label} | {partition.job_area_label} | page {page_num}",
    }


def resolve_resume_start_index(
    page_tasks: list[tuple[Partition, int]],
    resume_state: dict[str, Any] | None,
) -> int:
    if not resume_state:
        return 0

    next_page = resume_state.get("next_page") or {}
    next_partition_key = clean_text(next_page.get("partition_key", ""))
    next_page_num = int(next_page.get("page_num") or 0)
    if next_partition_key and next_page_num > 0:
        for index, (partition, page_num) in enumerate(page_tasks):
            if partition.key == next_partition_key and page_num == next_page_num:
                return index

    page_done = int(resume_state.get("page_done") or 0)
    return min(max(page_done, 0), len(page_tasks))


def build_client(
    *,
    transport: str = "browser",
    browser_visible: bool = False,
    browser_profile_dir: str = "data/runtime/51job/browser_profile",
    manual_verify: bool = False,
    manual_verify_wait: int = 180,
    browser_cdp_url: str = "",
    browser_min_interval: float = 0.35,
    browser_max_retries: int = 3,
) -> Any:
    if transport == "browser":
        return BrowserSearchClient(
            headless=not browser_visible,
            user_data_dir=browser_profile_dir,
            allow_manual_verification=manual_verify,
            manual_verification_wait_ms=max(manual_verify_wait, 1) * 1000,
            cdp_url=clean_text(browser_cdp_url),
            min_interval_seconds=max(browser_min_interval, 0.0),
            max_retries=max(browser_max_retries, 1),
        )
    return SearchClient()


def close_client(client: Any) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        close()


def crawl_scope(
    *,
    client: Any,
    function_codes: list[FunctionCode],
    area_tree: list[AreaNode],
    area_index: dict[str, AreaNode],
    output_raw: Path,
    manifest_path: Path,
    progress_path: Path,
    workers: int = 1,
    page_size: int = PAGE_SIZE,
    cap_threshold: int = CAP_THRESHOLD,
    keep_empty_jd: bool = False,
    start_job_area: str = "",
    root_area_offset: int = 0,
    root_area_limit: int = 0,
    append_output: bool = False,
    append_manifest: bool = False,
    write_manifest: bool = True,
    resume_state: dict[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    resume_started_at = 0.0
    if resume_state:
        try:
            resume_started_at = float(resume_state.get("started_at") or 0.0)
        except (TypeError, ValueError):
            resume_started_at = 0.0
    state = ProgressState(
        function_total=len(function_codes),
        started_at=resume_started_at or time.time(),
    )
    page_tasks: list[tuple[Partition, int]] = []
    next_page_index = 0

    def build_checkpoint(next_index: int) -> dict[str, Any]:
        next_page = None
        if 0 <= next_index < len(page_tasks):
            next_page = describe_page_task(*page_tasks[next_index])
        return {
            "stage": state.stage,
            "function_total": state.function_total,
            "function_done": state.function_done,
            "partition_total": state.partition_total,
            "partition_done": state.partition_done,
            "final_partition_total": state.final_partition_total,
            "capped_partition_total": state.capped_partition_total,
            "page_total": state.page_total,
            "page_done": state.page_done,
            "page_failures": state.page_failures,
            "records_written": state.records_written,
            "empty_jd_dropped": state.empty_jd_dropped,
            "current_label": state.current_label,
            "status_note": state.status_note,
            "started_at": state.started_at,
            "resume_page_index": next_index,
            "resume_next_page": next_page,
        }

    def emit_progress(*, force_render: bool = False, next_index: int | None = None) -> None:
        nonlocal next_page_index
        if next_index is not None:
            next_page_index = next_index
        write_progress(progress_path, state)
        if callable(progress_callback):
            progress_callback(build_checkpoint(next_page_index))
        state.render(force=force_render)

    emit_progress(force_render=True, next_index=0)

    if isinstance(client, BrowserSearchClient):
        def handle_browser_status(event: str, payload: dict[str, Any]) -> None:
            if event == "manual_verification_waiting":
                wait_seconds = int(payload.get("wait_seconds") or 0)
                state.status_note = (
                    f"waiting manual verification ({wait_seconds}s)"
                    if wait_seconds > 0
                    else "waiting manual verification"
                )
            elif event == "manual_verification_resumed":
                state.status_note = ""
            else:
                return
            emit_progress(force_render=True)

        client.status_callback = handle_browser_status

    state.stage = "planning"
    emit_progress()
    final_partitions = plan_partitions(
        client=client,
        function_codes=function_codes,
        area_tree=area_tree,
        area_index=area_index,
        manifest_path=manifest_path,
        progress_path=progress_path,
        state=state,
        cap_threshold=max(cap_threshold, 1),
        start_job_area=clean_text(start_job_area),
        root_area_offset=max(root_area_offset, 0),
        root_area_limit=max(root_area_limit, 0),
        append_manifest=append_manifest,
        write_manifest=write_manifest,
    )

    effective_page_size = max(page_size, 1)
    for partition in final_partitions:
        page_count = max(math.ceil(partition.total_count / effective_page_size), 1)
        for page_num in range(1, page_count + 1):
            page_tasks.append((partition, page_num))
    resume_start_index = resolve_resume_start_index(page_tasks, resume_state)
    state.page_total = len(page_tasks)
    state.page_done = resume_start_index
    if resume_state:
        state.records_written = int(resume_state.get("records_written") or 0)
        state.empty_jd_dropped = int(resume_state.get("empty_jd_dropped") or 0)
    next_page_index = resume_start_index
    state.stage = "fetching"
    if resume_start_index < len(page_tasks):
        state.current_label = describe_page_task(*page_tasks[resume_start_index])["label"]
    else:
        state.current_label = ""
    emit_progress(force_render=True, next_index=resume_start_index)
    if resume_start_index > 0:
        if resume_start_index < len(page_tasks):
            print(
                "Resuming current batch from page checkpoint "
                f"{resume_start_index}/{len(page_tasks)}: {state.current_label}",
                flush=True,
            )
        else:
            print(
                f"Resume checkpoint indicates this batch already reached {resume_start_index}/{len(page_tasks)} pages.",
                flush=True,
            )

    ensure_parent(output_raw)
    seen_job_ids: set[str] = load_existing_job_ids(output_raw) if append_output else set()
    raw_mode = "a" if append_output else "w"
    if append_output:
        print(
            f"Appending into existing raw output with {len(seen_job_ids)} known job ids: {output_raw}",
            flush=True,
        )

    with output_raw.open(raw_mode, encoding="utf-8") as raw_file:
        if isinstance(client, BrowserSearchClient):
            for page_index, (partition, page_num) in enumerate(page_tasks[resume_start_index:], start=resume_start_index):
                next_page_index = page_index
                state.current_label = f"{partition.function_label} | {partition.job_area_label} | page {page_num}"
                try:
                    items, _ = client.get_job_page(
                        function_code=partition.function_code,
                        job_area=partition.job_area,
                        page_num=page_num,
                        page_size=effective_page_size,
                    )
                except Exception as exc:
                    print("", flush=True)
                    print(
                        f"page fetch failed for {partition.function_code}/{partition.job_area}/page={page_num}: {exc}",
                        flush=True,
                    )
                    state.page_failures += 1
                    state.status_note = "page fetch failed; rerun will resume from this page"
                    emit_progress(force_render=True, next_index=page_index)
                    break

                emitted = 0
                for item in items:
                    source_job_id = clean_text(item.get("jobId", ""))
                    if not source_job_id or source_job_id in seen_job_ids:
                        continue
                    row = normalize_job_row(item, partition, page_num)
                    if not row["jd_text_raw"] and not keep_empty_jd:
                        state.empty_jd_dropped += 1
                        continue
                    seen_job_ids.add(source_job_id)
                    raw_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    emitted += 1

                raw_file.flush()
                state.records_written += emitted
                state.page_done = page_index + 1
                state.status_note = ""
                emit_progress(next_index=page_index + 1)
        else:
            with ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
                future_map = {
                    executor.submit(fetch_page_task, partition, page_num, effective_page_size): (partition, page_num)
                    for partition, page_num in page_tasks[resume_start_index:]
                }
                for future in as_completed(future_map):
                    partition, page_num = future_map[future]
                    state.current_label = f"{partition.function_label} | {partition.job_area_label} | page {page_num}"
                    try:
                        _, _, _, items = future.result()
                    except Exception as exc:
                        print("", flush=True)
                        print(
                            f"page fetch failed for {partition.function_code}/{partition.job_area}/page={page_num}: {exc}",
                            flush=True,
                        )
                        state.page_done += 1
                        state.page_failures += 1
                        emit_progress(force_render=True, next_index=state.page_done)
                        continue

                    emitted = 0
                    for item in items:
                        source_job_id = clean_text(item.get("jobId", ""))
                        if not source_job_id or source_job_id in seen_job_ids:
                            continue
                        row = normalize_job_row(item, partition, page_num)
                        if not row["jd_text_raw"] and not keep_empty_jd:
                            state.empty_jd_dropped += 1
                            continue
                        seen_job_ids.add(source_job_id)
                        raw_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                        emitted += 1

                    raw_file.flush()
                    state.records_written += emitted
                    state.page_done += 1
                    state.status_note = ""
                    emit_progress(next_index=state.page_done)

    if state.page_failures > 0:
        state.newline()
        print(
            f"paused current batch at page checkpoint {state.page_done}/{state.page_total}; "
            f"page_failures={state.page_failures}",
            flush=True,
        )
        return {
            "state": state,
            "final_partitions": final_partitions,
        }

    state.stage = "completed"
    emit_progress(force_render=True, next_index=state.page_total)
    state.newline()
    print(
        f"saved {state.records_written} unique raw jobs to {output_raw}; "
        f"final_partitions={state.final_partition_total}; "
        f"capped_leaf_partitions={state.capped_partition_total}; "
        f"page_failures={state.page_failures}; "
        f"empty_jd_dropped={state.empty_jd_dropped}",
        flush=True,
    )
    return {
        "state": state,
        "final_partitions": final_partitions,
    }


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    output_raw = ROOT_DIR / args.output_raw
    manifest_path = ROOT_DIR / args.partition_manifest
    progress_path = ROOT_DIR / args.progress_file
    function_path = ROOT_DIR / args.function_file
    area_path = ROOT_DIR / args.area_file

    raw_function_codes, area_tree, area_index = refresh_taxonomies(
        function_path=function_path,
        area_path=area_path,
        verbose=True,
    )
    function_codes = select_function_codes(raw_function_codes, args.specific_only)
    if clean_text(args.function_code):
        function_codes = [row for row in function_codes if row.function_code == clean_text(args.function_code)]
    if args.max_functions > 0:
        function_codes = function_codes[: args.max_functions]
    selected_root_children = select_root_children(
        area_tree,
        offset=max(args.top_level_area_offset, 0),
        limit=max(args.top_level_area_limit, 0),
    )

    state = ProgressState(function_total=len(function_codes))
    write_progress(progress_path, state)
    state.render(force=True)

    print("", flush=True)
    print(f"Loaded {len(function_codes)} function codes and {len(area_index)} area nodes", flush=True)
    if not clean_text(args.job_area) and selected_root_children:
        labels = ", ".join(f"{node.label}({node.area_id})" for node in selected_root_children[:8])
        suffix = " ..." if len(selected_root_children) > 8 else ""
        print(
            f"Selected {len(selected_root_children)} top-level areas in nationwide order: {labels}{suffix}",
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
    try:
        crawl_scope(
            client=client,
            function_codes=function_codes,
            area_tree=area_tree,
            area_index=area_index,
            output_raw=output_raw,
            manifest_path=manifest_path,
            progress_path=progress_path,
            workers=args.workers,
            page_size=args.page_size,
            cap_threshold=args.cap_threshold,
            keep_empty_jd=args.keep_empty_jd,
            start_job_area=clean_text(args.job_area),
            root_area_offset=args.top_level_area_offset,
            root_area_limit=args.top_level_area_limit,
            append_output=args.append_output,
            append_manifest=args.append_manifest,
        )
    finally:
        close_client(client)


if __name__ == "__main__":
    main()
