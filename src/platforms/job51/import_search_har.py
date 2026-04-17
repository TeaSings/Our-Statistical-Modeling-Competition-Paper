from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, clean_text, ensure_parent  # noqa: E402
from platforms.job51.fetch_social_jobs import (  # noqa: E402
    FunctionCode,
    Partition,
    load_area_tree,
    load_function_codes,
    normalize_job_row,
)
from platforms.job51.search_taxonomy import flatten_area_index  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import 51job search-pc responses from HAR files")
    parser.add_argument(
        "--har",
        nargs="+",
        required=True,
        help="One or more HAR files exported from browser DevTools",
    )
    parser.add_argument(
        "--output-raw",
        default="data/raw/51job/records/51job_social_jobs_raw_from_har.jsonl",
        help="Output JSONL with normalized 51job rows imported from HAR",
    )
    parser.add_argument(
        "--function-file",
        default="data/input/51job/51job_search_function_codes.json",
        help="Cached function code JSON used to recover function labels",
    )
    parser.add_argument(
        "--area-file",
        default="data/input/51job/51job_search_area_tree.json",
        help="Cached area tree JSON used to recover area labels",
    )
    parser.add_argument(
        "--keep-empty-jd",
        action="store_true",
        help="Keep rows whose JD text is empty",
    )
    return parser.parse_args()


def _decode_har_text(content: dict[str, Any]) -> str:
    text = content.get("text", "")
    if not text:
        return ""
    if clean_text(content.get("encoding", "")).lower() == "base64":
        return base64.b64decode(text).decode("utf-8", errors="replace")
    return text


def iter_search_payloads(har_path: Path) -> list[tuple[str, dict[str, Any]]]:
    payload = json.loads(har_path.read_text(encoding="utf-8"))
    entries = payload.get("log", {}).get("entries", []) or []
    output: list[tuple[str, dict[str, Any]]] = []
    for entry in entries:
        request = entry.get("request", {}) or {}
        url = clean_text(request.get("url", ""))
        if "/api/job/search-pc" not in url:
            continue
        response_content = entry.get("response", {}).get("content", {}) or {}
        text = _decode_har_text(response_content)
        if not text:
            continue
        try:
            search_payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if str(search_payload.get("status")) != "1":
            continue
        output.append((url, search_payload))
    return output


def build_function_label_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    rows: list[FunctionCode] = load_function_codes(path)
    return {row.function_code: row.function_label for row in rows}


def build_area_label_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    area_index = flatten_area_index(load_area_tree(path))
    return {area_id: node.label for area_id, node in area_index.items()}


def infer_partition(
    request_url: str,
    payload: dict[str, Any],
    function_label_map: dict[str, str],
    area_label_map: dict[str, str],
) -> tuple[Partition, int]:
    query = parse_qs(urlparse(request_url).query)
    function_code = clean_text(query.get("function", [""])[0])
    keyword = clean_text(query.get("keyword", [""])[0])
    job_area = clean_text(query.get("jobArea", [""])[0]) or "000000"
    page_num_text = clean_text(query.get("pageNum", ["1"])[0]) or "1"
    page_num = int(page_num_text)

    items = payload.get("resultbody", {}).get("job", {}).get("items", []) or []
    fallback_area = clean_text(items[0].get("jobAreaString", "")) if items else ""
    function_label = function_label_map.get(function_code) or keyword or function_code or "manual_import"
    area_label = area_label_map.get(job_area) or fallback_area or job_area

    return (
        Partition(
            function_code=function_code,
            function_label=function_label,
            job_area=job_area,
            job_area_label=area_label,
            depth=0,
        ),
        page_num,
    )


def main() -> None:
    args = parse_args()
    output_raw = ROOT_DIR / args.output_raw
    function_label_map = build_function_label_map(ROOT_DIR / args.function_file)
    area_label_map = build_area_label_map(ROOT_DIR / args.area_file)

    ensure_parent(output_raw)
    seen_job_ids: set[str] = set()
    pages_imported = 0
    records_written = 0
    empty_jd_dropped = 0

    with output_raw.open("w", encoding="utf-8") as raw_file:
        for har_file in args.har:
            har_path = ROOT_DIR / har_file if not Path(har_file).is_absolute() else Path(har_file)
            payloads = iter_search_payloads(har_path)
            pages_imported += len(payloads)
            for request_url, payload in payloads:
                partition, page_num = infer_partition(
                    request_url=request_url,
                    payload=payload,
                    function_label_map=function_label_map,
                    area_label_map=area_label_map,
                )
                items = payload.get("resultbody", {}).get("job", {}).get("items", []) or []
                for item in items:
                    source_job_id = clean_text(item.get("jobId", ""))
                    if not source_job_id or source_job_id in seen_job_ids:
                        continue
                    row = normalize_job_row(item, partition, page_num)
                    if not row["jd_text_raw"] and not args.keep_empty_jd:
                        empty_jd_dropped += 1
                        continue
                    seen_job_ids.add(source_job_id)
                    raw_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    records_written += 1

    print(
        f"imported {pages_imported} HAR search pages into {output_raw}; "
        f"records_written={records_written}; "
        f"empty_jd_dropped={empty_jd_dropped}"
    )


if __name__ == "__main__":
    main()
