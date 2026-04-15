from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen

from common import DEFAULT_HEADERS, ROOT_DIR, append_jsonl, ensure_parent, load_json, sha1_text, sleep_with_jitter


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取列表页或详情页 HTML")
    parser.add_argument("--seed-file", required=True, help="CSV 文件，至少包含 url 列")
    parser.add_argument("--manifest", required=True, help="抓取日志 jsonl")
    parser.add_argument("--output-dir", default="data/raw/html", help="HTML 输出目录")
    parser.add_argument("--url-column", default="url", help="链接列名")
    parser.add_argument("--platform-column", default="platform", help="平台列名")
    parser.add_argument("--page-type-column", default="page_type", help="页面类型列名")
    parser.add_argument("--delay", type=float, default=2.0, help="请求间隔秒数")
    parser.add_argument("--timeout", type=float, default=20.0, help="超时时间")
    parser.add_argument("--skip-existing", action="store_true", help="已存在则跳过")
    parser.add_argument("--limit", type=int, default=0, help="只抓前 N 条，0 表示不限制")
    parser.add_argument("--config", default="", help="可选的平台配置 json")
    return parser.parse_args()


def read_seed_rows(seed_file: Path) -> list[dict]:
    with open(seed_file, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    args = parse_args()
    seed_file = ROOT_DIR / args.seed_file
    manifest_path = ROOT_DIR / args.manifest
    output_dir = ROOT_DIR / args.output_dir

    rows = read_seed_rows(seed_file)
    if args.limit > 0:
        rows = rows[: args.limit]

    headers = dict(DEFAULT_HEADERS)
    if args.config:
        config = load_json(ROOT_DIR / args.config)
        headers.update(config.get("request_headers", {}))

    manifest_rows = []
    for row in rows:
        url = (row.get(args.url_column) or "").strip()
        if not url:
            continue

        platform = (row.get(args.platform_column) or "unknown").strip() or "unknown"
        page_type = (row.get(args.page_type_column) or "detail").strip() or "detail"
        filename = f"{sha1_text(url)}.html"
        html_path = output_dir / platform / page_type / filename
        ensure_parent(html_path)

        status_code = None
        error = ""
        fetched = False

        if args.skip_existing and html_path.exists():
            fetched = False
        else:
            try:
                request = Request(url, headers=headers)
                with urlopen(request, timeout=args.timeout) as response:
                    status_code = getattr(response, "status", None) or response.getcode()
                    body = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    html_path.write_text(body.decode(encoding, errors="ignore"), encoding="utf-8")
                fetched = True
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

        manifest_row = {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "platform": platform,
            "page_type": page_type,
            "url": url,
            "local_path": str(html_path),
            "status_code": status_code,
            "fetched": fetched,
            "error": error,
        }

        for key, value in row.items():
            if key not in manifest_row:
                manifest_row[key] = value

        manifest_rows.append(manifest_row)
        sleep_with_jitter(args.delay)

    append_jsonl(manifest_path, manifest_rows)
    print(f"saved {len(manifest_rows)} rows to {manifest_path}")


if __name__ == "__main__":
    main()
