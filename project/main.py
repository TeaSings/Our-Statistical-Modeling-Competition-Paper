from __future__ import annotations

import argparse
from dataclasses import replace

from adapters.base import AdapterConfigError, BaseAdapter, BlockedResponseError, CrawlerError
from adapters.boss import BossAdapter
from adapters.job51 import BlockedException, Job51Adapter
from adapters.zhilian import ZhilianAdapter
from browser_session import get_cookies
from config import (
    BROWSER_CONFIG,
    DEFAULT_PAGE_COUNT,
    DEFAULT_SITE,
    REQUEST_CONFIG,
    STORAGE_CONFIG,
    get_site_config,
)
from storage import JobStorage


ADAPTERS: dict[str, type[BaseAdapter]] = {
    "job51": Job51Adapter,
    "boss": BossAdapter,
    "zhilian": ZhilianAdapter,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-site job crawler framework for list pages only."
    )
    parser.add_argument(
        "--site",
        choices=sorted(ADAPTERS),
        default=DEFAULT_SITE,
        help="Which site adapter to use.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=DEFAULT_PAGE_COUNT,
        help="How many pages to crawl. Default: 3",
    )
    parser.add_argument(
        "--keyword",
        default="python",
        help="Keyword placeholder for URL templates that use {keyword}.",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=None,
        help="Multiple keywords to crawl sequentially, for example: --keywords python java 数据分析",
    )
    parser.add_argument(
        "--city",
        default="上海",
        help="City placeholder for URL templates that use {city}.",
    )
    parser.add_argument(
        "--cdp-url",
        default=BROWSER_CONFIG.cdp_url,
        help="Edge remote debugging endpoint.",
    )
    parser.add_argument(
        "--list-url-template",
        default=None,
        help="Override the list URL template configured in project/config.py.",
    )
    return parser.parse_args()


def build_adapter(site_name: str, *, list_url_template: str | None, cookies: dict[str, str]) -> BaseAdapter:
    site_config = get_site_config(site_name)
    if list_url_template:
        site_config = replace(site_config, list_url_template=list_url_template)
    adapter_cls = ADAPTERS[site_name]
    return adapter_cls(site_config=site_config, request_config=REQUEST_CONFIG, cookies=cookies)


def resolve_keywords(args: argparse.Namespace) -> list[str]:
    return args.keywords if args.keywords else [args.keyword]


def main() -> int:
    args = parse_args()
    site_config = get_site_config(args.site)
    keywords = resolve_keywords(args)

    cookies = get_cookies(
        cdp_url=args.cdp_url,
        target_url=site_config.cookie_url,
    )

    storage = JobStorage(STORAGE_CONFIG)
    total_fetched = 0
    total_saved = 0

    with build_adapter(
        args.site,
        list_url_template=args.list_url_template,
        cookies=cookies,
    ) as adapter:
        for index, keyword in enumerate(keywords, start=1):
            print(
                f"[{site_config.display_name}] keyword {index}/{len(keywords)} start: "
                f"keyword={keyword} city={args.city} pages={args.pages}"
            )
            keyword_jobs = []

            try:
                for page in range(1, args.pages + 1):
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page {page}/{args.pages} fetching"
                    )
                    response = adapter.fetch_page(
                        page=page,
                        keyword=keyword,
                        city=args.city,
                    )
                    jobs = adapter.parse(response, page=page)
                    keyword_jobs.extend(jobs)
                    print(
                        f"[{site_config.display_name}] keyword={keyword} "
                        f"page {page}/{args.pages} fetched_jobs={len(jobs)}"
                    )
            except BlockedException as exc:
                print(
                    f"[{site_config.display_name}] keyword={keyword} blocked: {exc}. "
                    "Stopping batch crawl."
                )
                return 2

            saved_count = storage.save_jobs(keyword_jobs)
            total_fetched += len(keyword_jobs)
            total_saved += saved_count
            print(
                f"[{site_config.display_name}] keyword={keyword} completed "
                f"fetched={len(keyword_jobs)} new_saved={saved_count} "
                f"csv={STORAGE_CONFIG.csv_path} sqlite={STORAGE_CONFIG.sqlite_path}"
            )

    print(
        f"[{site_config.display_name}] crawl completed keywords={len(keywords)} "
        f"total_fetched={total_fetched} total_new_saved={total_saved} "
        f"csv={STORAGE_CONFIG.csv_path} sqlite={STORAGE_CONFIG.sqlite_path}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Runtime error: {exc}")
        raise SystemExit(5)
    except BlockedResponseError as exc:
        print(f"Blocked: {exc}")
        raise SystemExit(2)
    except AdapterConfigError as exc:
        print(f"Config error: {exc}")
        raise SystemExit(3)
    except CrawlerError as exc:
        print(f"Crawler error: {exc}")
        raise SystemExit(4)
