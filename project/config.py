from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
CSV_PATH = DATA_DIR / "jobs.csv"
SQLITE_PATH = DATA_DIR / "jobs.db"

DEFAULT_SITE = "job51"
DEFAULT_PAGE_COUNT = 3

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
}

DEFAULT_BLOCK_KEYWORDS = (
    "验证码",
    "verify",
    "captcha",
)


@dataclass(frozen=True)
class BrowserConfig:
    cdp_url: str = "http://127.0.0.1:9222"


@dataclass(frozen=True)
class RequestConfig:
    timeout_seconds: float = 30.0
    min_sleep_seconds: float = 1.0
    max_sleep_seconds: float = 3.0


@dataclass(frozen=True)
class StorageConfig:
    csv_path: Path = CSV_PATH
    sqlite_path: Path = SQLITE_PATH


@dataclass(frozen=True)
class SiteConfig:
    name: str
    display_name: str
    base_url: str
    cookie_url: str
    list_url_template: str | None = None
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    extra_query_params: dict[str, str] = field(default_factory=dict)
    blocked_keywords: tuple[str, ...] = DEFAULT_BLOCK_KEYWORDS


BROWSER_CONFIG = BrowserConfig()
REQUEST_CONFIG = RequestConfig()
STORAGE_CONFIG = StorageConfig()

SITE_CONFIGS: dict[str, SiteConfig] = {
    "job51": SiteConfig(
        name="job51",
        display_name="前程无忧",
        base_url="https://www.51job.com",
        cookie_url="https://www.51job.com",
        # TODO: 填入已验证可用的 51job 职位列表接口模板，例如带 {page}/{keyword}/{city} 的 URL。
        list_url_template=os.getenv("JOB51_LIST_URL_TEMPLATE") or None,
        headers={
            **DEFAULT_HEADERS,
            "Referer": "https://www.51job.com/",
        },
    ),
    "boss": SiteConfig(
        name="boss",
        display_name="BOSS直聘",
        base_url="https://www.zhipin.com",
        cookie_url="https://www.zhipin.com",
        # TODO: 填入已验证可用的 BOSS 职位列表接口模板，例如带 {page}/{keyword}/{city} 的 URL。
        list_url_template=os.getenv("BOSS_LIST_URL_TEMPLATE") or None,
        headers={
            **DEFAULT_HEADERS,
            "Referer": "https://www.zhipin.com/",
        },
    ),
    "zhilian": SiteConfig(
        name="zhilian",
        display_name="智联招聘",
        base_url="https://www.zhaopin.com",
        cookie_url="https://www.zhaopin.com",
        # TODO: 填入已验证可用的智联职位列表接口模板，例如带 {page}/{keyword}/{city} 的 URL。
        list_url_template=os.getenv("ZHILIAN_LIST_URL_TEMPLATE") or None,
        headers={
            **DEFAULT_HEADERS,
            "Referer": "https://www.zhaopin.com/",
        },
    ),
}


def get_site_config(name: str) -> SiteConfig:
    try:
        return SITE_CONFIGS[name]
    except KeyError as exc:
        supported = ", ".join(sorted(SITE_CONFIGS))
        raise KeyError(f"Unsupported site '{name}'. Supported sites: {supported}") from exc
