from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urlparse

from common import DEFAULT_HEADERS, ROOT_DIR
from platforms.job51.we_search_client import build_search_params


SEARCH_PAGE_BASE_URL = "https://we.51job.com/pc/search"
SEARCH_PAGE_URL = f"{SEARCH_PAGE_BASE_URL}?jobArea=000000"
EDGE_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]


def _ensure_playwright_import() -> None:
    vendor_path = ROOT_DIR / "third_party" / "pydeps"
    if vendor_path.exists() and str(vendor_path) not in sys.path:
        sys.path.insert(0, str(vendor_path))


def discover_browser_path() -> str:
    for candidate in EDGE_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    raise RuntimeError("could not find Edge or Chrome executable on this machine")


@dataclass
class BrowserSearchClient:
    timeout: float = 30.0
    headless: bool = True
    browser_path: str | None = None
    ready_wait_ms: int = 2500
    request_timeout_ms: int = 20000
    max_retries: int = 3
    min_interval_seconds: float = 0.35
    user_data_dir: str | None = None
    allow_manual_verification: bool = False
    manual_verification_wait_ms: int = 180000
    cdp_url: str | None = None
    status_callback: Callable[[str, dict[str, Any]], None] | None = None

    def __post_init__(self) -> None:
        _ensure_playwright_import()
        from playwright.sync_api import sync_playwright

        self.browser_path = self.browser_path or discover_browser_path()
        self._playwright_manager = sync_playwright()
        self._playwright = self._playwright_manager.start()
        self._browser = None
        self._attached_via_cdp = False
        if self.cdp_url:
            self._browser = self._playwright.chromium.connect_over_cdp(self.cdp_url)
            self._attached_via_cdp = True
            if not self._browser.contexts:
                raise RuntimeError(f"no browser contexts available at {self.cdp_url}")
            self._context = self._browser.contexts[0]
            self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
            self._last_request_at = 0.0
            self._prime()
            return

        launch_args = {
            "executable_path": self.browser_path,
            "headless": self.headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        context_args = {
            "locale": "zh-CN",
            "ignore_https_errors": True,
            "user_agent": DEFAULT_HEADERS["User-Agent"],
            "extra_http_headers": {"Accept-Language": DEFAULT_HEADERS["Accept-Language"]},
        }
        if self.user_data_dir:
            profile_dir = Path(self.user_data_dir)
            if not profile_dir.is_absolute():
                profile_dir = ROOT_DIR / profile_dir
            profile_dir.mkdir(parents=True, exist_ok=True)
            self._context = self._playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                **launch_args,
                **context_args,
            )
        else:
            self._browser = self._playwright.chromium.launch(**launch_args)
            self._context = self._browser.new_context(**context_args)
        self._context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            });
            """
        )
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        self._last_request_at = 0.0
        self._prime()

    def _prime(self) -> None:
        self._page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
        self._page.wait_for_timeout(self.ready_wait_ms)

    def close(self) -> None:
        try:
            if not self._attached_via_cdp:
                self._context.close()
        finally:
            try:
                if self._browser is not None:
                    self._browser.close()
            finally:
                self._playwright.stop()

    def _reprime(self) -> None:
        try:
            self._page.close()
        except Exception:
            pass
        self._page = self._context.new_page()
        self._prime()

    def _respect_rate_limit(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        delay = self.min_interval_seconds - (time.time() - self._last_request_at)
        if delay > 0:
            time.sleep(delay)

    def _wait_for_manual_verification(self, search_url: str) -> bool:
        if not self.allow_manual_verification or (self.headless and not self._attached_via_cdp):
            return False
        wait_seconds = self.manual_verification_wait_ms // 1000
        if callable(self.status_callback):
            self.status_callback(
                "manual_verification_waiting",
                {
                    "search_url": search_url,
                    "wait_seconds": wait_seconds,
                },
            )
        print(
            "51job requires manual intervention in the opened browser window. "
            f"Waiting up to {wait_seconds} seconds...",
            flush=True,
        )
        try:
            self._page.bring_to_front()
        except Exception:
            pass
        self._page.goto(search_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
        self._page.wait_for_timeout(self.manual_verification_wait_ms)
        self._reprime()
        if callable(self.status_callback):
            self.status_callback(
                "manual_verification_resumed",
                {
                    "search_url": search_url,
                },
            )
        return True

    def _matches_search_response(self, response: Any, params: dict[str, str]) -> bool:
        parsed = urlparse(response.url)
        if parsed.path != "/api/job/search-pc":
            return False
        query = parse_qs(parsed.query)
        for key in ("keyword", "function", "industry", "jobArea", "pageNum", "pageSize"):
            expected = params.get(key, "")
            actual = query.get(key, [""])[0]
            if actual != expected:
                return False
        return True

    def _request_json(self, params: dict[str, str]) -> dict[str, Any]:
        last_error = ""
        blocked = False
        search_url = f"{SEARCH_PAGE_BASE_URL}?{urlencode(params)}"
        query = urlencode(
            {
                "api_key": "51job",
                "timestamp": str(int(time.time())),
                **params,
            }
        )
        api_path = f"/api/job/search-pc?{query}"
        for attempt in range(1, self.max_retries + 1):
            self._respect_rate_limit()
            self._last_request_at = time.time()
            try:
                if self._attached_via_cdp:
                    result = self._page.evaluate(
                        """
                        async ({ apiPath, timeoutMs }) => {
                          const controller = new AbortController();
                          const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
                          try {
                            const response = await fetch(apiPath, {
                              credentials: "include",
                              signal: controller.signal,
                              headers: {
                                "Accept": "application/json, text/plain, */*"
                              }
                            });
                            const text = await response.text();
                            return {
                              status: response.status,
                              contentType: response.headers.get("content-type") || "",
                              text,
                              error: ""
                            };
                          } catch (error) {
                            return {
                              status: 0,
                              contentType: "",
                              text: "",
                              error: String(error)
                            };
                          } finally {
                            clearTimeout(timer);
                          }
                        }
                        """,
                        {"apiPath": api_path, "timeoutMs": self.request_timeout_ms},
                    )
                    if "json" in result.get("contentType", "").lower():
                        return json.loads(result["text"])
                    response_text = result.get("text", "")
                    last_error = (
                        f"status={result.get('status')} contentType={result.get('contentType')} "
                        f"error={result.get('error')} pageUrl={self._page.url} "
                        f"sample={response_text[:200]!r}"
                    )
                    blocked = "滑动验证页面" in response_text or "<title>405" in response_text.lower()
                    if (
                        blocked
                        and self._wait_for_manual_verification(search_url)
                    ):
                        continue
                    raise RuntimeError(last_error)

                with self._page.expect_response(
                    lambda response: self._matches_search_response(response, params),
                    timeout=self.request_timeout_ms,
                ) as response_info:
                    self._page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=int(self.timeout * 1000),
                    )
                response = response_info.value
                text = response.text()
                content_type = response.headers.get("content-type", "")
                if "json" in content_type.lower():
                    return json.loads(text)
                last_error = (
                    f"status={response.status} contentType={content_type} "
                    f"pageUrl={self._page.url} sample={text[:200]!r}"
                )
                blocked = "滑动验证页面" in text or "<title>405" in text.lower()
                if blocked and self._wait_for_manual_verification(search_url):
                    continue
            except Exception as exc:
                last_error = repr(exc)

            if attempt < self.max_retries:
                time.sleep(min(6.0 if blocked else 1.2 * attempt, 10.0))
                self._reprime()

        raise RuntimeError(f"browser request did not return JSON after retries: {last_error}")

    def search_jobs(
        self,
        *,
        keyword: str = "",
        function_code: str = "",
        industry_code: str = "",
        job_area: str = "000000",
        page_num: int = 1,
        page_size: int = 100,
    ) -> dict[str, Any]:
        params = build_search_params(
            keyword=keyword,
            function_code=function_code,
            industry_code=industry_code,
            job_area=job_area,
            page_num=page_num,
            page_size=page_size,
        )
        payload = self._request_json(params)
        if str(payload.get("status")) != "1":
            raise RuntimeError(f"browser search failed: {json.dumps(payload, ensure_ascii=False)}")
        return payload

    def get_job_page(
        self,
        *,
        keyword: str = "",
        function_code: str = "",
        industry_code: str = "",
        job_area: str = "000000",
        page_num: int = 1,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        payload = self.search_jobs(
            keyword=keyword,
            function_code=function_code,
            industry_code=industry_code,
            job_area=job_area,
            page_num=page_num,
            page_size=page_size,
        )
        job_block = payload.get("resultbody", {}).get("job", {}) or {}
        items = job_block.get("items", []) or []
        total_count = int(job_block.get("totalCount") or job_block.get("totalcount") or 0)
        return items, total_count
