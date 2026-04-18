from __future__ import annotations

import json
import socket
import subprocess
import sys
import threading
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


class ManualVerificationCoordinator:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active = False
        self._owner = ""
        self._search_url = ""
        self._wait_seconds = 0
        self._pause_count = 0
        self._last_started_at = 0.0
        self._last_resumed_at = 0.0

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> dict[str, Any]:
        return {
            "manual_verification_active": self._active,
            "owner": self._owner,
            "search_url": self._search_url,
            "wait_seconds": self._wait_seconds,
            "pause_count": self._pause_count,
            "started_at": self._last_started_at,
            "last_resumed_at": self._last_resumed_at,
        }

    def wait_if_paused(self, worker_label: str) -> None:
        with self._condition:
            while self._active and self._owner != worker_label:
                self._condition.wait(timeout=0.5)

    def begin(
        self,
        *,
        worker_label: str,
        search_url: str,
        wait_seconds: int,
        status_callback: Callable[[str, dict[str, Any]], None] | None,
    ) -> bool:
        with self._condition:
            if self._active:
                return self._owner == worker_label
            self._active = True
            self._owner = worker_label
            self._search_url = search_url
            self._wait_seconds = wait_seconds
            self._pause_count += 1
            self._last_started_at = time.time()
            payload = self._snapshot_unlocked()
            self._condition.notify_all()
        if callable(status_callback):
            status_callback("manual_verification_waiting", payload)
        return True

    def finish(
        self,
        *,
        worker_label: str,
        status_callback: Callable[[str, dict[str, Any]], None] | None,
    ) -> None:
        payload = None
        with self._condition:
            if not self._active or self._owner != worker_label:
                return
            self._active = False
            self._last_resumed_at = time.time()
            payload = self._snapshot_unlocked()
            self._owner = ""
            self._search_url = ""
            self._wait_seconds = 0
            self._condition.notify_all()
        if callable(status_callback):
            status_callback("manual_verification_resumed", payload or {})

    def wait_for_resume(self) -> None:
        with self._condition:
            while self._active:
                self._condition.wait(timeout=0.5)


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
    speed_profile: str = "balanced"
    max_effective_workers: int = 0
    startup_stagger_seconds: float = 0.0
    status_callback: Callable[[str, dict[str, Any]], None] | None = None
    worker_label: str = "browser-main"
    manual_verification_coordinator: ManualVerificationCoordinator | None = None
    reuse_existing_page_on_cdp: bool = True

    def __post_init__(self) -> None:
        _ensure_playwright_import()
        from playwright.sync_api import sync_playwright

        self.browser_path = self.browser_path or discover_browser_path()
        if self.allow_manual_verification and self.manual_verification_coordinator is None:
            self.manual_verification_coordinator = ManualVerificationCoordinator()
        self._playwright_manager = sync_playwright()
        self._playwright = self._playwright_manager.start()
        self._browser = None
        self._attached_via_cdp = False
        self._spawned_browser_process: subprocess.Popen[str] | None = None
        self._owns_browser_process = False
        self._owns_page = False
        self._startup_stagger_applied = False
        if self.cdp_url:
            self._browser = self._playwright.chromium.connect_over_cdp(self.cdp_url)
            self._attached_via_cdp = True
            if not self._browser.contexts:
                raise RuntimeError(f"no browser contexts available at {self.cdp_url}")
            self._context = self._browser.contexts[0]
            self._page = None
            if self.reuse_existing_page_on_cdp and self._context.pages:
                self._page = self._context.pages[0]
                self._prime()
            elif not self._attached_via_cdp:
                self._page = self._context.new_page()
                self._owns_page = True
            self._last_request_at = 0.0
            return

        # When manual verification is enabled, auto-start a real visible browser
        # and attach over CDP so the user no longer has to launch port 9222 manually.
        if self.allow_manual_verification:
            self._launch_auto_debug_browser()
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

    def _resolve_profile_dir(self, *, isolated_session: bool = False) -> Path:
        profile_dir = Path(self.user_data_dir or "data/runtime/51job/browser_profile_auto")
        if not profile_dir.is_absolute():
            profile_dir = ROOT_DIR / profile_dir
        # Keep the auto-started browser on a dedicated sub-profile so it does not
        # conflict with other normal Edge / Chrome sessions. When this stable
        # profile is already occupied by an existing browser process, we can
        # fall back to a one-off isolated session so CDP startup still succeeds.
        profile_dir = profile_dir / "manual_verify_cdp"
        if isolated_session:
            session_name = f"session_{int(time.time() * 1000)}"
            profile_dir = profile_dir / session_name
        profile_dir.mkdir(parents=True, exist_ok=True)
        return profile_dir

    def _pick_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _connect_over_cdp_with_retry(self, cdp_url: str, timeout_seconds: float = 20.0) -> Any:
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            if self._spawned_browser_process is not None and self._spawned_browser_process.poll() is not None:
                raise RuntimeError(
                    "auto-started browser exited before CDP became ready; "
                    f"return_code={self._spawned_browser_process.returncode}"
                )
            try:
                browser = self._playwright.chromium.connect_over_cdp(cdp_url)
                if browser.contexts:
                    return browser
                last_error = f"no browser contexts available at {cdp_url}"
                try:
                    browser.close()
                except Exception:
                    pass
            except Exception as exc:
                last_error = repr(exc)
            time.sleep(0.5)
        raise RuntimeError(f"timed out waiting for CDP browser at {cdp_url}: {last_error}")

    def _launch_auto_debug_browser(self) -> None:
        last_error: Exception | None = None
        for isolated_session in (False, True):
            profile_dir = self._resolve_profile_dir(isolated_session=isolated_session)
            port = self._pick_free_port()
            cdp_url = f"http://127.0.0.1:{port}"
            launch_args = [
                self.browser_path,
                f"--user-data-dir={profile_dir}",
                f"--remote-debugging-port={port}",
                "--no-first-run",
                "--disable-blink-features=AutomationControlled",
                SEARCH_PAGE_URL,
            ]
            print(
                "Auto-starting a local browser for 51job manual verification: "
                f"{cdp_url}",
                flush=True,
            )
            self._spawned_browser_process = subprocess.Popen(
                launch_args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            try:
                self._browser = self._connect_over_cdp_with_retry(cdp_url)
                self._attached_via_cdp = True
                self._owns_browser_process = True
                self.cdp_url = cdp_url
                self._context = self._browser.contexts[0]
                self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
                self._owns_page = False
                self._last_request_at = 0.0
                self._prime()
                return
            except Exception as exc:
                last_error = exc
                if self._spawned_browser_process is not None and self._spawned_browser_process.poll() is None:
                    try:
                        self._spawned_browser_process.terminate()
                        self._spawned_browser_process.wait(timeout=5)
                    except Exception:
                        try:
                            self._spawned_browser_process.kill()
                        except Exception:
                            pass
                self._spawned_browser_process = None
                self._browser = None
                if not isolated_session:
                    print(
                        "Auto-started browser did not stay attached on the shared profile; "
                        "retrying with an isolated session profile.",
                        flush=True,
                    )
                    continue
                raise
        if last_error is not None:
            raise last_error

    def _prime(self) -> None:
        page = self._ensure_page()
        timeout_ms = int(self.timeout * 1000)
        try:
            page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception:
            # Shared CDP pages occasionally stall on DOMContentLoaded even though the
            # navigation has already committed and the 51job origin is ready for API fetches.
            fallback_timeout_ms = max(min(timeout_ms // 2, 15000), 5000)
            page.goto(SEARCH_PAGE_URL, wait_until="commit", timeout=fallback_timeout_ms)
        page.wait_for_timeout(self.ready_wait_ms)

    def _ensure_page(self) -> Any:
        if getattr(self, "_page", None) is None:
            self._page = self._context.new_page()
            self._owns_page = True
        return self._page

    def supports_parallel_workers(self) -> bool:
        return True

    def derive_worker_profile_dir(self, worker_index: int) -> str | None:
        if not self.user_data_dir:
            return None
        base = Path(self.user_data_dir)
        worker_suffix = max(int(worker_index), 0) + 1
        return str(base.with_name(f"{base.name}_worker_{worker_suffix:02d}"))

    def derive_worker_startup_stagger(self, worker_index: int) -> float:
        ordinal = max(int(worker_index), 0) + 1
        interval = max(float(self.min_interval_seconds), 0.0)
        if interval <= 0:
            return min(0.08 * ordinal, 0.40)
        return min(max(interval * 0.35 * ordinal, 0.05 * ordinal), 1.50)

    def clone_for_parallel_worker(
        self,
        worker_index: int,
        *,
        headless: bool | None = None,
    ) -> "BrowserSearchClient":
        worker_label = f"browser-worker-{max(int(worker_index), 0) + 1:02d}"
        shared_cdp_url = self.cdp_url if self._attached_via_cdp else None
        return BrowserSearchClient(
            timeout=self.timeout,
            headless=self.headless if headless is None else headless,
            browser_path=self.browser_path,
            ready_wait_ms=self.ready_wait_ms,
            request_timeout_ms=self.request_timeout_ms,
            max_retries=self.max_retries,
            min_interval_seconds=self.min_interval_seconds,
            user_data_dir=None if shared_cdp_url else self.derive_worker_profile_dir(worker_index),
            allow_manual_verification=self.allow_manual_verification if shared_cdp_url else False,
            manual_verification_wait_ms=self.manual_verification_wait_ms,
            cdp_url=shared_cdp_url,
            speed_profile=self.speed_profile,
            max_effective_workers=self.max_effective_workers,
            startup_stagger_seconds=self.derive_worker_startup_stagger(worker_index),
            status_callback=self.status_callback,
            worker_label=worker_label,
            manual_verification_coordinator=self.manual_verification_coordinator if shared_cdp_url else None,
            reuse_existing_page_on_cdp=False,
        )

    def close(self) -> None:
        try:
            if self._attached_via_cdp:
                if self._owns_page:
                    try:
                        self._page.close()
                    except Exception:
                        pass
            else:
                self._context.close()
        finally:
            try:
                if self._browser is not None and (not self._attached_via_cdp or self._owns_browser_process):
                    self._browser.close()
            finally:
                try:
                    if self._owns_browser_process and self._spawned_browser_process is not None:
                        if self._spawned_browser_process.poll() is None:
                            self._spawned_browser_process.terminate()
                            try:
                                self._spawned_browser_process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                self._spawned_browser_process.kill()
                finally:
                    self._playwright.stop()

    def _reprime(self) -> None:
        try:
            if self._page is not None:
                self._page.close()
        except Exception:
            pass
        self._page = self._context.new_page()
        self._owns_page = True
        self._prime()

    def _respect_rate_limit(self) -> None:
        if not self._startup_stagger_applied:
            self._startup_stagger_applied = True
            if self.startup_stagger_seconds > 0:
                time.sleep(self.startup_stagger_seconds)
        if self.min_interval_seconds <= 0:
            return
        delay = self.min_interval_seconds - (time.time() - self._last_request_at)
        if delay > 0:
            time.sleep(delay)

    def _is_blocked_response_text(self, text: str) -> bool:
        lowered = text.lower()
        return "滑动验证页面" in text or "<title>405" in lowered

    def _wait_until_manual_verification_resolved(self, page: Any) -> bool:
        deadline = time.time() + max(self.manual_verification_wait_ms, 1000) / 1000.0
        while time.time() < deadline:
            try:
                current_url = str(getattr(page, "url", "") or "")
            except Exception:
                current_url = ""
            try:
                current_title = str(page.title() or "")
            except Exception:
                current_title = ""
            if "滑动验证" not in current_title and "405" not in current_title:
                if current_url.startswith("https://we.51job.com"):
                    try:
                        page.wait_for_timeout(max(min(self.ready_wait_ms, 1500), 300))
                    except Exception:
                        pass
                    return True
            try:
                page.wait_for_timeout(1000)
            except Exception:
                return False
        return False

    def _fetch_json_via_page(self, api_path: str) -> dict[str, Any]:
        page = self._ensure_page()
        current_url = ""
        try:
            current_url = str(getattr(page, "url", "") or "")
        except Exception:
            current_url = ""
        if not current_url.startswith("https://we.51job.com"):
            self._prime()
            page = self._ensure_page()
        return page.evaluate(
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

    def _wait_for_manual_verification(self, search_url: str) -> bool:
        if not self.allow_manual_verification or (self.headless and not self._attached_via_cdp):
            return False
        wait_seconds = self.manual_verification_wait_ms // 1000
        coordinator = self.manual_verification_coordinator
        if coordinator is not None:
            is_owner = coordinator.begin(
                worker_label=self.worker_label,
                search_url=search_url,
                wait_seconds=wait_seconds,
                status_callback=self.status_callback,
            )
            if not is_owner:
                coordinator.wait_for_resume()
                return True
        else:
            if callable(self.status_callback):
                self.status_callback(
                    "manual_verification_waiting",
                    {
                        "search_url": search_url,
                        "wait_seconds": wait_seconds,
                        "owner": self.worker_label,
                        "manual_verification_active": True,
                    },
                )
        print(
            "51job requires manual intervention in the opened browser window. "
            f"Waiting up to {wait_seconds} seconds...",
            flush=True,
        )
        page = self._ensure_page()
        try:
            try:
                page.bring_to_front()
            except Exception:
                pass
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            except Exception:
                fallback_timeout_ms = max(min(int(self.timeout * 1000) // 2, 15000), 5000)
                page.goto(search_url, wait_until="commit", timeout=fallback_timeout_ms)
            resolved = self._wait_until_manual_verification_resolved(page)
            if not self._attached_via_cdp:
                self._reprime()
            elif resolved:
                page.wait_for_timeout(max(min(self.ready_wait_ms, 1000), 250))
        finally:
            if coordinator is not None:
                coordinator.finish(
                    worker_label=self.worker_label,
                    status_callback=self.status_callback,
                )
            elif callable(self.status_callback):
                self.status_callback(
                    "manual_verification_resumed",
                    {
                        "search_url": search_url,
                        "owner": self.worker_label,
                        "manual_verification_active": False,
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
        api_url = f"https://we.51job.com{api_path}"
        for attempt in range(1, self.max_retries + 1):
            if self.manual_verification_coordinator is not None:
                self.manual_verification_coordinator.wait_if_paused(self.worker_label)
            self._respect_rate_limit()
            self._last_request_at = time.time()
            try:
                if self._attached_via_cdp:
                    response = self._context.request.get(
                        api_url,
                        timeout=self.request_timeout_ms,
                        headers={
                            "Accept": "application/json, text/plain, */*",
                            "Accept-Language": DEFAULT_HEADERS["Accept-Language"],
                            "Origin": "https://we.51job.com",
                            "Referer": search_url,
                            "User-Agent": DEFAULT_HEADERS["User-Agent"],
                        },
                    )
                    response_text = response.text()
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type.lower():
                        return json.loads(response_text)
                    last_error = (
                        f"status={response.status} contentType={content_type} "
                        f"worker={self.worker_label} apiUrl={api_url} "
                        f"sample={response_text[:200]!r}"
                    )
                    blocked = self._is_blocked_response_text(response_text)
                    if (
                        blocked
                        and self._wait_for_manual_verification(search_url)
                    ):
                        continue
                    page_result = self._fetch_json_via_page(api_path)
                    page_text = str(page_result.get("text", "") or "")
                    page_content_type = str(page_result.get("contentType", "") or "")
                    if "json" in page_content_type.lower():
                        return json.loads(page_text)
                    page_blocked = self._is_blocked_response_text(page_text)
                    if (
                        page_blocked
                        and self._wait_for_manual_verification(search_url)
                    ):
                        continue
                    blocked = blocked or page_blocked
                    last_error = (
                        f"context={last_error}; "
                        f"page_fetch=status={page_result.get('status')} "
                        f"contentType={page_content_type} "
                        f"error={page_result.get('error')} "
                        f"worker={self.worker_label} "
                        f"sample={page_text[:200]!r}"
                    )
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
                blocked = self._is_blocked_response_text(text)
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
