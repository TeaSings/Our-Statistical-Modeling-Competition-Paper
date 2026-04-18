from __future__ import annotations

from urllib.parse import urlparse


def _domain_matches(cookie_domain: str, target_host: str) -> bool:
    normalized_cookie_domain = cookie_domain.lstrip(".").lower()
    normalized_target_host = target_host.lower()
    return (
        normalized_cookie_domain == normalized_target_host
        or normalized_target_host.endswith(f".{normalized_cookie_domain}")
    )


def get_cookies(cdp_url: str, target_url: str | None = None) -> dict[str, str]:
    """Connect to an existing Edge instance via CDP and return cookies.

    TODO:
    - 启动 Edge 时需要带上 `--remote-debugging-port=9222`
    - 如果目标站点需要登录或人工过验证，需要先在 Edge 中手动完成
    """

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Run `pip install -r requirements.txt` first."
        ) from exc

    target_host = urlparse(target_url).hostname if target_url else None
    cookies_by_name: dict[str, str] = {}

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
        contexts = browser.contexts
        if not contexts:
            raise RuntimeError(
                "No Edge contexts found. Start Edge with remote debugging and keep at least one tab open."
            )

        for context in contexts:
            if target_url:
                try:
                    context_cookies = context.cookies([target_url])
                except Exception:
                    context_cookies = context.cookies()
            else:
                context_cookies = context.cookies()

            for cookie in context_cookies:
                name = cookie.get("name")
                value = cookie.get("value")
                domain = cookie.get("domain", "")
                if not name or value is None:
                    continue
                if target_host and domain and not _domain_matches(domain, target_host):
                    continue
                cookies_by_name[name] = value

    if target_url and not cookies_by_name:
        raise RuntimeError(
            f"No cookies found for {target_url}. Open the site in Edge and complete any manual steps first."
        )

    return cookies_by_name
