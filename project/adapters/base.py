from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urljoin

import httpx

from config import RequestConfig, SiteConfig


DEFAULT_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    # TODO: 各站点真实字段确认后，应在子类中覆盖或补充这些候选键。
    "job_url": (
        "job_url",
        "jobUrl",
        "detail_url",
        "detailUrl",
        "position_url",
        "positionUrl",
        "url",
    ),
    "job_title": (
        "job_title",
        "jobTitle",
        "title",
        "name",
        "position_name",
        "positionName",
    ),
    "company_name": (
        "company_name",
        "companyName",
        "company",
        "brandName",
    ),
    "city": (
        "city",
        "cityName",
        "jobArea",
        "jobCity",
        "location",
    ),
    "salary": (
        "salary",
        "salaryDesc",
        "salaryName",
        "salaryText",
        "providesalary_text",
    ),
    "summary": (
        "summary",
        "jobSummary",
        "short_desc",
        "shortDesc",
        "description",
    ),
}


class CrawlerError(Exception):
    """Base crawler error."""


class AdapterConfigError(CrawlerError):
    """Raised when adapter config is incomplete."""


class BlockedResponseError(CrawlerError):
    """Raised when response indicates bot defense or verification."""


class ParseResponseError(CrawlerError):
    """Raised when response payload cannot be parsed into jobs."""


@dataclass(slots=True)
class JobRecord:
    site: str
    page: int
    job_url: str
    job_title: str
    company_name: str = ""
    city: str = ""
    salary: str = ""
    summary: str = ""
    raw_payload: str = ""

    def to_row(self) -> dict[str, Any]:
        return {
            "site": self.site,
            "page": self.page,
            "job_url": self.job_url,
            "job_title": self.job_title,
            "company_name": self.company_name,
            "city": self.city,
            "salary": self.salary,
            "summary": self.summary,
            "raw_payload": self.raw_payload,
        }


class BaseAdapter:
    def __init__(
        self,
        site_config: SiteConfig,
        request_config: RequestConfig,
        cookies: dict[str, str] | None = None,
    ) -> None:
        self.site_config = site_config
        self.request_config = request_config
        self.cookies = cookies or {}
        self.client = httpx.Client(
            headers=site_config.headers,
            cookies=self.cookies,
            timeout=request_config.timeout_seconds,
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "BaseAdapter":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def fetch_page(
        self,
        page: int,
        *,
        keyword: str | None = None,
        city: str | None = None,
    ) -> httpx.Response:
        if not self.site_config.list_url_template:
            raise AdapterConfigError(
                f"{self.site_config.display_name} list_url_template is not configured. "
                "TODO: fill the verified list API URL in project/config.py or pass --list-url-template."
            )

        delay = random.uniform(
            self.request_config.min_sleep_seconds,
            self.request_config.max_sleep_seconds,
        )
        time.sleep(delay)

        url = self.site_config.list_url_template.format(
            page=page,
            keyword=quote_plus(keyword or ""),
            city=quote_plus(city or ""),
        )
        params = self._build_query_params(page=page, keyword=keyword, city=city)
        try:
            response = self.client.request(
                self.site_config.method.upper(),
                url,
                params=params or None,
            )
        except httpx.RequestError as exc:
            raise CrawlerError(
                f"{self.site_config.display_name} request failed for page {page}: {exc}"
            ) from exc

        block_reason = self._block_reason(response)
        if block_reason:
            raise BlockedResponseError(
                f"{self.site_config.display_name} page {page} appears blocked: {block_reason}"
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise CrawlerError(
                f"{self.site_config.display_name} page {page} returned HTTP {response.status_code}"
            ) from exc
        return response

    def parse(self, response: httpx.Response, *, page: int) -> list[JobRecord]:
        raise NotImplementedError

    def is_blocked(self, response: httpx.Response) -> bool:
        return self._block_reason(response) is not None

    def parse_json_jobs(self, response: httpx.Response, *, page: int) -> list[JobRecord]:
        payload = self._load_json_payload(response)
        items = self._extract_candidate_items(payload)
        jobs: list[JobRecord] = []

        for item in items:
            if not isinstance(item, dict):
                continue
            record = self._build_job_record(item=item, page=page)
            if record is not None:
                jobs.append(record)

        if not jobs:
            raise ParseResponseError(
                f"{self.site_config.display_name} page {page} returned JSON, but no jobs could be mapped. "
                "TODO: verify the list schema and update the adapter field mapping."
            )

        return jobs

    def field_candidates(self) -> dict[str, tuple[str, ...]]:
        return DEFAULT_FIELD_CANDIDATES

    def _build_query_params(
        self,
        *,
        page: int,
        keyword: str | None,
        city: str | None,
    ) -> dict[str, str]:
        params: dict[str, str] = {}
        for key, raw_value in self.site_config.extra_query_params.items():
            params[key] = raw_value.format(
                page=page,
                keyword=keyword or "",
                city=city or "",
            )
        return params

    def _load_json_payload(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            snippet = response.text[:200].strip().replace("\n", " ")
            raise ParseResponseError(
                f"{self.site_config.display_name} did not return valid JSON. Response starts with: {snippet}"
            ) from exc

    def _extract_candidate_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            direct_candidates = (
                "jobs",
                "list",
                "items",
                "results",
                "data",
                "rows",
            )
            for key in direct_candidates:
                value = payload.get(key)
                if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
                    return value
                if isinstance(value, dict):
                    nested = self._extract_candidate_items(value)
                    if nested:
                        return nested

            for value in payload.values():
                nested = self._extract_candidate_items(value)
                if nested:
                    return nested

        return []

    def _build_job_record(self, *, item: dict[str, Any], page: int) -> JobRecord | None:
        field_map = self.field_candidates()
        job_url = self._pick_first(item, field_map["job_url"])
        job_title = self._pick_first(item, field_map["job_title"])

        if not job_url or not job_title:
            return None

        company_name = self._pick_first(item, field_map["company_name"])
        city = self._pick_first(item, field_map["city"])
        salary = self._pick_first(item, field_map["salary"])
        summary = self._pick_first(item, field_map["summary"])

        return JobRecord(
            site=self.site_config.name,
            page=page,
            job_url=urljoin(self.site_config.base_url, job_url),
            job_title=str(job_title).strip(),
            company_name=str(company_name or "").strip(),
            city=str(city or "").strip(),
            salary=str(salary or "").strip(),
            summary=str(summary or "").strip(),
            raw_payload=json.dumps(item, ensure_ascii=False, sort_keys=True),
        )

    def _pick_first(self, item: dict[str, Any], candidates: tuple[str, ...]) -> Any:
        for key in candidates:
            if key in item and item[key] not in (None, ""):
                return item[key]
        return None

    def _block_reason(self, response: httpx.Response) -> str | None:
        content_type = response.headers.get("content-type", "").lower()
        response_text = response.text[:4000].lower()

        if "text/html" in content_type:
            return "received HTML instead of JSON"

        for keyword in self.site_config.blocked_keywords:
            if keyword.lower() in response_text:
                return f"response contains blocked keyword '{keyword}'"

        return None
