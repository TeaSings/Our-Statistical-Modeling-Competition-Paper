from __future__ import annotations

import json
import random
import time
import uuid
from typing import Any
from urllib.parse import urljoin

import httpx

from adapters.base import (
    AdapterConfigError,
    BaseAdapter,
    BlockedResponseError,
    CrawlerError,
    JobRecord,
    ParseResponseError,
)


class BlockedException(BlockedResponseError):
    """Raised when 51job returns a verification or blocked response."""


class Job51Adapter(BaseAdapter):
    SEARCH_URL = "https://we.51job.com/api/job/search-pc"
    REFERER = "https://we.51job.com/pc/search"
    PAGE_SIZE = 20

    def build_request(
        self,
        page: int,
        *,
        keyword: str | None = None,
        city: str | None = None,
    ) -> httpx.Request:
        job_area = str(city or "").strip()
        if not job_area:
            raise AdapterConfigError(
                "51job requires a jobArea code. Pass it via the existing `city` argument, "
                "for example `--city 020000`."
            )

        user_agent = (
            self.client.headers.get("User-Agent")
            or self.site_config.headers.get("User-Agent")
            or "Mozilla/5.0"
        )
        headers = {
            "User-Agent": user_agent,
            "Referer": self.REFERER,
            "From-Domain": "51job_web",
        }
        params = {
            "api_key": "51job",
            "keyword": keyword or "",
            "jobArea": job_area,
            "pageNum": str(page),
            "pageSize": str(self.PAGE_SIZE),
            "searchType": "2",
            "sortType": "0",
            "pageCode": "sou|sou|soulb",
            "scene": "7",
            "timestamp": str(int(time.time() * 1000)),
            "requestId": uuid.uuid4().hex,
        }
        return self.client.build_request(
            "GET",
            self.SEARCH_URL,
            params=params,
            headers=headers,
        )

    def fetch_page(
        self,
        page: int,
        *,
        keyword: str | None = None,
        city: str | None = None,
    ) -> httpx.Response:
        delay = random.uniform(
            self.request_config.min_sleep_seconds,
            self.request_config.max_sleep_seconds,
        )
        time.sleep(delay)

        request = self.build_request(page=page, keyword=keyword, city=city)
        try:
            response = self.client.send(request)
        except httpx.RequestError as exc:
            raise CrawlerError(
                f"{self.site_config.display_name} request failed for page {page}: {exc}"
            ) from exc

        block_reason = self._block_reason(response)
        if block_reason:
            raise BlockedException(
                f"{self.site_config.display_name} page {page} appears blocked: {block_reason}"
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise CrawlerError(
                f"{self.site_config.display_name} page {page} returned HTTP {response.status_code}"
            ) from exc
        return response

    def is_blocked(self, response: httpx.Response) -> bool:
        return self._block_reason(response) is not None

    def parse_jobs(self, response: httpx.Response) -> list[dict[str, str]]:
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            snippet = response.text[:200].strip().replace("\n", " ")
            raise ParseResponseError(
                f"{self.site_config.display_name} did not return valid JSON. Response starts with: {snippet}"
            ) from exc

        print("DEBUG JSON KEYS:", list(data.keys()))
        if isinstance(data.get("resultbody"), dict):
            print("DEBUG resultbody KEYS:", list(data.get("resultbody", {}).keys()))

        job_list = None

        if isinstance(data.get("resultbody"), dict):
            resultbody = data["resultbody"]
            if isinstance(resultbody.get("job"), dict):
                job_list = resultbody["job"].get("items")
            if job_list is None:
                job_list = resultbody.get("jobList")

        if job_list is None and "jobList" in data:
            job_list = data["jobList"]

        if job_list is None and "list" in data:
            job_list = data["list"]

        if job_list is None and isinstance(data.get("data"), dict):
            job_list = data["data"].get("jobList") or data["data"].get("list")

        if not job_list:
            raise ParseResponseError(
                f"Cannot find job list. Keys: {list(data.keys())}, preview: {str(data)[:1000]}"
            )

        parsed_jobs: list[dict[str, str]] = []
        for item in job_list:
            if not isinstance(item, dict):
                continue

            job_name = self._get_text(item, "jobName")
            company_name = (
                self._get_text(item, "companyName")
                or self._get_text(item, "fullCompanyName")
                or self._get_text(item, "company")
                or self._get_nested_text(item, "companyDetail.companyName")
            )
            salary = self._get_text(item, "provideSalaryString") or self._get_text(item, "salaryDesc")
            location = self._get_text(item, "jobAreaString")
            job_url = (
                self._get_text(item, "jobHref")
                or self._get_text(item, "jobUrl")
                or self._get_text(item, "href")
            )
            if not job_name or not job_url:
                continue

            parsed_jobs.append(
                {
                    "job_name": job_name,
                    "company_name": company_name,
                    "salary": salary,
                    "location": location,
                    "experience": self._get_text(item, "workYearString"),
                    "education": self._get_text(item, "degreeString"),
                    "publish_time": self._get_text(item, "issueDateString"),
                    "raw_tags": self._normalize_tags(item.get("jobTags") or item.get("tags")),
                    "job_url": job_url,
                    "raw_payload": json.dumps(item, ensure_ascii=False, sort_keys=True),
                }
            )

        if job_list and not parsed_jobs:
            first_item = next((item for item in job_list if isinstance(item, dict)), None)
            if isinstance(first_item, dict):
                print("DEBUG first item KEYS:", list(first_item.keys()))
            else:
                print("DEBUG first item:", first_item)

        return parsed_jobs

    def parse(self, response: httpx.Response, *, page: int) -> list[JobRecord]:
        parsed_jobs = self.parse_jobs(response)
        return [
            JobRecord(
                site=self.site_config.name,
                page=page,
                job_url=urljoin(self.site_config.base_url, job["job_url"]),
                job_title=job["job_name"],
                company_name=job["company_name"],
                city=job["location"],
                salary=job["salary"],
                summary="",
                raw_payload=job.get("raw_payload") or json.dumps(job, ensure_ascii=False, sort_keys=True),
            )
            for job in parsed_jobs
        ]

    def _block_reason(self, response: httpx.Response) -> str | None:
        content_type = response.headers.get("content-type", "").lower()
        response_text = response.text[:4000]

        if "text/html" in content_type:
            return "received HTML instead of JSON"

        for marker in ("滑动滑块", "访问验证", "TraceID"):
            if marker in response_text:
                return f"response contains blocked marker '{marker}'"

        return None

    def _get_text(self, item: dict[str, Any], key: str) -> str:
        value = item.get(key)
        if value is None:
            return ""
        return str(value).strip()

    def _get_nested_text(self, item: dict[str, Any], path: str) -> str:
        current: Any = item
        for key in path.split("."):
            if not isinstance(current, dict):
                return ""
            current = current.get(key)
            if current is None:
                return ""
        return str(current).strip()

    def _normalize_tags(self, raw_value: Any) -> str:
        if raw_value is None:
            return ""
        if isinstance(raw_value, str):
            return raw_value.strip()
        if isinstance(raw_value, list):
            values: list[str] = []
            for item in raw_value:
                if isinstance(item, str):
                    text = item.strip()
                elif isinstance(item, dict):
                    text = (
                        self._get_text(item, "name")
                        or self._get_text(item, "tag")
                        or self._get_text(item, "label")
                    )
                else:
                    text = str(item).strip()
                if text:
                    values.append(text)
            return ",".join(values)
        return str(raw_value).strip()
