from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import requests

from common import DEFAULT_HEADERS, clean_text


WAF_XOR_KEY = "3000176000856006061501533003690027800375"
SEARCH_BASE_URL = "https://we.51job.com/api/job/search-pc"


def _unsbox_waf_arg(arg: str) -> str:
    order = [
        15,
        35,
        29,
        24,
        33,
        16,
        1,
        38,
        10,
        9,
        19,
        31,
        40,
        27,
        22,
        23,
        25,
        13,
        6,
        11,
        39,
        18,
        20,
        8,
        14,
        21,
        32,
        26,
        2,
        30,
        7,
        4,
        17,
        5,
        3,
        28,
        34,
        37,
        12,
        36,
    ]
    output = [""] * len(order)
    for index, ch in enumerate(arg):
        for target_index, target in enumerate(order):
            if target == index + 1:
                output[target_index] = ch
                break
    return "".join(output)


def _xor_hex(left: str, right: str) -> str:
    length = min(len(left), len(right))
    return "".join(
        f"{int(left[i : i + 2], 16) ^ int(right[i : i + 2], 16):02x}"
        for i in range(0, length, 2)
    )


def build_search_params(
    *,
    keyword: str = "",
    function_code: str = "",
    industry_code: str = "",
    job_area: str = "000000",
    page_num: int = 1,
    page_size: int = 100,
) -> dict[str, str]:
    return {
        "keyword": clean_text(keyword),
        "searchType": "2",
        "function": clean_text(function_code),
        "industry": clean_text(industry_code),
        "jobArea": clean_text(job_area) or "000000",
        "jobArea2": "",
        "landmark": "",
        "metro": "",
        "salary": "",
        "workYear": "",
        "degree": "",
        "companyType": "",
        "companySize": "",
        "jobType": "",
        "issueDate": "",
        "sortType": "0",
        "pageNum": str(max(page_num, 1)),
        "requestId": "",
        "pageSize": str(max(page_size, 1)),
        "source": "1",
        "accountId": "",
        "pageCode": "sou|sou|soulb",
        "scene": "7",
    }


@dataclass
class SearchClient:
    session: requests.Session = field(default_factory=requests.Session)
    timeout: float = 30.0
    referer: str = "https://we.51job.com/pc/search"
    max_retries: int = 4

    def __post_init__(self) -> None:
        self.session.trust_env = False
        headers = dict(DEFAULT_HEADERS)
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("Referer", self.referer)
        self.session.headers.update(headers)

    def _reset_session(self) -> None:
        headers = dict(self.session.headers)
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(headers)

    def _request_json(self, url: str) -> dict[str, Any]:
        last_error = ""
        for attempt in range(1, self.max_retries + 1):
            response = self.session.get(url, timeout=self.timeout)
            content_type = response.headers.get("content-type", "")
            if content_type.startswith("application/json"):
                return response.json()

            match = re.search(r"arg1='([0-9A-F]+)'", response.text)
            if match:
                waf_cookie = _xor_hex(_unsbox_waf_arg(match.group(1)), WAF_XOR_KEY)
                self.session.cookies.set("acw_sc__v2", waf_cookie, domain="we.51job.com", path="/")
                response = self.session.get(url, timeout=self.timeout)
                if response.headers.get("content-type", "").startswith("application/json"):
                    return response.json()
                last_error = response.text[:200]
            else:
                last_error = response.text[:200]

            if attempt < self.max_retries:
                self._reset_session()
                time.sleep(min(0.5 * attempt, 2.0))

        response.raise_for_status()
        raise RuntimeError(f"51job WAF challenge page did not expose arg1; sample={last_error!r}")

    def search_jobs(
        self,
        *,
        keyword: str = "",
        function_code: str = "",
        industry_code: str = "",
        job_area: str = "000000",
        page_num: int = 1,
        page_size: int = 100,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        if timestamp is None:
            timestamp = int(time.time())
        params = build_search_params(
            keyword=keyword,
            function_code=function_code,
            industry_code=industry_code,
            job_area=job_area,
            page_num=page_num,
            page_size=page_size,
        )
        url = f"{SEARCH_BASE_URL}?api_key=51job&timestamp={timestamp}&{urlencode(params)}"
        payload = self._request_json(url)
        if str(payload.get("status")) != "1":
            raise RuntimeError(f"51job search failed: {json.dumps(payload, ensure_ascii=False)}")
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
