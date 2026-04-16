from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import requests


COAPI_BASE_URL = "https://coapi.51job.com"
COAPI_SIGNING_KEY = (
    "tuD&#mheJQBlgy&Sm300l8xK^X4NzFYBcrN8@YLCret$fv1AZbtujg*KN^$YnUkh"
)


def parse_jsonp_text(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    return json.loads(text)


def decode_response_text(response: requests.Response) -> str:
    return response.content.decode("utf-8", errors="replace")


@dataclass
class CoapiClient:
    session: requests.Session
    keyindex: int = 1
    timeout: float = 30.0

    def _build_signed_params(self, payload: dict[str, Any]) -> dict[str, str | int]:
        params_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        secret = COAPI_SIGNING_KEY[self.keyindex : self.keyindex + 15]
        sign = hashlib.md5(("coapi" + params_json + secret).encode("utf-8")).hexdigest()
        return {
            "key": self.keyindex,
            "sign": sign,
            "params": params_json,
        }

    def _get(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{COAPI_BASE_URL}/{endpoint.lstrip('/')}"
        response = self.session.get(
            url,
            params={**self._build_signed_params(payload), "jsoncallback": ""},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return parse_jsonp_text(decode_response_text(response))

    def get_job_list(
        self,
        ctmid: str,
        *,
        pagenum: int = 1,
        pagesize: int = 100,
        keyword: str = "",
        coid: str = "",
        functype: str = "",
        jobarea: str = "",
    ) -> dict[str, Any]:
        return self._get(
            "job_list.php",
            {
                "ctmid": ctmid,
                "pagesize": pagesize,
                "pagenum": pagenum,
                "keyword": keyword,
                "coid": coid,
                "functype": functype,
                "jobarea": jobarea,
            },
        )

    def get_job_detail(self, jobid: str | int) -> dict[str, Any]:
        return self._get("job_detail.php", {"jobid": str(jobid)})
