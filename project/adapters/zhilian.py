from __future__ import annotations

import httpx

from adapters.base import BaseAdapter, JobRecord


class ZhilianAdapter(BaseAdapter):
    def parse(self, response: httpx.Response, *, page: int) -> list[JobRecord]:
        # TODO: 确认智联招聘列表接口后，在这里补充真实字段映射。
        return self.parse_json_jobs(response, page=page)
