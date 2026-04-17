from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import (  # noqa: E402
    DEFAULT_HEADERS,
    ROOT_DIR,
    clean_text,
    configure_utf8_stdio,
    ensure_parent,
    portable_path,
    sha1_text,
    write_jsonl,
)
from platforms.job51.coapi import CoapiClient  # noqa: E402


PLATFORM_NAME = "51job_campus"
CTMID_PATTERNS = [
    re.compile(r"ctmid\s*[:=]\s*[\"']?(\d+)[\"']?", re.I),
    re.compile(r"Apply\.aspx\?(?:[^\"']*?)(?:CtmID|ctmid)=(\d+)", re.I),
]
JOBID_PATTERNS = [
    re.compile(r"Apply\.aspx\?(?:[^\"']*?)(?:jobid|JobID)=(\d+)", re.I),
    re.compile(r"/(\d+)\.html(?:\?|$)", re.I),
]
INLINE_VAR_NAMES = ("jobData", "data", "jobsData", "postData")


@dataclass
class Blob:
    url: str
    text: str
    local_path: str | None = None


@dataclass
class PageBundle:
    seed_url: str
    page_blob: Blob
    blobs: list[Blob]
    soup: BeautifulSoup
    company_name: str
    page_title: str


@dataclass
class ProgressBar:
    seed_total: int
    started_at: float = field(default_factory=time.time)
    current_seed_index: int = 0
    current_label: str = ""
    seed_done: int = 0
    job_total: int = 0
    job_done: int = 0
    records_done: int = 0
    last_render_at: float = 0.0

    def set_seed(self, index: int, label: str) -> None:
        self.current_seed_index = index
        self.current_label = label
        self.render(force=True)

    def add_jobs(self, count: int) -> None:
        self.job_total += max(count, 0)
        self.render(force=True)

    def tick_jobs(self, count: int = 1) -> None:
        self.job_done += max(count, 0)
        self.records_done += max(count, 0)
        self.render()

    def finish_seed(self) -> None:
        self.seed_done += 1
        self.render(force=True)

    def render(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_render_at < 0.2:
            return
        self.last_render_at = now
        elapsed = max(now - self.started_at, 1e-6)
        progress_total = self.job_total if self.job_total > 0 else max(self.seed_total, 1)
        progress_done = self.job_done if self.job_total > 0 else self.seed_done
        ratio = min(max(progress_done / max(progress_total, 1), 0.0), 1.0)
        width = 28
        filled = int(width * ratio)
        bar = "[" + "#" * filled + "-" * (width - filled) + "]"
        rate = progress_done / elapsed
        eta_seconds: int | None = None
        if rate > 0 and progress_done < progress_total:
            eta_seconds = int((progress_total - progress_done) / rate)

        message = (
            f"\r{bar} {progress_done}/{progress_total} "
            f"| seeds {self.seed_done}/{self.seed_total} "
            f"| records {self.records_done} "
            f"| current {self.current_seed_index}/{self.seed_total}:{self.current_label[:42]} "
            f"| elapsed {format_seconds(int(elapsed))} "
            f"| ETA {format_seconds(eta_seconds) if eta_seconds is not None else '--'}"
        )
        print(message[:220], end="", flush=True)

    def newline(self) -> None:
        print("", flush=True)


def format_seconds(seconds: int | None) -> str:
    if seconds is None:
        return "--"
    if seconds <= 0:
        return "0s"
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{sec:02d}s"
    return f"{sec}s"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch verified JD records from 51job campus pages")
    parser.add_argument(
        "--seed-file",
        default="data/input/51job/campus_seed_urls.csv",
        help="CSV file with at least a seed_url column",
    )
    parser.add_argument(
        "--output-raw",
        default="data/raw/51job/records/51job_campus_jobs_raw.jsonl",
        help="Output JSONL with normalized raw job rows",
    )
    parser.add_argument(
        "--manifest",
        default="data/raw/51job/manifests/51job_campus_seed_manifest.jsonl",
        help="Per-seed processing manifest JSONL",
    )
    parser.add_argument(
        "--page-dir",
        default="data/raw/51job/html/pages",
        help="Directory for saving fetched seed page HTML snapshots",
    )
    parser.add_argument(
        "--asset-dir",
        default="data/raw/51job/html/assets",
        help="Directory for saving fetched local JS snapshots",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Thread workers for coapi detail requests",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout seconds",
    )
    parser.add_argument(
        "--max-seeds",
        type=int,
        default=0,
        help="Only process the first N seeds when > 0",
    )
    return parser.parse_args()


def decode_response(response: requests.Response) -> str:
    encoding = response.encoding
    if not encoding or encoding.lower() in {"iso-8859-1", "latin-1"}:
        encoding = response.apparent_encoding or encoding or "utf-8"
    response.encoding = encoding
    return response.text


def save_text_snapshot(url: str, text: str, output_dir: Path, suffix: str) -> str:
    filename = f"{sha1_text(url)}{suffix}"
    path = output_dir / filename
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")
    return portable_path(path)


def load_seed_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            seed_url = clean_text(row.get("seed_url", ""))
            if not seed_url:
                continue
            rows.append(
                {
                    "seed_url": seed_url,
                    "name": clean_text(row.get("name", "")),
                    "note": clean_text(row.get("note", "")),
                }
            )
    return rows


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    session.headers.setdefault("Referer", "https://campus.51job.com/")
    return session


def guess_company_name(soup: BeautifulSoup) -> str:
    def normalize_candidate(value: str) -> str:
        value = clean_text(value)
        value = re.sub(r"前程无忧官方网站.*$", "", value)
        value = re.sub(r"\d{4}.*?(?:全球校招|校园招聘|校招|招聘).*?$", "", value)
        value = re.sub(r"(?:全球校招|校园招聘|校招|招聘).*$", "", value)
        return clean_text(value)

    description = ""
    meta_description = soup.find("meta", attrs={"name": "description"})
    if meta_description and meta_description.get("content"):
        description = clean_text(str(meta_description["content"]))
        if description.startswith("人才，招聘，简历"):
            description = ""
        if "，前程无忧" in description:
            candidate = normalize_candidate(description.split("，前程无忧", 1)[0])
            if candidate:
                return candidate
        description = normalize_candidate(description)
        if description:
            return clean_text(description)

    title = clean_text(soup.title.get_text(strip=True) if soup.title else "")
    if title:
        bracket_match = re.search(r"【(.+?)】", title)
        if bracket_match:
            candidate = normalize_candidate(bracket_match.group(1))
            if candidate:
                return clean_text(candidate)
        title = re.sub(r"[-|_].*$", "", title)
        title = normalize_candidate(title)
        if title:
            return clean_text(title)

    return ""


def fetch_page_bundle(
    session: requests.Session,
    seed_url: str,
    *,
    timeout: float,
    page_dir: Path,
    asset_dir: Path,
) -> PageBundle:
    response = session.get(seed_url, timeout=timeout)
    response.raise_for_status()
    page_text = decode_response(response)
    page_local_path = save_text_snapshot(seed_url, page_text, page_dir, ".html")
    page_blob = Blob(url=seed_url, text=page_text, local_path=page_local_path)
    soup = BeautifulSoup(page_text, "html.parser")
    blobs = [page_blob]

    base_dir_url = seed_url.rsplit("/", 1)[0] + "/"
    asset_urls: list[str] = []

    for extra_path in (
        "job.html",
        "campus.html",
        "about.html",
        "about2.html",
        "position.html",
        "qanda.html",
        "index.html",
        "js/job.js",
        "js/main.js",
        "js/custom.js",
    ):
        asset_urls.append(urljoin(base_dir_url, extra_path))

    for script in soup.find_all("script", src=True):
        src_url = urljoin(seed_url, script["src"])
        if src_url.startswith(base_dir_url) and src_url.lower().endswith(".js"):
            asset_urls.append(src_url)

    for anchor in soup.find_all("a", href=True):
        href = urljoin(seed_url, anchor["href"])
        parsed = urlparse(href)
        if parsed.netloc and parsed.netloc != urlparse(seed_url).netloc:
            continue
        if not href.startswith(base_dir_url):
            continue
        if not href.lower().endswith(".html"):
            continue
        if any(token in href.lower() for token in ("job", "campus", "about", "qanda", "position", "index")):
            asset_urls.append(href)

    seen_urls = {seed_url}
    for asset_url in asset_urls:
        if asset_url in seen_urls:
            continue
        seen_urls.add(asset_url)
        try:
            asset_response = session.get(asset_url, timeout=timeout)
            asset_response.raise_for_status()
        except requests.RequestException:
            continue
        asset_text = decode_response(asset_response)
        suffix = ".js" if asset_url.lower().endswith(".js") else ".html"
        asset_local_path = save_text_snapshot(asset_url, asset_text, asset_dir, suffix)
        blobs.append(Blob(url=asset_url, text=asset_text, local_path=asset_local_path))

    return PageBundle(
        seed_url=seed_url,
        page_blob=page_blob,
        blobs=blobs,
        soup=soup,
        company_name=guess_company_name(soup),
        page_title=clean_text(soup.title.get_text(strip=True) if soup.title else ""),
    )


def extract_ctmids(bundle: PageBundle) -> list[str]:
    ctmids: list[str] = []
    for blob in bundle.blobs:
        for pattern in CTMID_PATTERNS:
            ctmids.extend(pattern.findall(blob.text))
    parsed_seed = urlparse(bundle.seed_url)
    for value in parse_qs(parsed_seed.query).get("CtmID", []):
        ctmids.append(value)
    deduped = []
    seen = set()
    for ctmid in ctmids:
        if ctmid in seen:
            continue
        seen.add(ctmid)
        deduped.append(ctmid)
    return deduped


def extract_js_literal(text: str, variable_name: str) -> str | None:
    pattern = re.compile(rf"\b(?:var|let|const)\s+{re.escape(variable_name)}\s*=\s*")
    match = pattern.search(text)
    if not match:
        return None

    start = match.end()
    while start < len(text) and text[start].isspace():
        start += 1
    if start >= len(text) or text[start] not in "[{":
        return None

    opening = text[start]
    closing = "]" if opening == "[" else "}"
    depth = 0
    in_single = False
    in_double = False
    escaped = False

    for index in range(start, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if in_single:
            if char == "'":
                in_single = False
            continue
        if in_double:
            if char == '"':
                in_double = False
            continue
        if char == "'":
            in_single = True
            continue
        if char == '"':
            in_double = True
            continue
        if char == opening:
            depth += 1
            continue
        if char == closing:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def js_literal_to_python(text: str) -> Any:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"(^|[^:])//.*?$", r"\1", text, flags=re.M)
    text = re.sub(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*:)", r'\1"\2"\3', text)
    text = re.sub(r"\btrue\b", "True", text)
    text = re.sub(r"\bfalse\b", "False", text)
    text = re.sub(r"\bnull\b", "None", text)
    return ast.literal_eval(text)


def extract_inline_dataset(bundle: PageBundle) -> tuple[str, list[dict[str, Any]]] | None:
    for blob in bundle.blobs:
        for variable_name in INLINE_VAR_NAMES:
            literal = extract_js_literal(blob.text, variable_name)
            if not literal:
                continue
            try:
                data = js_literal_to_python(literal)
            except (SyntaxError, ValueError):
                continue
            if isinstance(data, list) and data:
                return variable_name, data
    return None


def extract_apply_url(value: str) -> str:
    return clean_text(value)


def extract_jobid_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in ("jobid", "JobID"):
        values = query.get(key, [])
        if values:
            return clean_text(values[0])
    for pattern in JOBID_PATTERNS:
        match = pattern.search(url)
        if match:
            return match.group(1)
    return ""


def html_fragment_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return clean_text(soup.get_text("\n", strip=True))


def first_match(text: str, patterns: list[re.Pattern[str]]) -> str:
    for pattern in patterns:
        match = pattern.search(text or "")
        if match:
            return clean_text(match.group(1))
    return ""


def build_record(
    *,
    strategy: str,
    seed_url: str,
    company_name: str,
    job_title: str,
    city: str = "",
    salary: str = "",
    education: str = "",
    experience: str = "",
    industry: str = "",
    company_size: str = "",
    job_tags: str = "",
    jd_text: str = "",
    detail_url: str = "",
    source_job_id: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job_id = source_job_id or sha1_text(f"{seed_url}|{job_title}|{city}|{detail_url}")[:16]
    row = {
        "platform": PLATFORM_NAME,
        "job_id": job_id,
        "detail_url": detail_url or seed_url,
        "source_url": seed_url,
        "city_seed": clean_text(city),
        "keyword_seed": clean_text(job_tags),
        "job_title_raw": clean_text(job_title),
        "company_name_raw": clean_text(company_name),
        "city_raw": clean_text(city),
        "salary_raw": clean_text(salary),
        "education_raw": clean_text(education),
        "experience_raw": clean_text(experience),
        "company_industry_raw": clean_text(industry),
        "company_size_raw": clean_text(company_size),
        "job_tags_raw": clean_text(job_tags),
        "jd_text_raw": clean_text(jd_text),
        "strategy": strategy,
    }
    if extra:
        row.update(extra)
    return row


def build_coapi_records(
    bundle: PageBundle,
    client: CoapiClient,
    progress: ProgressBar,
    *,
    workers: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for ctmid in extract_ctmids(bundle):
        first_page = client.get_job_list(ctmid, pagenum=1, pagesize=100)
        resultbody = first_page.get("resultbody") or {}
        total = int(resultbody.get("totalnum") or 0)
        if total <= 0:
            continue

        joblist = list(resultbody.get("joblist") or [])
        total_pages = max(math.ceil(total / 100), 1)
        for page_number in range(2, total_pages + 1):
            page_data = client.get_job_list(ctmid, pagenum=page_number, pagesize=100)
            page_body = page_data.get("resultbody") or {}
            joblist.extend(page_body.get("joblist") or [])

        progress.add_jobs(len(joblist))
        indexed_jobs = {clean_text(str(job.get("jobid", ""))): job for job in joblist}

        with ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
            future_map = {
                executor.submit(client.get_job_detail, jobid): jobid
                for jobid in indexed_jobs.keys()
                if jobid
            }
            for future in as_completed(future_map):
                jobid = future_map[future]
                try:
                    detail_data = future.result()
                except Exception:
                    progress.tick_jobs(1)
                    continue
                detail = detail_data.get("resultbody") or {}
                base = indexed_jobs.get(jobid, {})
                detail_url = (
                    f"https://xyz.51job.com/external/apply.aspx?jobid={detail.get('jobid', jobid)}"
                    f"&ctmid={detail.get('ctmid', ctmid)}"
                )
                records.append(
                    build_record(
                        strategy="coapi",
                        seed_url=bundle.seed_url,
                        company_name=clean_text(detail.get("coname")) or bundle.company_name,
                        job_title=clean_text(detail.get("jobname")) or clean_text(base.get("jobname")),
                        city=clean_text(detail.get("jobareaname")) or clean_text(base.get("jobareaname")),
                        salary=clean_text(detail.get("providesalarname")) or clean_text(detail.get("monthlysalary")),
                        education=clean_text(detail.get("degreefrom")),
                        experience=clean_text(detail.get("workyearname")),
                        industry=clean_text(detail.get("indtype")),
                        company_size=clean_text(detail.get("companysizename")),
                        job_tags=clean_text(detail.get("funtype")) or clean_text(base.get("funtype")),
                        jd_text=html_fragment_to_text(clean_text(detail.get("jobinfo"))),
                        detail_url=detail_url,
                        source_job_id=clean_text(str(detail.get("jobid", jobid))),
                        extra={
                            "ctmid": clean_text(str(detail.get("ctmid", ctmid))),
                            "coid": clean_text(str(detail.get("coid", ""))),
                            "address_raw": clean_text(detail.get("address")),
                        },
                    )
                )
                progress.tick_jobs(1)
    return records


def build_inline_records(bundle: PageBundle, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if data and isinstance(data[0], dict) and "jobs" in data[0]:
        for block in data:
            company_name = clean_text(block.get("company")) or bundle.company_name
            for job in block.get("jobs", []):
                responsibilities = clean_text(job.get("responsibilities", ""))
                requirements = clean_text(job.get("requirements", ""))
                jd_parts = [part for part in [responsibilities, requirements] if part]
                apply_url = extract_apply_url(job.get("applyUrl", ""))
                records.append(
                    build_record(
                        strategy="inline_js",
                        seed_url=bundle.seed_url,
                        company_name=company_name,
                        job_title=clean_text(job.get("title", "")),
                        city=clean_text(job.get("city", "")),
                        job_tags=clean_text(job.get("department", "")),
                        jd_text="\n\n".join(jd_parts),
                        detail_url=apply_url or bundle.seed_url,
                        source_job_id=extract_jobid_from_url(apply_url),
                        extra={
                            "ctmid": first_match(apply_url, CTMID_PATTERNS),
                        },
                    )
                )
        return records

    for item in data:
        if not isinstance(item, dict):
            continue
        apply_url = extract_apply_url(item.get("link", "")) or extract_apply_url(item.get("applyUrl", ""))
        records.append(
            build_record(
                strategy="inline_js",
                seed_url=bundle.seed_url,
                company_name=bundle.company_name,
                job_title=clean_text(item.get("jobname", "")) or clean_text(item.get("title", "")),
                city=clean_text(item.get("city", "")),
                job_tags=clean_text(item.get("fl", "")) or clean_text(item.get("category", "")),
                jd_text=html_fragment_to_text(clean_text(item.get("jobinfo", "")) or clean_text(item.get("description", ""))),
                detail_url=apply_url or bundle.seed_url,
                source_job_id=extract_jobid_from_url(apply_url),
                extra={
                    "ctmid": first_match(apply_url, CTMID_PATTERNS),
                },
            )
        )

    return records


def extract_city_from_text(text: str) -> str:
    patterns = [
        r"工作地点(?:为|：|:)\s*([^\n。；;]+)",
        r"工作地点在([^\n。；;]+)",
        r"\[([^\[\]]+)\]",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return clean_text(match.group(1))
    return ""


def extract_education_from_text(text: str) -> str:
    keywords = ["博士", "硕士", "研究生", "本科", "大专", "专科", "不限"]
    for keyword in keywords:
        if keyword in text:
            return keyword
    return ""


def extract_experience_from_text(text: str) -> str:
    keywords = ["应届", "在校生", "无经验", "1-3年", "3-5年", "5-10年", "10年以上", "不限"]
    for keyword in keywords:
        if keyword in text:
            return keyword
    return ""


def extract_announcement_jd_text(text: str) -> str:
    start_markers = ["一、招聘岗位及报名资格", "招聘岗位及报名资格", "招聘岗位："]
    start_index = 0
    for marker in start_markers:
        found = text.find(marker)
        if found != -1:
            start_index = found
            break

    end_index = len(text)
    for marker in ("二、报名时间及方式", "三、招聘流程", "注意事项"):
        found = text.find(marker)
        if found != -1 and found > start_index:
            end_index = min(end_index, found)
    return clean_text(text[start_index:end_index])


def build_static_accordion_records(bundle: PageBundle) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in bundle.soup.select("ul.info-ul2 > li"):
        title_node = item.select_one("div.top > div")
        details_node = item.select_one("div.btm")
        apply_node = item.select_one("div.dj a[href]")
        if not title_node or not details_node:
            continue
        title_text = clean_text(title_node.get_text(" ", strip=True))
        if not title_text:
            continue
        details_text = html_fragment_to_text(str(details_node))
        if "岗位职责" not in details_text and "应聘条件" not in details_text:
            continue
        apply_url = clean_text(apply_node["href"]) if apply_node else bundle.seed_url
        records.append(
            build_record(
                strategy="static_html",
                seed_url=bundle.seed_url,
                company_name=bundle.company_name,
                job_title=title_text,
                city=extract_city_from_text(details_text),
                education=extract_education_from_text(details_text),
                experience=extract_experience_from_text(details_text),
                jd_text=details_text,
                detail_url=apply_url,
                source_job_id=extract_jobid_from_url(apply_url) or clean_text(item.get("data-id", "")),
                extra={
                    "ctmid": first_match(apply_url, CTMID_PATTERNS),
                },
            )
        )
    return records


def build_static_announcement_record(bundle: PageBundle) -> dict[str, Any] | None:
    main = bundle.soup.select_one("div.main, div.content, body")
    if not main:
        return None
    main_text = clean_text(main.get_text("\n", strip=True))
    if "招聘岗位" not in main_text:
        return None

    title_match = re.search(r"招聘岗位[：:]\s*([^\n。；;]+?)(?=\s*\d+\.|$)", main_text)
    job_title = clean_text(title_match.group(1)) if title_match else clean_text(bundle.page_title)
    if not job_title:
        return None

    location_match = re.search(r"工作地点(?:在|为|：|:)\s*([^\n。；;]+)", main_text)
    city = clean_text(location_match.group(1)) if location_match else ""
    education = extract_education_from_text(main_text)
    experience = extract_experience_from_text(main_text)
    apply_node = bundle.soup.select_one('a[href*="Apply.aspx"]')
    apply_url = clean_text(apply_node["href"]) if apply_node else bundle.seed_url

    return build_record(
        strategy="static_announcement",
        seed_url=bundle.seed_url,
        company_name=bundle.company_name,
        job_title=job_title,
        city=city,
        education=education,
        experience=experience,
        jd_text=extract_announcement_jd_text(main_text),
        detail_url=apply_url,
        source_job_id=extract_jobid_from_url(apply_url),
        extra={
            "ctmid": first_match(apply_url, CTMID_PATTERNS),
        },
    )


def build_static_records(bundle: PageBundle) -> list[dict[str, Any]]:
    records = build_static_accordion_records(bundle)
    if records:
        return records

    single_record = build_static_announcement_record(bundle)
    if single_record:
        return [single_record]
    return []


def dedupe_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            clean_text(row.get("job_title_raw", "")),
            clean_text(row.get("company_name_raw", "")),
            clean_text(row.get("city_raw", "")),
            clean_text(row.get("detail_url", "")),
            clean_text(row.get("jd_text_raw", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def process_seed(
    seed_row: dict[str, str],
    *,
    session: requests.Session,
    client: CoapiClient,
    timeout: float,
    page_dir: Path,
    asset_dir: Path,
    workers: int,
    progress: ProgressBar,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    seed_url = seed_row["seed_url"]
    bundle = fetch_page_bundle(
        session,
        seed_url,
        timeout=timeout,
        page_dir=page_dir,
        asset_dir=asset_dir,
    )

    records: list[dict[str, Any]] = []
    strategy = ""
    error = ""

    try:
        coapi_records = build_coapi_records(bundle, client, progress, workers=workers)
        if coapi_records:
            records.extend(coapi_records)
            strategy = "coapi"
        else:
            inline_payload = extract_inline_dataset(bundle)
            if inline_payload:
                _, data = inline_payload
                inline_records = build_inline_records(bundle, data)
                if inline_records:
                    progress.add_jobs(len(inline_records))
                    progress.tick_jobs(len(inline_records))
                    records.extend(inline_records)
                    strategy = "inline_js"
            if not records:
                static_records = build_static_records(bundle)
                if static_records:
                    progress.add_jobs(len(static_records))
                    progress.tick_jobs(len(static_records))
                    records.extend(static_records)
                    strategy = static_records[0].get("strategy", "static_html")
    except Exception as exc:
        error = str(exc)

    records = dedupe_records(records)
    manifest_row = {
        "seed_url": seed_url,
        "name": seed_row.get("name", ""),
        "note": seed_row.get("note", ""),
        "company_name": bundle.company_name,
        "page_title": bundle.page_title,
        "strategy": strategy or "none",
        "records_emitted": len(records),
        "page_local_path": bundle.page_blob.local_path,
        "error": error,
        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return records, manifest_row


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    seed_file = ROOT_DIR / args.seed_file
    output_raw = ROOT_DIR / args.output_raw
    manifest_path = ROOT_DIR / args.manifest
    page_dir = ROOT_DIR / args.page_dir
    asset_dir = ROOT_DIR / args.asset_dir

    seeds = load_seed_rows(seed_file)
    if args.max_seeds > 0:
        seeds = seeds[: args.max_seeds]
    if not seeds:
        raise SystemExit(f"no seed rows found in {seed_file}")

    session = build_session()
    client = CoapiClient(session=session, timeout=args.timeout)
    progress = ProgressBar(seed_total=len(seeds))
    all_records: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []

    for index, seed_row in enumerate(seeds, start=1):
        label = seed_row.get("name") or seed_row["seed_url"].replace("https://campus.51job.com/", "")
        progress.set_seed(index, label)
        try:
            records, manifest_row = process_seed(
                seed_row,
                session=session,
                client=client,
                timeout=args.timeout,
                page_dir=page_dir,
                asset_dir=asset_dir,
                workers=args.workers,
                progress=progress,
            )
        except Exception as exc:
            records = []
            manifest_row = {
                "seed_url": seed_row["seed_url"],
                "name": seed_row.get("name", ""),
                "note": seed_row.get("note", ""),
                "company_name": "",
                "page_title": "",
                "strategy": "failed",
                "records_emitted": 0,
                "page_local_path": "",
                "error": str(exc),
                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        all_records.extend(records)
        manifest_rows.append(manifest_row)
        progress.finish_seed()

    progress.newline()
    all_records = dedupe_records(all_records)
    write_jsonl(output_raw, all_records)
    write_jsonl(manifest_path, manifest_rows)

    total_cities = len(
        {
            clean_text(row.get("city_raw", ""))
            for row in all_records
            if clean_text(row.get("city_raw", ""))
        }
    )
    print(
        json.dumps(
            {
                "seed_count": len(seeds),
                "record_count": len(all_records),
                "city_count": total_cities,
                "output_raw": portable_path(output_raw),
                "manifest": portable_path(manifest_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
