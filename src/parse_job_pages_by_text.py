from __future__ import annotations

import argparse
import re
from pathlib import Path

from common import ROOT_DIR, append_jsonl, clean_text, load_jsonl


SECTION_LABELS = {
    "职位描述",
    "职位详情",
    "岗位职责",
    "任职要求",
    "职位福利",
    "工作地点",
    "公司信息",
    "公司介绍",
    "所属行业",
    "公司性质",
    "公司规模",
    "所在地址",
    "单位名称",
    "月薪范围",
    "学历要求",
    "工作经验",
    "发布时间",
    "联系人",
    "联系方式",
    "行业类型",
    "岗位类型",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按页面可见文本解析职位详情页")
    parser.add_argument("--manifest", required=True, help="抓取日志 jsonl")
    parser.add_argument("--output", required=True, help="输出 jsonl")
    parser.add_argument(
        "--platform-filter",
        default="",
        help="可选，逗号分隔的平台过滤",
    )
    return parser.parse_args()


def visible_lines_from_html(html: str) -> list[str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = []
    last = ""
    for raw in text.splitlines():
        line = clean_text(raw)
        if not line:
            continue
        if line == last:
            continue
        if line in {"Image", "Input", "Button", "意见反馈"}:
            continue
        if line.startswith("Image:") or line.startswith("Button:") or line.startswith("Input:"):
            continue
        lines.append(line)
        last = line
    return lines


def detect_parser(platform: str, url: str) -> str:
    platform = platform or ""
    url = url or ""
    if platform == "mohrss_public" or "job.mohrss.gov.cn" in url:
        return "mohrss_public"
    if platform == "ncss_jobs" or "ncss.cn/student/jobs/" in url:
        return "ncss_jobs"
    if platform == "zhaopin_detail" or "zhaopin.com/jobdetail/" in url:
        return "zhaopin_detail"
    return "generic"


def is_salary_line(line: str) -> bool:
    patterns = [
        r"\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*[Kk]",
        r"\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*万",
        r"\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*元",
        r"面议",
    ]
    return any(re.search(pattern, line) for pattern in patterns)


def looks_like_company(line: str) -> bool:
    keywords = [
        "有限公司",
        "集团",
        "公司",
        "大学",
        "学院",
        "研究院",
        "研究所",
        "中心",
        "医院",
        "银行",
        "学校",
        "事务所",
        "管委会",
        "政府",
    ]
    return any(keyword in line for keyword in keywords)


def looks_like_education(line: str) -> bool:
    return any(token in line for token in ["博士", "硕士", "本科", "大专", "专科", "学历不限", "及以上"])


def looks_like_experience(line: str) -> bool:
    return any(token in line for token in ["经验不限", "应届", "1-3年", "3-5年", "5-10年", "10年以上", "1年以下"])


def looks_like_city(line: str) -> bool:
    if any(token in line for token in ["工作地点", "招聘", "来源", "公司", "职位", "本科", "大专"]):
        return False
    if len(line) > 20:
        return False
    return bool(re.fullmatch(r"[\u4e00-\u9fa5A-Za-z·\-\s]{2,20}", line))


def guess_title(lines: list[str]) -> str:
    salary_idx = next((idx for idx, line in enumerate(lines) if is_salary_line(line)), None)
    if salary_idx is not None:
        for idx in range(max(0, salary_idx - 3), salary_idx):
            line = lines[idx]
            if line in SECTION_LABELS:
                continue
            if line.startswith("更新于"):
                continue
            if looks_like_company(line):
                continue
            if is_salary_line(line):
                continue
            if len(line) <= 40:
                return line
    for line in lines:
        if line in SECTION_LABELS:
            continue
        if not looks_like_company(line) and len(line) <= 40:
            return line
    return ""


def extract_value_after_label(lines: list[str], labels: list[str]) -> str:
    for idx, line in enumerate(lines):
        for label in labels:
            if line == label:
                for next_idx in range(idx + 1, min(idx + 4, len(lines))):
                    next_line = lines[next_idx]
                    if next_line not in SECTION_LABELS:
                        return next_line
            if line.startswith(label):
                value = clean_text(
                    line.replace(label, "", 1)
                    .lstrip("：: ")
                )
                if value:
                    return value
    return ""


def collect_block(lines: list[str], start_labels: list[str], stop_labels: list[str]) -> str:
    collecting = False
    buffer: list[str] = []
    stops = set(stop_labels)

    for idx, line in enumerate(lines):
        if not collecting and any(line.startswith(label) for label in start_labels):
            collecting = True
            appended = False
            for label in start_labels:
                if line.startswith(label):
                    remain = clean_text(line.replace(label, "", 1).lstrip("：: "))
                    if remain:
                        buffer.append(remain)
                        appended = True
                    break
            if appended:
                continue
            continue

        if collecting:
            if line in stops and buffer:
                break
            if any(line.startswith(stop) for stop in stop_labels) and buffer:
                break
            buffer.append(line)

    return clean_text(" ".join(buffer))


def find_first_company(lines: list[str]) -> str:
    for line in lines:
        if looks_like_company(line):
            return line
    return ""


def parse_recruit_num(lines: list[str]) -> str:
    for line in lines:
        match = re.search(r"(招聘\s*\d+\s*人|招\s*\d+\s*人)", line)
        if match:
            return clean_text(match.group(1))
    return ""


def parse_salary(lines: list[str]) -> str:
    for line in lines:
        if is_salary_line(line):
            return line
    return ""


def parse_city_near_top(lines: list[str]) -> str:
    salary_idx = next((idx for idx, line in enumerate(lines) if is_salary_line(line)), None)
    if salary_idx is None:
        return extract_value_after_label(lines, ["工作地点"])
    for idx in range(salary_idx + 1, min(salary_idx + 6, len(lines))):
        line = lines[idx]
        if looks_like_city(line) and not looks_like_experience(line) and not looks_like_education(line):
            return line
    return extract_value_after_label(lines, ["工作地点"])


def parse_mohrss(lines: list[str]) -> dict:
    return {
        "job_title_raw": guess_title(lines),
        "company_name_raw": extract_value_after_label(lines, ["单位名称"]) or find_first_company(lines),
        "city_raw": extract_value_after_label(lines, ["工作地点"]) or parse_city_near_top(lines),
        "salary_raw": extract_value_after_label(lines, ["月薪范围"]) or parse_salary(lines),
        "education_raw": extract_value_after_label(lines, ["学历要求"]),
        "experience_raw": extract_value_after_label(lines, ["工作经验"]),
        "company_industry_raw": extract_value_after_label(lines, ["行业类型", "所属行业"]),
        "company_size_raw": extract_value_after_label(lines, ["单位规模", "公司规模"]),
        "job_tags_raw": "",
        "jd_text_raw": collect_block(
            lines,
            ["岗位职责", "职位描述", "工作内容"],
            ["工作地点", "联系人", "联系方式", "公司信息", "单位名称", "发布时间"],
        ),
        "recruit_num_raw": parse_recruit_num(lines),
    }


def parse_ncss(lines: list[str]) -> dict:
    company_name = extract_value_after_label(lines, ["来源"])
    if company_name.startswith("："):
        company_name = company_name[1:].strip()
    jd_text = collect_block(
        lines,
        ["职位详情", "职位说明", "岗位职责", "职位描述"],
        ["所属行业", "公司性质", "公司规模", "所在地址", "公司信息"],
    )
    if not company_name:
        company_name = find_first_company(reversed(lines))
    return {
        "job_title_raw": guess_title(lines),
        "company_name_raw": company_name or find_first_company(lines),
        "city_raw": extract_value_after_label(lines, ["工作地点"]) or "",
        "salary_raw": parse_salary(lines),
        "education_raw": next((line for line in lines if looks_like_education(line)), ""),
        "experience_raw": next((line for line in lines if looks_like_experience(line)), ""),
        "company_industry_raw": extract_value_after_label(lines, ["所属行业"]),
        "company_size_raw": extract_value_after_label(lines, ["公司规模"]),
        "job_tags_raw": "",
        "jd_text_raw": jd_text,
        "recruit_num_raw": parse_recruit_num(lines),
    }


def parse_zhaopin(lines: list[str]) -> dict:
    jd_text = collect_block(
        lines,
        ["职位描述", "岗位职责"],
        ["工作地点", "公司信息", "公司介绍"],
    )
    company_info = collect_block(
        lines,
        ["公司信息"],
        ["公司介绍", "工作地点", "职位福利"],
    )
    company_size = ""
    industry = ""
    for chunk in company_info.split():
        if re.search(r"\d+\s*-\s*\d+人|10000人以上|20人以下", chunk):
            company_size = chunk
        if any(token in chunk for token in ["互联网", "软件", "教育", "制造", "装饰", "医疗", "金融", "贸易"]):
            industry = chunk

    title = guess_title(lines)
    company = extract_value_after_label(lines, ["公司信息"]) or find_first_company(lines)
    education = next((line for line in lines if looks_like_education(line)), "")
    experience = next((line for line in lines if looks_like_experience(line)), "")

    tags = ""
    if jd_text:
        first_sentence = jd_text.split("岗位职责", 1)[0].strip()
        if len(first_sentence) <= 50 and "。" not in first_sentence:
            tags = first_sentence

    return {
        "job_title_raw": title,
        "company_name_raw": company,
        "city_raw": extract_value_after_label(lines, ["工作地点"]) or parse_city_near_top(lines),
        "salary_raw": parse_salary(lines),
        "education_raw": education,
        "experience_raw": experience,
        "company_industry_raw": industry,
        "company_size_raw": company_size,
        "job_tags_raw": tags,
        "jd_text_raw": jd_text,
        "recruit_num_raw": parse_recruit_num(lines),
    }


def parse_generic(lines: list[str]) -> dict:
    return {
        "job_title_raw": guess_title(lines),
        "company_name_raw": find_first_company(lines),
        "city_raw": parse_city_near_top(lines),
        "salary_raw": parse_salary(lines),
        "education_raw": next((line for line in lines if looks_like_education(line)), ""),
        "experience_raw": next((line for line in lines if looks_like_experience(line)), ""),
        "company_industry_raw": "",
        "company_size_raw": "",
        "job_tags_raw": "",
        "jd_text_raw": collect_block(lines, ["职位描述", "职位详情", "岗位职责"], ["公司信息", "工作地点"]),
        "recruit_num_raw": parse_recruit_num(lines),
    }


def parse_lines_by_platform(platform: str, lines: list[str]) -> dict:
    if platform == "mohrss_public":
        return parse_mohrss(lines)
    if platform == "ncss_jobs":
        return parse_ncss(lines)
    if platform == "zhaopin_detail":
        return parse_zhaopin(lines)
    return parse_generic(lines)


def main() -> None:
    args = parse_args()
    rows = load_jsonl(ROOT_DIR / args.manifest)
    platform_filter = {
        item.strip() for item in args.platform_filter.split(",") if item.strip()
    }

    output_rows = []
    for row in rows:
        if row.get("page_type") != "detail":
            continue
        if row.get("error"):
            continue

        platform = detect_parser(row.get("platform", ""), row.get("url", ""))
        if platform_filter and platform not in platform_filter:
            continue

        html_path = Path(row["local_path"])
        if not html_path.exists():
            continue

        html = html_path.read_text(encoding="utf-8", errors="ignore")
        lines = visible_lines_from_html(html)
        parsed = parse_lines_by_platform(platform, lines)

        output_row = {
            "platform": row.get("platform", platform),
            "parser_used": platform,
            "detail_url": row.get("url", ""),
            "source_url": row.get("source_url", ""),
            "city_seed": row.get("city", ""),
            "keyword_seed": row.get("keyword", ""),
            **parsed,
        }

        output_rows.append(output_row)

    append_jsonl(ROOT_DIR / args.output, output_rows)
    print(f"parsed {len(output_rows)} pages to {ROOT_DIR / args.output}")


if __name__ == "__main__":
    main()
