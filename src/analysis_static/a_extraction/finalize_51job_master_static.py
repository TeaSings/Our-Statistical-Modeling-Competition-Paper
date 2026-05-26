from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import textwrap
from collections import Counter
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
SRC_DIR = THIS_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from common import ROOT_DIR, configure_utf8_stdio
import build_51job_master_static as builder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="汇总 51job A 线行级输出并生成说明文档")
    parser.add_argument("--output-root", default=str(builder.DEFAULT_OUTPUT_ROOT), help="analysis_static 输出根目录")
    parser.add_argument("--docs-root", default=str(builder.DEFAULT_DOCS_ROOT), help="A 线文档输出目录")
    parser.add_argument("--seed", type=int, default=20260421, help="抽样随机种子")
    return parser.parse_args()


def load_detail_urls(union_raw_path: Path, wanted_job_ids: set[str]) -> dict[str, str]:
    detail_map: dict[str, str] = {}
    if not wanted_job_ids:
        return detail_map
    with union_raw_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            job_id = row.get("job_id", "")
            if job_id in wanted_job_ids and job_id not in detail_map:
                detail_map[job_id] = row.get("detail_url", "")
    return detail_map


def build_annotation_seed(master_path: Path, seed: int) -> tuple[list[dict[str, str]], Counter[str], Counter[str], Counter[str], int]:
    rng = random.Random(seed)
    reservoir_state: dict[str, object] = {}
    quality_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    exposure_counter: Counter[str] = Counter()
    total_rows = 0

    with master_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            family_counter[row.get("job_family_std", "")] += 1
            exposure_counter[row.get("genai_exposure_level", "")] += 1

            if row.get("city_std_final", ""):
                quality_counter["city_mapped"] += 1
            if row.get("province_std_final", ""):
                quality_counter["province_mapped"] += 1
            quality_counter[f"city_conf_{row.get('city_std_confidence', '')}"] += 1
            quality_counter[f"family_conf_{row.get('job_family_confidence', '')}"] += 1
            if row.get("city_std_confidence", "") == "low":
                quality_counter["city_manual_review_rows"] += 1
            if row.get("job_family_confidence", "") == "low":
                quality_counter["job_family_manual_review_rows"] += 1

            if row.get("skill_hit_count", "0") not in {"", "0"}:
                quality_counter["jobs_with_skill_hit"] += 1
            if row.get("task_hit_count", "0") not in {"", "0"}:
                quality_counter["jobs_with_task_hit"] += 1
            if row.get("genai_related_skill_list", ""):
                quality_counter["jobs_with_genai_skill_hit"] += 1

            annotation_row = {
                "job_id": row.get("job_id", ""),
                "source_job_id": row.get("source_job_id", ""),
                "job_title_std": row.get("job_title_std", ""),
                "city_std_final": row.get("city_std_final", ""),
                "province_std_final": row.get("province_std_final", ""),
                "job_family_std": row.get("job_family_std", ""),
                "job_family_confidence": row.get("job_family_confidence", ""),
                "city_std_confidence": row.get("city_std_confidence", ""),
                "skill_list": row.get("skill_list", ""),
                "task_list": row.get("task_list", ""),
                "genai_exposure_level": row.get("genai_exposure_level", ""),
                "jd_text_clean": row.get("jd_text_clean", ""),
                "review_job_family": "",
                "review_skill_list": "",
                "review_task_list": "",
                "review_notes": "",
            }

            if row.get("genai_exposure_level", "") == "high":
                builder.update_reservoir(reservoir_state, "high", 120, annotation_row, rng)
            elif row.get("genai_exposure_level", "") == "medium":
                builder.update_reservoir(reservoir_state, "medium", 120, annotation_row, rng)
            if row.get("job_family_confidence", "") == "low":
                builder.update_reservoir(reservoir_state, "family_low", 130, annotation_row, rng)
            if row.get("city_std_confidence", "") == "low":
                builder.update_reservoir(reservoir_state, "city_low", 80, annotation_row, rng)
            builder.update_reservoir(reservoir_state, "general", 120, annotation_row, rng)

    annotation_rows: list[dict[str, str]] = []
    for bucket in ("high_rows", "medium_rows", "family_low_rows", "city_low_rows", "general_rows"):
        for row in reservoir_state.get(bucket, []):
            if row not in annotation_rows:
                annotation_rows.append(row)
            if len(annotation_rows) >= 500:
                break
        if len(annotation_rows) >= 500:
            break

    return annotation_rows[:500], quality_counter, family_counter, exposure_counter, total_rows


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    output_root = ROOT_DIR / args.output_root
    docs_root = ROOT_DIR / args.docs_root
    extraction_root = output_root / "a_extraction"
    interfaces_root = output_root / "interfaces"

    builder.ensure_dir(extraction_root)
    builder.ensure_dir(interfaces_root)
    builder.ensure_dir(docs_root)

    union_raw_path = interfaces_root / "job_level_panel_union_raw.csv"
    master_path = interfaces_root / "job_level_master_static.csv"
    city_std_path = extraction_root / "job_level_panel_city_std.csv"
    family_std_path = extraction_root / "job_level_panel_job_family_std.csv"
    skill_raw_path = extraction_root / "skill_extraction_table_raw.csv"

    city_mapping_path = extraction_root / "city_mapping_table.csv"
    city_manual_review_path = extraction_root / "city_manual_review.csv"
    job_family_manual_review_path = extraction_root / "job_family_manual_review.csv"
    skill_token_norm_path = extraction_root / "skill_token_norm.csv"
    annotation_seed_path = extraction_root / "annotation_seed_500.csv"
    schema_mapping_path = interfaces_root / "schema_mapping_static.json"

    annotation_rows, quality_counter, family_counter, exposure_counter, total_rows = build_annotation_seed(master_path, args.seed)

    city_mapping_stats: dict[tuple[str, ...], dict[str, object]] = {}
    with city_std_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row.get("province_std_source", ""),
                row.get("city_std_source", ""),
                row.get("district_std_source", ""),
                row.get("province_std_final", ""),
                row.get("city_std_final", ""),
                row.get("district_std_final", ""),
                row.get("city_std_confidence", ""),
                row.get("city_std_reason", ""),
            )
            stat = city_mapping_stats.setdefault(
                key,
                {
                    "record_count": 0,
                    "example_job_id": row.get("job_id", ""),
                    "example_source_job_id": row.get("source_job_id", ""),
                },
            )
            stat["record_count"] = int(stat["record_count"]) + 1

    family_review_stats: dict[tuple[str, ...], dict[str, object]] = {}
    with family_std_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row.get("job_title_std", ""),
                row.get("keyword_seed", ""),
                row.get("job_family_code", ""),
                row.get("job_family_std", ""),
                row.get("job_family_confidence", ""),
                row.get("job_family_reason", ""),
            )
            stat = family_review_stats.setdefault(
                key,
                {
                    "record_count": 0,
                    "example_job_id": row.get("job_id", ""),
                    "example_source_job_id": row.get("source_job_id", ""),
                },
            )
            stat["record_count"] = int(stat["record_count"]) + 1

    skill_token_norm_counter: Counter[tuple[str, str, str, str]] = Counter()
    with skill_raw_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row.get("match_value", ""),
                row.get("skill_norm", ""),
                row.get("skill_category", ""),
                row.get("is_genai_skill", ""),
            )
            skill_token_norm_counter[key] += 1

    wanted_job_ids = {
        value["example_job_id"]
        for value in city_mapping_stats.values()
    } | {
        value["example_job_id"]
        for value in family_review_stats.values()
    }
    detail_map = load_detail_urls(union_raw_path, wanted_job_ids)

    city_mapping_rows = sorted(
        (
            {
                "province_std_source": key[0],
                "city_std_source": key[1],
                "district_std_source": key[2],
                "province_std_final": key[3],
                "city_std_final": key[4],
                "district_std_final": key[5],
                "city_std_confidence": key[6],
                "city_std_reason": key[7],
                "record_count": value["record_count"],
                "example_job_id": value["example_job_id"],
                "example_source_job_id": value["example_source_job_id"],
                "example_detail_url": detail_map.get(str(value["example_job_id"]), ""),
            }
            for key, value in city_mapping_stats.items()
        ),
        key=lambda row: (-int(row["record_count"]), row["city_std_source"], row["province_std_source"]),
    )
    builder.write_csv(
        city_mapping_path,
        [
            "province_std_source",
            "city_std_source",
            "district_std_source",
            "province_std_final",
            "city_std_final",
            "district_std_final",
            "city_std_confidence",
            "city_std_reason",
            "record_count",
            "example_job_id",
            "example_source_job_id",
            "example_detail_url",
        ],
        city_mapping_rows,
    )
    builder.write_csv(
        city_manual_review_path,
        [
            "province_std_source",
            "city_std_source",
            "district_std_source",
            "province_std_final",
            "city_std_final",
            "district_std_final",
            "city_std_confidence",
            "city_std_reason",
            "record_count",
            "example_job_id",
            "example_source_job_id",
            "example_detail_url",
        ],
        (
            row
            for row in city_mapping_rows
            if row["city_std_confidence"] == "low" or not row["city_std_final"] or not row["province_std_final"]
        ),
    )

    family_review_rows = sorted(
        (
            {
                "job_title_std": key[0],
                "keyword_seed": key[1],
                "assigned_job_family_code": key[2],
                "assigned_job_family_std": key[3],
                "job_family_confidence": key[4],
                "job_family_reason": key[5],
                "record_count": value["record_count"],
                "example_job_id": value["example_job_id"],
                "example_source_job_id": value["example_source_job_id"],
                "example_detail_url": detail_map.get(str(value["example_job_id"]), ""),
            }
            for key, value in family_review_stats.items()
            if key[4] == "low"
        ),
        key=lambda row: (-int(row["record_count"]), row["assigned_job_family_code"], row["job_title_std"]),
    )
    builder.write_csv(
        job_family_manual_review_path,
        [
            "job_title_std",
            "keyword_seed",
            "assigned_job_family_code",
            "assigned_job_family_std",
            "job_family_confidence",
            "job_family_reason",
            "record_count",
            "example_job_id",
            "example_source_job_id",
            "example_detail_url",
        ],
        family_review_rows,
    )

    builder.write_csv(
        skill_token_norm_path,
        ["match_value", "skill_norm", "skill_category", "is_genai_skill", "hit_count"],
        (
            {
                "match_value": key[0],
                "skill_norm": key[1],
                "skill_category": key[2],
                "is_genai_skill": key[3],
                "hit_count": count,
            }
            for key, count in sorted(
                skill_token_norm_counter.items(),
                key=lambda item: (-item[1], item[0][1], item[0][0]),
            )
        ),
    )

    builder.write_csv(
        annotation_seed_path,
        [
            "job_id",
            "source_job_id",
            "job_title_std",
            "city_std_final",
            "province_std_final",
            "job_family_std",
            "job_family_confidence",
            "city_std_confidence",
            "skill_list",
            "task_list",
            "genai_exposure_level",
            "jd_text_clean",
            "review_job_family",
            "review_skill_list",
            "review_task_list",
            "review_notes",
        ],
        annotation_rows,
    )

    schema_mapping = builder.build_schema_mapping(total_rows)
    schema_mapping_path.write_text(json.dumps(schema_mapping, ensure_ascii=False, indent=2), encoding="utf-8")

    root_readme = ROOT_DIR / "README.md"
    processed_readme = ROOT_DIR / "data/processed/README.md"

    (docs_root / "00_branch_baseline_audit.md").write_text(
        textwrap.dedent(
            f"""\
            # 00 分支基线核对（A 线 / 51job-only）

            ## 结论摘要

            - 当前 A 线主样本固定为 `data/processed/51job/51job_social_jobs_clean_with_publish.csv`。
            - 实际样本量为 `{total_rows}` 行，显著高于仓库中遗留的 `8184` 条旧口径。
            - `data/processed/51job/README.md` 仍描述旧文件 `51job_social_jobs_clean.csv`，与当前主样本不一致。
            - 当前 51job 社招大表已足够支撑 `city-job_family` 静态版主表建设。

            ## README/快照差异

            - 根目录 `README.md` 对 `_with_publish` 的遗留数字：`{builder.read_outdated_count(root_readme, '51job_social_jobs_clean_with_publish.csv')}`
            - `data/processed/README.md` 对同一文件的遗留数字：`{builder.read_outdated_count(processed_readme, '51job_social_jobs_clean_with_publish.csv')}`

            ## 当前主样本建议

            - 主样本：`data/processed/51job/51job_social_jobs_clean_with_publish.csv`
            - 当前执行口径：`51job-only`、`city-job_family`
            - 当前不纳入：`NCSS`、其他平台、完整时间面板主线
            """
        ),
        encoding="utf-8",
    )

    top_families_text = "\n".join(
        f"- {label}: {count}"
        for label, count in family_counter.most_common(12)
    )
    (docs_root / "02_city_standardization.md").write_text(
        textwrap.dedent(
            f"""\
            # 02 城市标准化说明

            ## 结果摘要

            - 总样本：`{total_rows}`
            - 已映射到城市：`{quality_counter['city_mapped']}`
            - 已映射到省份：`{quality_counter['province_mapped']}`
            - 高置信城市映射：`{quality_counter['city_conf_high']}`
            - 中置信城市映射：`{quality_counter['city_conf_medium']}`
            - 低置信或需人工复核：`{quality_counter['city_conf_low']}`

            ## 产物

            - `data/processed/analysis_static/a_extraction/city_mapping_table.csv`
            - `data/processed/analysis_static/a_extraction/city_manual_review.csv`
            - `data/processed/analysis_static/a_extraction/job_level_panel_city_std.csv`
            """
        ),
        encoding="utf-8",
    )
    (docs_root / "03_job_family_standardization.md").write_text(
        textwrap.dedent(
            f"""\
            # 03 岗位族标准化说明

            ## 结果摘要

            - 总样本：`{total_rows}`
            - 高置信岗位族：`{quality_counter['family_conf_high']}`
            - 中置信岗位族：`{quality_counter['family_conf_medium']}`
            - 低置信岗位族：`{quality_counter['family_conf_low']}`

            ## 高频岗位族

            {top_families_text}
            """
        ),
        encoding="utf-8",
    )
    (docs_root / "04_rule_dictionary_notes.md").write_text(
        textwrap.dedent(
            f"""\
            # 04 规则词典与抽取说明

            ## 结果摘要

            - 有技能命中的职位：`{quality_counter['jobs_with_skill_hit']}`
            - 有任务命中的职位：`{quality_counter['jobs_with_task_hit']}`
            - 有 GenAI 相关技能命中的职位：`{quality_counter['jobs_with_genai_skill_hit']}`
            - `genai_exposure_level=high`：`{exposure_counter['high']}`
            - `genai_exposure_level=medium`：`{exposure_counter['medium']}`
            - `genai_exposure_level=low`：`{exposure_counter['low']}`
            - `genai_exposure_level=none`：`{exposure_counter['none']}`
            """
        ),
        encoding="utf-8",
    )
    (docs_root / "05_extraction_quality_report.md").write_text(
        textwrap.dedent(
            f"""\
            # 05 抽取质量报告

            ## 基础规模

            - 总职位数：`{total_rows}`
            - 输出主接口表：`data/processed/analysis_static/interfaces/job_level_master_static.csv`

            ## 城市标准化

            - 已映射城市占比：`{quality_counter['city_mapped'] / total_rows:.4%}`
            - 已映射省份占比：`{quality_counter['province_mapped'] / total_rows:.4%}`
            - 需人工复核城市行数：`{quality_counter['city_manual_review_rows']}`

            ## 岗位族标准化

            - 低置信岗位族行数：`{quality_counter['job_family_manual_review_rows']}`
            - 最常见岗位族：`{family_counter.most_common(1)[0][0] if family_counter else 'NA'}`

            ## 技能与任务抽取

            - 技能命中占比：`{quality_counter['jobs_with_skill_hit'] / total_rows:.4%}`
            - 任务命中占比：`{quality_counter['jobs_with_task_hit'] / total_rows:.4%}`
            - GenAI 命中占比：`{quality_counter['jobs_with_genai_skill_hit'] / total_rows:.4%}`
            """
        ),
        encoding="utf-8",
    )

    print(
        f"finalized A-line outputs: total_rows={total_rows}, "
        f"city_mapping={city_mapping_path}, family_review={job_family_manual_review_path}"
    )


if __name__ == "__main__":
    main()
