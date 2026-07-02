# 51job 静态抽取概览

该流程把 51job 社招清洗表整理为论文使用的 job-level 主表和抽取结果。当前分析采用静态截面口径，核心单元为 `city × job_family`。

## 主输入

| 文件 | 说明 |
| --- | --- |
| `data/processed/51job/51job_social_jobs_clean_with_publish.csv` | 51job 社招清洗表，包含岗位标题、城市、企业、正文、薪资和发布时间 |
| `data/input/ncss/ncss_area_codes_all.csv` | 全国地区码，用于城市标准化 |
| `data/input/51job/51job_search_area_tree.json` | 51job 地区树，用于平台城市口径补充 |

主样本规模为 `1111676` 条岗位记录。

## 主脚本

```powershell
python src/analysis_static/a_extraction/build_51job_master_static.py
python src/analysis_static/a_extraction/finalize_51job_master_static.py
```

脚本负责：

- 统一字段 schema。
- 标准化城市、省份和岗位族。
- 基于规则词典抽取技能、任务和 GenAI 相关表达。
- 生成抽取结果表、复核表和质量报告。

## 主输出

| 输出 | 用途 |
| --- | --- |
| `analysis/tables/extraction_outputs/schema_mapping_static.json` | 字段映射说明 |
| `analysis/tables/extraction_outputs/city_mapping_table.csv` | 城市映射结果 |
| `analysis/tables/extraction_outputs/job_family_rules.csv` | 岗位族规则 |
| `analysis/tables/extraction_outputs/skill_extraction_table_final.csv` | 技能、任务和 GenAI 抽取结果 |
| `analysis/tables/extraction_outputs/*_manual_review.csv` | 低置信和复核样本 |

大体量全量中间表在 Release 数据包中归档，Git 仓库保留论文所需的紧凑输出和复核表。
