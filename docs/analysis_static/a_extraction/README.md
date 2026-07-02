# 51job 静态抽取文档

本目录记录 51job 静态抽取流程。

| 文件 | 内容 |
| --- | --- |
| `01_static_extraction_overview.md` | 主输入、脚本和输出 |
| `02_city_standardization.md` | 城市和省份映射逻辑 |
| `03_job_family_standardization.md` | 岗位族分类逻辑 |
| `04_rule_dictionary_notes.md` | 技能、任务和 GenAI 词典 |
| `05_extraction_quality_report.md` | 最终覆盖率和质量指标 |
| `06_rule_iteration_notes.md` | 规则迭代和复核边界 |

对应脚本位于 `src/analysis_static/a_extraction/`，紧凑输出位于 `analysis/tables/extraction_outputs/`。
