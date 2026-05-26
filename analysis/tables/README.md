# analysis/tables 说明

这个目录保留论文最终写作和复核需要的表格。

## 子目录

| 路径 | 内容 |
| --- | --- |
| `paper_tables_csv/` | 正文表 1-10 及省级生成式 AI 强度等论文直接引用表 |
| `model_inputs/` | 回归/分组分析使用的紧凑模型输入表 |
| `city_small_sample_penalty/` | 城市小样本惩罚后的分类、排名和质量检查 |
| `extraction_outputs/` | 技能/任务抽取词典、人工复核表、最终抽取表和 schema |

## 未纳入 Git 的大表

以下文件保留在本地归档，不进入 GitHub：

```text
_local_archive_not_for_github/analysis/job_level_scored.csv
_local_archive_not_for_github/data/processed/analysis_static/interfaces/job_level_master_static.csv
_local_archive_not_for_github/data/processed/analysis_static/interfaces/job_level_panel_union_raw.csv
_local_archive_not_for_github/data/processed/analysis_static/a_extraction/job_level_panel_city_std.csv
_local_archive_not_for_github/data/processed/analysis_static/a_extraction/job_level_panel_job_family_std.csv
```

这些文件用于全量复现或深度复核；论文正文引用优先使用本目录内的紧凑表格。
