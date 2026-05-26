# 静态分析实现模块

本文件将早期 `analysis-method` 分支的协作设想整理为当前仓库的实现模块。当前主线已经收束为静态版 `city × job_family` 分析，不再把多人任务拆分写成进度记录。

## 模块 A：标准化与抽取

| 内容 | 实现 |
| --- | --- |
| 输入 | `data/processed/51job/51job_social_jobs_clean_with_publish.csv` |
| 脚本 | `src/analysis_static/a_extraction/build_51job_master_static.py`、`finalize_51job_master_static.py` |
| 输出 | `analysis/tables/extraction_outputs/` |
| 文档 | `docs/analysis_static/a_extraction/` |

模块 A 完成城市标准化、岗位族标准化、技能任务抽取、GenAI 标签构造和质量报告。

## 模块 B：指标与模型

| 内容 | 实现 |
| --- | --- |
| 输入 | 标准化后的 job-level 抽取结果 |
| 主分析表 | `analysis/tables/model_inputs/panel_cg_model.csv` |
| 城市表 | `analysis/tables/model_inputs/city_classification.csv` |
| 正文表 | `analysis/tables/paper_tables_csv/` |

模块 B 把 job-level 抽取结果聚合为城市、岗位族和城市-岗位族指标，并形成论文的主回归、机制、异质性和稳健性表。

## 模块 C：论文展示

| 内容 | 实现 |
| --- | --- |
| 图形 | `analysis/figures/` |
| 表格 | `analysis/tables/paper_tables_csv/` |
| 论文 | `analysis/manuscript/` |
| 参考资料 | `analysis/references/` |

模块 C 的当前交付形态是论文图表和文稿，不再包含早期设想中的独立交互式地图应用。若后续扩展可视化产品，应以 `analysis/tables/model_inputs/` 为数据接口。

## 接口约定

| 接口 | 用途 |
| --- | --- |
| `skill_extraction_table_final.csv` | 岗位级技能、任务和 GenAI 抽取结果 |
| `panel_cg_model.csv` | 城市-岗位族模型输入 |
| `city_classification.csv` | 城市分组、排名和展示指标 |
| `paper_tables_csv/` | 正文表格的最终 CSV 版本 |

所有大体量中间表在 GitHub Release 数据包中归档；Git 仓库只保留小型接口表、论文表格、代码和说明文档。
