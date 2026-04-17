# data/processed 说明

这个目录存放已经清洗、标准化、适合直接分析的结果表。

## 当前目录

- `51job/`：51job 校招样本 clean 表，以及社招顺序抓取形成的阶段性 clean 快照
- `ncss/`：NCSS 主线 clean 表和查询摘要

## 当前主用文件

- `ncss/ncss_jobs_all_areas_clean.csv`：当前主线 JD 主表，`30985` 条
- `ncss/ncss_listings_all_areas_flat.csv`：全国职位覆盖底表，`41407` 个唯一职位
- `ncss/ncss_list_query_summary_all_areas.csv`：全国列表抓取摘要
- `51job/51job_campus_jobs_clean.csv`：51job 校招 clean 样本，`245` 条
- `51job/51job_social_jobs_clean.csv`：51job 社招阶段性 clean 快照，`13989` 条

## 使用建议

- 现在就开始正文分析、技能抽取和招聘要求建模：优先用 `ncss/ncss_jobs_all_areas_clean.csv`
- 需要说明全国覆盖范围：配合 `ncss/ncss_listings_all_areas_flat.csv` 与 `ncss/ncss_list_query_summary_all_areas.csv`
- 需要对比市场化岗位和 NCSS 的正文质量：再看 `51job/51job_social_jobs_clean.csv`
