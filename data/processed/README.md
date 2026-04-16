# data/processed 说明

这个目录存放已经标准化、适合直接分析的结果表。

## 当前目录

- `51job/`：前程无忧校招专题页清洗表
- `ncss/`：NCSS 清洗表和查询摘要

## 当前主用文件

- `51job/51job_campus_jobs_clean.csv`：当前 51job 校招专题页清洗表，快照为 `245` 条
- `ncss/ncss_jobs_all_areas_clean.csv`：当前主用 JD 数据，快照为 `30985` 条清洗后职位
- `ncss/ncss_listings_all_areas_flat.csv`：全地区列表平铺表，适合做城市覆盖、薪资和学历要求分析
- `ncss/ncss_jobs_balanced_clean.csv`：第一版均衡样本清洗表，适合小样本调试
- `ncss/ncss_list_query_summary.csv`：第一版均衡抓取查询摘要
- `ncss/ncss_list_query_summary_all_areas.csv`：全地区列表抓取摘要

## 使用建议

- 需要对比 NCSS 与 51job 的 JD 质量时，可先看 `51job/51job_campus_jobs_clean.csv`
- 现在就要做正文分析或技能抽取，优先用 `ncss/ncss_jobs_all_areas_clean.csv`
- 需要交代覆盖范围，配合 `ncss/ncss_list_query_summary_all_areas.csv`
- 需要全国职位总覆盖口径时，配合 `ncss/ncss_listings_all_areas_flat.csv`
- 后续新的清洗表也都放到 `processed/ncss/` 下，不再散落在顶层
