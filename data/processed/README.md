# data/processed 说明

这个目录存放已经标准化、适合直接分析的结果表。

## 当前目录

- `ncss/`：NCSS 清洗表和查询摘要

## 当前主用文件

- `ncss/ncss_jobs_balanced_clean.csv`：第一版清洗后的 JD 主数据
- `ncss/ncss_listings_all_areas_flat.csv`：全地区列表平铺表，适合做城市覆盖、薪资和学历要求分析
- `ncss/ncss_jobs_all_areas_clean.csv`：全地区详情快照清洗表，适合做技能抽取和正文分析
- `ncss/ncss_list_query_summary.csv`：第一版均衡抓取查询摘要
- `ncss/ncss_list_query_summary_all_areas.csv`：全地区列表抓取摘要

## 使用建议

- 现在就要做文本分析，先用 `ncss/ncss_jobs_balanced_clean.csv`
- 需要更大规模正文样本时，用 `ncss/ncss_jobs_all_areas_clean.csv`
- 需要交代覆盖范围，配合 `ncss/ncss_list_query_summary_all_areas.csv`
- 后续新的清洗表也都放到 `processed/ncss/` 下，不再散落在顶层
