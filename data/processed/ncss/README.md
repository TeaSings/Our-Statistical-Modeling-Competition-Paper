# data/processed/ncss 说明

这个目录存放 NCSS 的清洗后结果和查询摘要。

## 当前文件

- `ncss_jobs_balanced_clean.csv`：第一版均衡样本清洗表
- `ncss_listings_all_areas_flat.csv`：全地区列表平铺表，覆盖全部唯一职位
- `ncss_jobs_all_areas_clean.csv`：全地区详情快照清洗表
- `ncss_list_query_summary.csv`：第一版常规列表抓取摘要
- `ncss_list_query_summary_all_areas.csv`：全地区列表抓取摘要

## 当前建议

- 现在就开始做技能抽取，用 `ncss_jobs_balanced_clean.csv`
- 现在就开始做城市覆盖、薪资和学历要求分析，用 `ncss_listings_all_areas_flat.csv`
- 需要更大规模的正文样本，用 `ncss_jobs_all_areas_clean.csv`
- 写覆盖范围和样本形成过程时，配合 `ncss_list_query_summary_all_areas.csv`
- 后续全地区详情清洗表也继续写到这个目录
