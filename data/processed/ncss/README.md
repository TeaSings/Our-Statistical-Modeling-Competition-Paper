# data/processed/ncss 说明

这个目录存放 NCSS 的清洗后结果和查询摘要。

## 当前文件

- `ncss_jobs_all_areas_clean.csv`：当前主用清洗表，快照为 `30985` 条职位
- `ncss_listings_all_areas_flat.csv`：全地区列表平铺表，覆盖 `41407` 个唯一职位
- `ncss_jobs_balanced_clean.csv`：第一版均衡样本清洗表
- `ncss_list_query_summary.csv`：第一版常规列表抓取摘要
- `ncss_list_query_summary_all_areas.csv`：全地区列表抓取摘要

## 当前建议

- 现在就开始做技能抽取、正文分析和招聘要求建模，用 `ncss_jobs_all_areas_clean.csv`
- 做城市覆盖、薪资和学历要求分析，用 `ncss_listings_all_areas_flat.csv`
- 写覆盖范围和样本形成过程时，配合 `ncss_list_query_summary_all_areas.csv`
- 小样本调试或方法对比时，再回看 `ncss_jobs_balanced_clean.csv`

## 当前样本形成链路

- 全国列表去重后唯一职位：`41407`
- 成功解析为有效详情：`41400`
- 清洗、标准化、去重后主表：`30985`
