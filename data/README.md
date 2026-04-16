# 数据目录说明

`data/` 现在同时按两条逻辑组织：

1. 按数据生命周期分层：`input -> raw -> processed`
2. 在每一层内部按站点或来源分目录

整体可理解为：

```text
input/<site> -> raw/<site> -> processed/<site>
```

## 当前站点目录

- `input/ncss/`、`raw/ncss/`、`processed/ncss/`：当前主线数据源，已经形成全国范围可分析结果
- `input/mohrss/`、`raw/mohrss/`：中国公共招聘网相关种子和参考页面
- `input/zhaopin/`、`raw/zhaopin/`：智联公开详情页样例
- `input/sources/`：数据源注册表、参考页面清单和人工补链任务表
- `raw/stats/`、`raw/occupation/`、`raw/clds/`：官方统计、分类与参考资料快照

## 建议阅读顺序

- 看数据入口：先读 `input/README.md`
- 看原始抓取：再读 `raw/README.md`
- 看可分析数据：最后读 `processed/README.md`

## 当前主用文件

- `processed/ncss/ncss_jobs_all_areas_clean.csv`：当前主用 JD 数据，快照为 `30985` 条清洗后职位
- `processed/ncss/ncss_listings_all_areas_flat.csv`：NCSS 全地区列表平铺表，覆盖 `41407` 个唯一职位
- `raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`：NCSS 全地区详情解析原始表，当前为 `41400` 条有效详情
- `processed/ncss/ncss_list_query_summary_all_areas.csv`：NCSS 全地区列表查询摘要
- `input/ncss/ncss_detail_urls_all_areas.csv`：NCSS 全地区详情页种子
- `processed/ncss/ncss_jobs_balanced_clean.csv`：第一版均衡样本清洗表，适合小样本调试
