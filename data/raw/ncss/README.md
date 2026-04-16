# data/raw/ncss 说明

这个目录是当前项目的核心原始数据区。

## 子目录

- `html/`：NCSS 列表页和详情页 HTML 快照
- `manifests/`：NCSS 列表与详情抓取日志
- `records/`：NCSS 列表接口原始记录、详情解析原始记录

## 当前关键文件

- `records/ncss_listings_raw.jsonl`：第一版常规列表原始结果
- `records/ncss_listings_all_areas_raw.jsonl`：全地区列表原始结果
- `records/ncss_jobs_balanced_raw.jsonl`：第一版均衡样本详情解析结果
- `manifests/ncss_detail_manifest_balanced.jsonl`：均衡样本详情抓取日志
- `manifests/ncss_detail_manifest_all_areas.jsonl`：全地区详情抓取主日志
- `manifests/shards/`：全地区详情并发补抓日志

## 说明

- `records/ncss_listings_all_areas_raw.jsonl` 已经覆盖全地区列表扫描结果
- 详情页抓取仍会持续把更多 HTML 写入 `html/ncss_jobs/detail/`
- 如果要统计当前详情覆盖度，优先看 `manifests/` 和 `html/`
