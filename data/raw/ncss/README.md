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
- `records/ncss_jobs_all_areas_raw.jsonl`：全国主任务详情解析结果，当前为 `41400` 条有效记录
- `manifests/ncss_detail_manifest_balanced.jsonl`：均衡样本详情抓取日志
- `manifests/ncss_detail_manifest_all_areas.jsonl`：全地区详情抓取主日志，当前为 `41407` 行
- `manifests/shards/`：全地区详情并发补抓日志

## 说明

- `records/ncss_listings_all_areas_raw.jsonl` 已经覆盖全地区列表扫描结果
- `html/ncss_jobs/detail/` 已覆盖全部正式种子；目录中额外 HTML 主要是历史运行留下的附加快照
- 当前 `41407` 个种子中，`41400` 个已经解析为有效详情；剩余 `7` 个属于源站删除页或正文为空的异常页
- 如果要统计当前详情覆盖度，优先看 `manifests/`、`records/` 和 `src/watch_ncss_progress.py`
