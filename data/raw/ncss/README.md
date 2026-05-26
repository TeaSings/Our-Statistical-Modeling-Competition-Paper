# data/raw/ncss 说明

这个目录是当前项目最核心的 RAW 数据区。

## 子目录

- `html/`：NCSS 列表页和详情页 HTML 快照
- `manifests/`：列表抓取和详情抓取日志
- `records/`：列表接口 RAW 和详情解析 RAW

## 当前关键文件

- `records/ncss_listings_raw.jsonl`：第一版常规列表 RAW
- `records/ncss_listings_all_areas_raw.jsonl`：全国列表 RAW
- `records/ncss_jobs_balanced_raw.jsonl`：第一版均衡样本详情 RAW
- `records/ncss_jobs_all_areas_raw.jsonl`：全国主任务详情 RAW，`41400` 条有效记录
- `manifests/ncss_detail_manifest_balanced.jsonl`：均衡样本详情抓取日志
- `manifests/ncss_detail_manifest_all_areas.jsonl`：全国详情主 manifest，`41407` 行
- `manifests/shards/`：并发补抓任务的分片日志

## 现在这批数据意味着什么

- `records/ncss_listings_all_areas_raw.jsonl` 已覆盖全国列表扫描结果
- `html/ncss_jobs/detail/` 已覆盖正式主种子对应的详情页快照
- `41407` 个主种子中，`41400` 个已解析出有效详情
- 剩余 `7` 个是源站删除页或正文为空页，不是脚本未完成

## 使用建议

- 看全量覆盖度：优先读 `manifests/ncss_detail_manifest_all_areas.jsonl`
- 看真实详情正文：优先读 `records/ncss_jobs_all_areas_raw.jsonl`
- 想查看当前进度或补抓情况：使用 `src/platforms/ncss/watch_ncss_progress.py`
