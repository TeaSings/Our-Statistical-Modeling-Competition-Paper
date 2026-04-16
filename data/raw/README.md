# data/raw 说明

这个目录存放“已经抓到，但还没有完全标准化”的原始数据，已经按站点分目录。

## 子目录

- `ncss/`：NCSS 列表原始记录、详情页 HTML、抓取 manifest
- `mohrss/`：中国公共招聘网相关 HTML 快照
- `zhaopin/`：智联公开详情页快照
- `stats/`：国家统计局和普查相关页面快照
- `occupation/`：职业分类页面快照
- `clds/`：CLDS 参考页面快照

## manifest 文件说明

`manifest` 用来记录每次抓取尝试，常见字段包括：

- `fetched_at`
- `platform`
- `page_type`
- `url`
- `local_path`
- `status_code`
- `fetched`
- `error`

如果命令带了 `--skip-existing`，出现 `fetched=false` 往往表示本地已存在该 HTML，并不等于失败。

## 当前主线文件

- `ncss/records/ncss_listings_all_areas_raw.jsonl`：NCSS 全地区列表原始记录
- `ncss/manifests/ncss_list_query_progress_all_areas.jsonl`：NCSS 全地区列表抓取进度日志
- `ncss/manifests/ncss_detail_manifest_all_areas.jsonl`：NCSS 全地区详情抓取主 manifest，当前为 `41407` 行
- `ncss/records/ncss_jobs_all_areas_raw.jsonl`：NCSS 全地区详情解析原始表，当前为 `41400` 条有效记录
- `ncss/manifests/shards/`：NCSS 详情页并发补抓分片 manifest
- `ncss/html/ncss_jobs/detail/`：NCSS 详情页 HTML 快照

## 使用建议

- 回溯抓取过程先看 `manifest`
- 回看网页原貌先看 `html`
- 想做建模不要直接用 `raw/`，优先转到 `processed/`
- 当前 `41407 -> 41400` 的差额主要来自源站已删除或正文为空的异常页面，不代表主流程仍未完成
