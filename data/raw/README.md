# data/raw 说明

这个目录存放“已经抓到，但还没有完全标准化”的原始数据，按平台拆分。

## 子目录

- `51job/`：校招专题页的 RAW、HTML 快照，以及社招顺序抓取的运行态文件
- `ncss/`：NCSS 列表 RAW、详情 HTML 和主 manifest
- `mohrss/`：中国公共招聘网页面快照
- `zhaopin/`：智联公开详情页快照
- `stats/`：国家统计局与普查页面快照
- `occupation/`：职业分类页面快照
- `clds/`：CLDS 参考页面快照

## manifest 是什么

`manifest` 用来记录每次抓取尝试，常见字段包括：

- `fetched_at`
- `platform`
- `page_type`
- `url`
- `local_path`
- `status_code`
- `fetched`
- `error`

如果命令带了 `--skip-existing`，出现 `fetched=false` 往往只是说明本地文件已存在，并不等于抓取失败。

## 当前主线文件

- `51job/records/51job_campus_jobs_raw.jsonl`：51job 校招 RAW，`249` 条
- `51job/records/51job_social_jobs_raw_with_publish.jsonl`：51job 社招带发布时间的 active RAW 检查点，`8294` 条
- `51job/manifests/51job_campus_seed_manifest.jsonl`：51job 校招处理 manifest
- `51job/html/pages/`：51job 校招专题页 HTML 快照
- `ncss/records/ncss_listings_all_areas_raw.jsonl`：NCSS 全国列表 RAW
- `ncss/manifests/ncss_detail_manifest_all_areas.jsonl`：NCSS 全国详情主 manifest，`41407` 行
- `ncss/records/ncss_jobs_all_areas_raw.jsonl`：NCSS 全国详情 RAW，`41400` 条有效记录
- `ncss/html/ncss_jobs/detail/`：NCSS 详情 HTML 快照

## 哪些是数据快照，哪些是运行态

- `NCSS` 主线 RAW、`51job` 校招 RAW 和 `51job` 社招带发布时间检查点都可以作为分析或复核输入进入版本库
- `51job` 社招顺序抓取的 `cursor`、`progress` 和调度日志属于本地运行态，默认不提交
- 如果后续继续跑 51job 社招，新数据可以定期固化成新的 RAW checkpoint 再提交

## 使用建议

- 回看抓取过程先看 `manifest`
- 回看网页原貌先看 `html`
- 做建模不要直接用 `raw/`，优先转到 `processed/`
- NCSS 当前 `41407 -> 41400` 的差额来自源站已删除或正文为空，不代表主流程仍未完成
