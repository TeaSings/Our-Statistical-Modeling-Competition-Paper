# src 说明

这个目录存放项目脚本。当前结构已经按“平台方案”和“跨平台工具”拆开，避免不同网站的脚本混在一起。

## 目录分工

| 路径 | 作用 |
| --- | --- |
| `clean_jobs.py` | 各平台 RAW 统一清洗入口 |
| `common.py` | 公共工具函数 |
| `platforms/ncss/` | NCSS 列表抓取、详情抓取、解析、进度监控和 manifest 重建 |
| `platforms/job51/` | 51job 校招专题页与社招搜索抓取方案 |
| `tools/` | 不属于单一平台的辅助脚本 |

## 结构迁移说明

旧的顶层脚本已经按职责迁移：

- 原 `src/fetch_ncss_jobs.py` 等 NCSS 脚本迁入 `src/platforms/ncss/`
- 原 `src/build_manual_seed_sheet.py`、`src/extract_links.py`、`src/parse_job_pages_by_text.py` 迁入 `src/tools/`
- 这样做的目的，是让每个平台的抓取、解析和监控脚本都能在各自目录中闭环

## NCSS 推荐命令

```powershell
python src/platforms/ncss/fetch_ncss_jobs.py --resume
```

```powershell
python src/platforms/ncss/fetch_pages.py `
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv `
  --manifest data/raw/ncss/manifests/ncss_detail_manifest_all_areas.jsonl `
  --output-dir data/raw/ncss/html `
  --config data/input/ncss/platform_ncss_detail.json `
  --skip-existing `
  --workers 16
```

```powershell
python src/platforms/ncss/parse_details.py `
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv `
  --config data/input/ncss/platform_ncss_detail.json `
  --output data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl `
  --overwrite `
  --workers 16
```

```powershell
python src/platforms/ncss/watch_ncss_progress.py --once
```

## 51job 推荐命令

以下命令建议统一在仓库根目录运行：

```powershell
cd <repo-root>
```

校招专题页：

```powershell
python src/platforms/job51/fetch_campus_jobs.py --workers 12
```

社招顺序抓取：

```powershell
python src/platforms/job51/run_sequential_social_crawl.py `
  --transport browser `
  --browser-min-interval 0.6 `
  --browser-max-retries 4 `
  --browser-speed-profile balanced `
  --manual-verify `
  --manual-verify-wait 120 `
  --workers 12 `
  --plan-prefetch-areas 4 `
  --page-size 50 `
  --specific-only `
  --refresh-clean `
  --refresh-clean-every-batches 20 `
  --refresh-clean-min-seconds 300
```

这个入口现在会默认写入并续跑 `_with_publish` 这一套 `raw / clean / cursor / progress / manifest` 文件，不再回到已经清理掉的旧 social 快照。

可直接复制这一行：

```powershell
python src/platforms/job51/run_sequential_social_crawl.py --transport browser --browser-min-interval 0.6 --browser-max-retries 4 --browser-speed-profile balanced --manual-verify --manual-verify-wait 120 --workers 12 --plan-prefetch-areas 4 --page-size 50 --specific-only --refresh-clean --refresh-clean-every-batches 20 --refresh-clean-min-seconds 300
```

默认启动时会优先使用本地缓存的 51job taxonomy，因此浏览器会更早拉起；只有你显式传 `--refresh-taxonomies` 时，脚本才会在启动前做一次在线刷新。

如果当前网络比较稳，且你想让浏览器模式真正提速，可以改成：

```powershell
python src/platforms/job51/run_sequential_social_crawl.py `
  --transport browser `
  --workers 14 `
  --browser-speed-profile aggressive `
  --browser-max-effective-workers 14 `
  --page-size 80 `
  --browser-min-interval 0.35 `
  --browser-max-retries 4 `
  --specific-only
```

这个顺序调度器现在会同时保存：

- 批次级 cursor
- 当前批次的页级断点
- watcher 可读的进度快照

中途中断后再次执行同一条命令，会优先从上次停下的位置继续。

关于速度要特别注意：

- 浏览器模式现在会真正读取 `--workers`
- 但它不会无脑开满，而是根据 `--workers`、`--page-size`、`--browser-min-interval` 自动算出一个有效并发
- 一旦开启 `--manual-verify`，脚本会改成“共享一个真实可见浏览器 + 多个 worker 共用验证态”的自适应并发模式
- `--browser-speed-profile` 可以在 `conservative / balanced / aggressive / max` 之间切换，更激进的档位会允许更高的有效并发
- `--browser-max-effective-workers` 可以手动压住 auto plan 的上限，方便你按网络质量控速
- 顺序调度器现在会在进程启动时一次性加载已抓到的 `job_id` 去重集合，而不是每个 batch 都重扫整份 raw
- 顺序调度器现在还会把“职能 + 顶层地区”的分区规划结果缓存到 `data/raw/51job/manifests/51job_social_plan_cache_with_publish/`；当前 area 未命中缓存时，会顺手预规划后续连续几个 area，后面的 batch 会直接跳过这段规划开销
- `progress` 和 `cursor` 写盘现在默认做了节流，避免高频 JSON 落盘把调度器本身拖慢
- `--refresh-clean` 现在是周期刷新，而不是每个 batch 都重刷整份 clean CSV
- 如果自动拉起浏览器时发现旧 profile 仍被占用，脚本会自动切到隔离 session profile 重试，避免看起来像“浏览器没有正常起来”
- 启动日志里的 `Browser execution plan` 会把“请求值”和“实际值”都打印出来，方便判断是脚本慢，还是你当前参数本来就被安全策略压住了
- watcher 现在也会显示人工校验是否正在全局暂停、是哪一个 worker 触发的，以及最近一次恢复时间

如果只想跑某一个职能、并按全国顶层地区顺序慢速推进：

```powershell
python src/platforms/job51/fetch_social_jobs.py `
  --transport browser `
  --browser-min-interval 0.6 `
  --browser-max-retries 4 `
  --function-code 0106 `
  --top-level-area-offset 0 `
  --top-level-area-limit 1 `
  --append-output `
  --append-manifest
```

清洗输出：

```powershell
python src/clean_jobs.py `
  --input data/raw/51job/records/51job_campus_jobs_raw.jsonl `
  --output data/processed/51job/51job_campus_jobs_clean.csv
```

```powershell
python src/clean_jobs.py `
  --input data/raw/51job/records/51job_social_jobs_raw_with_publish.jsonl `
  --output data/processed/51job/51job_social_jobs_clean_with_publish.csv
```

新的社招 clean CSV 会包含：

- `publish_time_raw`
- `publish_time_std`
- `update_time_raw`
- `update_time_std`
- `apply_time_text_raw`

实时进度条：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --interval 5
```

快速看一眼当前状态：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --once
```

如果你是在查看某个单独 smoke 生成的 `progress.json`，建议把对应的 `--cursor-file` 也一起传进去；否则 watcher 默认会展示主线顺序调度器的 cursor。

清理已经退场的旧 social 快照和旧日志：

```powershell
python src/platforms/job51/cleanup_social_legacy_data.py --skip-runtime-profiles
```

如果你想显式控制顺序调度器自己的 I/O 开销，还可以加：

- `--progress-write-interval 2`
- `--cursor-write-interval 2`
- `--refresh-clean-every-batches 20`
- `--refresh-clean-min-seconds 300`
