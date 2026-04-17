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
  --browser-cdp-url http://127.0.0.1:9222 `
  --browser-min-interval 0.6 `
  --browser-max-retries 4 `
  --manual-verify `
  --manual-verify-wait 120 `
  --workers 1 `
  --page-size 50 `
  --specific-only
```

这个顺序调度器现在会同时保存：

- 批次级 cursor
- 当前批次的页级断点
- watcher 可读的进度快照

中途中断后再次执行同一条命令，会优先从上次停下的位置继续。

如果只想跑某一个职能、并按全国顶层地区顺序慢速推进：

```powershell
python src/platforms/job51/fetch_social_jobs.py `
  --transport browser `
  --browser-cdp-url http://127.0.0.1:9222 `
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
  --input data/raw/51job/records/51job_social_jobs_raw.jsonl `
  --output data/processed/51job/51job_social_jobs_clean.csv
```

实时进度条：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --interval 5
```

快速看一眼当前状态：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --once
```
