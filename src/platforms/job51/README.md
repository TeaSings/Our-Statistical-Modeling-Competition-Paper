# src/platforms/job51 说明

这个目录存放 51job 抓取与导入脚本，覆盖两条链路：

- 校招专题页抓取
- `we.51job.com` 社招搜索页的浏览器顺序抓取

## 文件

- `coapi.py`：51job 校招 `coapi` 官方接口签名与请求封装
- `fetch_campus_jobs.py`：抓取并解析 51job 校招专题页，自动兼容三类模板
- `fetch_social_jobs.py`：按指定职能和地区抓取社招搜索结果，支持浏览器附着与增量追加
- `run_sequential_social_crawl.py`：稳定低速的社招顺序调度器，负责全国顺序推进和断点续跑
- `browser_search_client.py`：浏览器会话客户端，支持可见浏览器、人工验证和 CDP 附着
- `we_search_client.py`：51job 搜索 API / 页面请求层
- `search_taxonomy.py`：从搜索前端 bundle 提取地区树和职能编码
- `import_search_har.py`：把浏览器导出的 HAR 导入为统一 RAW JSONL
- `watch_51job_progress.py`：查看校招或社招抓取进度；社招模式可显示 cursor 状态

## 当前支持的校招页面模板

1. `coapi` 动态职位页
2. `job.js` / 内联脚本嵌入职位数组页
3. 静态公告页 / 折叠详情页

## 推荐运行方式

所有命令都建议在仓库根目录运行：

```powershell
cd <repo-root>
```

### 校招专题页

```powershell
python src/platforms/job51/fetch_campus_jobs.py --workers 12
```

### 社招顺序抓取

如果目标是“全国顺序逐步推进”而不是同时铺开多个城市，优先使用顺序调度器：

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

这个入口的默认特点：

- 单浏览器会话、低速顺序推进，尽量降低 `405` 和滑块重复触发概率
- 自动把当前职业、地区、批次位置写入 `data/raw/51job/manifests/51job_social_cursor.json`
- 当前批次内部也会保存页级断点，中途中断后可从上次停下的位置继续
- 会持续写入 `data/raw/51job/manifests/51job_social_progress.json`，供 watcher 读取
- 使用 UTF-8 输出，避免中文进度条乱码
- 遇到验证或页面卡住时，watcher 会提示“进度快照多久未更新”

### 单批慢速补跑

如果只想先补跑一个职能、并按全国顶层地区顺序一次推进一个区域，可使用：

```powershell
python src/platforms/job51/fetch_social_jobs.py `
  --transport browser `
  --browser-cdp-url http://127.0.0.1:9222 `
  --browser-min-interval 0.6 `
  --browser-max-retries 4 `
  --manual-verify `
  --manual-verify-wait 120 `
  --function-code 0106 `
  --top-level-area-offset 0 `
  --top-level-area-limit 1 `
  --page-size 50 `
  --append-output `
  --append-manifest
```

这组参数表示：

- 一次只跑一个顶层地区
- RAW 和 manifest 采用追加模式，不从头覆盖
- 适合网络波动、人工验证和 `405` 风险仍存在时的慢速增量推进

### HAR 导入

```powershell
python src/platforms/job51/import_search_har.py `
  --har path\to\51job_search.har `
  --output-raw data/raw/51job/records/51job_social_jobs_raw_from_har.jsonl
```

### 清洗

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

## watcher 进度条

如果你不在电脑前，推荐保留一个终端跑 crawler，另一个终端专门看进度条：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --interval 5
```

只想快速看一眼当前状态：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --once
```

## 输出文件说明

- `data/input/51job/51job_search_area_tree.json`：地区树缓存
- `data/input/51job/51job_search_function_codes.json`：职能编码缓存
- `data/raw/51job/manifests/51job_social_cursor.json`：顺序调度器断点文件
- `data/raw/51job/manifests/51job_social_progress.json`：watcher 进度快照
- `data/raw/51job/records/51job_social_jobs_raw.jsonl`：社招阶段性 RAW 快照
- `data/processed/51job/51job_social_jobs_clean.csv`：社招阶段性 clean 快照

其中 `cursor`、`progress` 和调度日志属于本地运行态；RAW 与 clean 快照则可以按阶段提交，供组员直接分析。
