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

可以直接复制这一行：

```powershell
python src/platforms/job51/run_sequential_social_crawl.py --transport browser --browser-min-interval 0.6 --browser-max-retries 4 --browser-speed-profile balanced --manual-verify --manual-verify-wait 120 --workers 12 --plan-prefetch-areas 4 --page-size 50 --specific-only --refresh-clean --refresh-clean-every-batches 20 --refresh-clean-min-seconds 300
```

默认启动时会优先使用本地缓存的 51job taxonomy，不再每次都先在线刷新，所以浏览器能更快进入自动拉起阶段；如果你确实想强制更新缓存，再加：

```powershell
  --refresh-taxonomies `
  --taxonomy-timeout 12
```

如果你想在“不启用人工验证窗口”的前提下让浏览器模式跑快一些，可以把参数调成：

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

这个入口的默认特点：

- 单浏览器会话、低速顺序推进，尽量降低 `405` 和滑块重复触发概率
- 开启 `--manual-verify` 时，会自动启动一个本地可见浏览器并附着，不再要求手动先准备 `9222` 端口
- 如果自动启动时发现稳定 profile 正被旧浏览器进程占用，脚本会自动切到隔离 session profile 重试，避免出现“浏览器没起来、脚本也不动”的假死感
- 开启 `--manual-verify` 且 `--workers > 1` 时，会让多个 worker 共享同一个真实浏览器的验证态；任一 worker 触发滑块时，会全局暂停等待你处理一次
- 自动把当前职业、地区、批次位置写入 `data/raw/51job/manifests/51job_social_cursor_with_publish.json`
- 当前批次内部也会保存页级断点，中途中断后可从上次停下的位置继续
- 会持续写入 `data/raw/51job/manifests/51job_social_progress_with_publish.json`，供 watcher 读取
- 使用 UTF-8 输出，避免中文进度条乱码
- 遇到验证或页面卡住时，watcher 会提示“进度快照多久未更新”，并显示当前是否在等待共享浏览器的人工校验恢复

从这个版本开始，浏览器模式的速度控制逻辑也变了：

- `--workers` 在浏览器模式里终于会真实生效，不再只是 requests 分支有效
- 脚本会根据 `--workers`、`--page-size`、`--browser-min-interval` 自动推导有效并发，并在启动时打印 `Browser execution plan`
- 如果你开了 `--manual-verify`，脚本会自动切到“共享浏览器验证态”的自适应并发模式，而不是简单退回单通道
- `--browser-speed-profile` 可以在 `conservative / balanced / aggressive / max` 之间切换，更激进的档位会允许更高的有效并发
- `--browser-max-effective-workers` 可以额外手动压住 auto plan 的上限，适合不同网络环境下控速
- 顺序调度器现在会在进程启动时一次性加载已抓到的 `job_id` 去重集合，而不是每个 batch 都重扫整份 raw
- 顺序调度器现在还会把“职能 + 顶层地区”的分区规划结果缓存到 `data/raw/51job/manifests/51job_social_plan_cache_with_publish/`；当前 area 未命中缓存时，会顺手预规划后续连续几个 area，后面的 batch 会直接跳过这段规划开销
- `progress` 和 `cursor` 写盘现在默认做了节流，避免高频 JSON 落盘把调度器本身拖慢
- `--refresh-clean` 现在是周期刷新，而不是每个 batch 都重刷整份 clean CSV
- 如果你接入了 `--browser-cdp-url`，脚本同样会把有效并发压得更保守，因为这仍然是共享的真实浏览器窗口
- 分区规划阶段和正式抓取阶段都会吃到这套并发策略，不再只有“抓页阶段”可调

可以直接参考这三档常用参数：

- 稳定档：`--manual-verify --workers 8 --browser-speed-profile conservative --page-size 50 --browser-min-interval 0.6`
- 均衡档：`--manual-verify --workers 12 --plan-prefetch-areas 4 --browser-speed-profile balanced --page-size 50 --browser-min-interval 0.6`
- 提速档：`--manual-verify --workers 14 --plan-prefetch-areas 6 --browser-speed-profile aggressive --browser-max-effective-workers 14 --page-size 80 --browser-min-interval 0.35`

如果又开始频繁遇到 `405`、长时间无新增或滑块，请先回退到“稳定档”或“均衡档”；必要时直接把 `--browser-max-effective-workers` 压到 `2` 或 `3`。

### 单批慢速补跑

如果只想先补跑一个职能、并按全国顶层地区顺序一次推进一个区域，可使用：

```powershell
python src/platforms/job51/fetch_social_jobs.py `
  --transport browser `
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

同样地，如果这里不启用 `--manual-verify`，`--workers` 也会在浏览器模式里参与自适应并发，不再被静默忽略。

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
  --input data/raw/51job/records/51job_social_jobs_raw_with_publish.jsonl `
  --output data/processed/51job/51job_social_jobs_clean_with_publish.csv
```

新的社招 clean CSV 已包含这些时间相关字段：

- `publish_time_raw`
- `publish_time_std`
- `update_time_raw`
- `update_time_std`
- `apply_time_text_raw`
- `salary_bound_kind`
- `salary_pay_months`
- `salary_min_annualized`
- `salary_max_annualized`
- `salary_avg_annualized`
- `province_std`
- `district_std`
- `jd_char_count`

## watcher 进度条

如果你不在电脑前，推荐保留一个终端跑 crawler，另一个终端专门看进度条：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --interval 5
```

新版本 watcher 会额外显示：

- `Browser execution plan` 对应的 `req / plan / fetch`
- 当前自适应速度档位与显式上限：`profile / max`
- 当前是否正在等待共享浏览器的人工校验恢复
- 触发人工校验暂停的是哪个 worker
- 最近一次人工校验的开始时间和恢复时间

只想快速看一眼当前状态：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --once
```

如果你是在查看某个自定义 smoke 的 `progress.json`，并且想让 watcher 同时展示匹配的顺序调度器状态，记得显式传 `--cursor-file`；不传时它现在会默认隐藏无关的主线 cursor，避免把别的运行态混进来。

## 输出文件说明

- `data/input/51job/51job_search_area_tree.json`：地区树缓存
- `data/input/51job/51job_search_function_codes.json`：职能编码缓存
- `data/raw/51job/manifests/51job_social_plan_cache_with_publish/`：按“职能 + 顶层地区”缓存好的分区规划结果，供顺序调度器后续 batch 直接复用
- `data/raw/51job/manifests/51job_social_cursor_with_publish.json`：顺序调度器断点文件
- `data/raw/51job/manifests/51job_social_progress_with_publish.json`：watcher 进度快照
- `data/raw/51job/records/51job_social_jobs_raw_with_publish.jsonl`：社招带发布时间的 active RAW 检查点
- `data/processed/51job/51job_social_jobs_clean_with_publish.csv`：社招带发布时间的 active clean 检查点

## 清理旧数据

旧的无发布时间 social 快照、旧 cursor / progress / manifest / scheduler 日志，可以用下面这条命令一次性清理：

```powershell
python src/platforms/job51/cleanup_social_legacy_data.py --skip-runtime-profiles
```

如果你想显式调节顺序调度器自己的 I/O 和 clean 刷新频率，还可以加：

- `--progress-write-interval 2`
- `--cursor-write-interval 2`
- `--plan-prefetch-areas 4`
- `--refresh-clean-every-batches 20`
- `--refresh-clean-min-seconds 300`

其中 `cursor`、`progress` 和调度日志属于本地运行态；RAW 与 clean 快照则可以按阶段提交，供组员直接分析。
