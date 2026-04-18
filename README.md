# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

## 当前仓库能直接提供什么

- `data/processed/ncss/ncss_jobs_all_areas_clean.csv`：当前主线 JD 主表，`30985` 条清洗后职位
- `data/processed/ncss/ncss_listings_all_areas_flat.csv`：全国职位覆盖底表，`41407` 个唯一职位
- `data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`：全国 NCSS 详情 RAW，`41400` 条有效详情
- `data/processed/51job/51job_campus_jobs_clean.csv`：51job 校招专题页验证样本，`245` 条带真实 JD 的职位
- `data/processed/51job/51job_social_jobs_clean_with_publish.csv`：51job 社招带发布时间的 active clean 检查点，当前 `8184` 条
- `data/raw/51job/records/51job_social_jobs_raw_with_publish.jsonl`：51job 社招带发布时间的 active RAW 检查点，当前 `8294` 条
- `src/platforms/job51/`：51job 社招浏览器顺序抓取方案，支持全国顺序续跑、页级断点和 watcher 进度条
- `docs/NCSS全量数据核验与交付说明-2026-04-16.md`：当前 NCSS 主线数据最终核验说明

说明：

- `NCSS` 仍然是当前仓库里最完整、最稳定、最适合直接进入论文主体分析的数据源
- `51job` 社招链路当前保留的是带发布时间字段的新检查点，顺序调度器会默认续跑这一套 `_with_publish` 文件
- 论文 PDF 统一放在 `papers/pdfs/`，不再放在 `docs/`，也不再保留 `reference_library/` 或 `libraries/` 这一层

## 快速开始

以下命令都建议在仓库根目录运行：

```powershell
cd <repo-root>
```

安装依赖：

```powershell
python -m pip install -r requirements.txt
```

如果要启动 51job 社招顺序抓取，推荐保留两个终端：

终端 A 跑抓取器：

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

上面这条命令现在会默认写入并续跑 `_with_publish` 这一套 `raw / clean / cursor / progress / manifest` 文件，不再回到已经清理掉的旧 social 快照。

PowerShell 直接复制的一行命令如下：

```powershell
python src/platforms/job51/run_sequential_social_crawl.py --transport browser --browser-min-interval 0.6 --browser-max-retries 4 --browser-speed-profile balanced --manual-verify --manual-verify-wait 120 --workers 12 --plan-prefetch-areas 4 --page-size 50 --specific-only --refresh-clean --refresh-clean-every-batches 20 --refresh-clean-min-seconds 300
```

从这个版本开始，脚本默认优先使用本地缓存的 51job taxonomy，不再每次启动都先做一次在线刷新；如果你怀疑职能或地区缓存过旧，再额外补上：

```powershell
  --refresh-taxonomies `
  --taxonomy-timeout 12
```

如果你想在不启用人工滑块验证的前提下提速，可以把同一入口改成“自适应浏览器并发”模式，例如：

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

脚本启动后会打印一行 `Browser execution plan`，明确告诉你：

- 你请求了多少个 `workers`
- 规划分区阶段实际用了多少并发
- 抓取页数据阶段实际用了多少并发
- 如果被限速，是被 `--manual-verify`、`--browser-cdp-url`，还是被 `--browser-min-interval` / `--page-size` 的安全上限压住了

终端 B 看实时进度条：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --interval 5
```

只想看一眼当前状态时：

```powershell
python src/platforms/job51/watch_51job_progress.py --mode social --once
```

如果只做 NCSS 主线数据处理，最常用的是：

```powershell
python src/clean_jobs.py `
  --input data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl `
  --output data/processed/ncss/ncss_jobs_all_areas_clean.csv
```

## 51job 顺序抓取怎么理解

这条链路的目标不是“一次铺满全国”，而是以较稳定的低速会话按职业和地区顺序推进，尽量降低 `405` 和滑块重复触发的概率。

从这个版本开始，浏览器模式不再是“表面上能调 `workers`、实际上仍然串行”：

- 未开启 `--manual-verify` 时，脚本会根据 `--workers`、`--page-size` 和 `--browser-min-interval` 自动计算有效并发
- 开启 `--manual-verify` 时，脚本会把多个 worker 收敛到同一个真实可见浏览器里，共享验证态并在需要时全局暂停等待你处理滑块
- `--browser-speed-profile` 可以在 `conservative / balanced / aggressive / max` 之间切换，更激进的档位会允许更高的有效并发
- `--browser-max-effective-workers` 可以再加一道硬上限，防止激进档位在你的网络环境里放得过快
- 显式使用 `--browser-cdp-url` 时，也会走共享浏览器会话，但有效并发仍会被压得更保守
- 分区规划阶段也会一起吃到这套并发策略，不再是一条一条探测全国分区
- 顺序调度器现在会在进程启动时一次性加载已抓到的 `job_id` 去重集合，而不是每个 batch 都重扫整份 raw
- 顺序调度器现在还会把“职能 + 顶层地区”的分区规划结果缓存到 `data/raw/51job/manifests/51job_social_plan_cache_with_publish/`；当前 area 未命中缓存时，会顺手预规划后续连续几个 area，后面的 batch 会直接跳过这段规划开销
- `progress` 和 `cursor` 写盘现在默认做了节流，避免高频 JSON 落盘把调度器本身拖慢
- `--refresh-clean` 现在是周期刷新，而不是每个 batch 都重刷整份 clean CSV

可以把它理解成三档常用速度：

- 稳定档：`--manual-verify --workers 8 --browser-speed-profile conservative --page-size 50 --browser-min-interval 0.6`
- 均衡档：`--manual-verify --workers 12 --plan-prefetch-areas 4 --browser-speed-profile balanced --page-size 50 --browser-min-interval 0.6`
- 提速档：`--manual-verify --workers 14 --plan-prefetch-areas 6 --browser-speed-profile aggressive --browser-max-effective-workers 14 --page-size 80 --browser-min-interval 0.35`

如果网络刚换过、站点风控偏敏感，建议先从“稳定档”起跑；如果又开始出现 `405` 或频繁滑块，就先把 `browser-speed-profile` 降回 `conservative`，或者把 `--browser-max-effective-workers` 压到 `2` 或 `3`。

运行时会持续写出以下本地产物：

- `data/raw/51job/manifests/51job_social_cursor_with_publish.json`
- `data/raw/51job/manifests/51job_social_progress_with_publish.json`
- `data/raw/51job/records/51job_social_jobs_raw_with_publish.jsonl`
- `data/processed/51job/51job_social_jobs_clean_with_publish.csv`

现在如果你开启了 `--manual-verify`，脚本会自动拉起一个本地可见浏览器并附着过去，不再要求你手动先准备 `http://127.0.0.1:9222`。

如果自动拉起的浏览器因为旧 profile 仍被占用而没有成功附着，新版本会自动改用一个隔离 session profile 重试，不会再悄悄卡死在“浏览器似乎没起来”的状态。

新版本里 watcher 也会额外显示：

- 当前浏览器计划并发：`req / plan / fetch`
- 当前自适应速度档位与显式上限：`profile / max`
- 是否正在等待人工校验
- 是哪个 worker 触发了共享浏览器的全局暂停
- 最近一次人工校验的开始与恢复时间

如果你是在看某个自定义 smoke 的 `progress.json`，记得同时传对应的 `--cursor-file`；否则 watcher 会默认读取主线顺序调度器的 cursor。

其中 `cursor`、`progress` 和调度日志属于运行态文件，默认不提交；`raw` 与 `clean` 则可以按阶段固化成数据快照，推送给组员直接分析。

如果需要顺手清掉旧的社招快照和旧日志，可以运行：

```powershell
python src/platforms/job51/cleanup_social_legacy_data.py --skip-runtime-profiles
```

如果你想进一步调节顺序调度器自己的开销，还可以显式传：

- `--progress-write-interval 2`
- `--cursor-write-interval 2`
- `--refresh-clean-every-batches 20`
- `--refresh-clean-min-seconds 300`

`51job` 社招 clean 输出现在已经包含这些时间相关字段：

- `publish_time_raw`
- `publish_time_std`
- `update_time_raw`
- `update_time_std`
- `apply_time_text_raw`

## 项目结构

```text
.
├── data/
│   ├── input/
│   │   ├── 51job/
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── sources/
│   │   └── zhaopin/
│   ├── raw/
│   │   ├── 51job/
│   │   ├── clds/
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── occupation/
│   │   ├── stats/
│   │   └── zhaopin/
│   ├── processed/
│   │   ├── 51job/
│   │   └── ncss/
│   └── runtime/
├── docs/
├── papers/
│   ├── pdfs/
│   └── download_results.csv
├── src/
│   ├── platforms/
│   │   ├── job51/
│   │   └── ncss/
│   ├── tools/
│   ├── clean_jobs.py
│   └── common.py
└── requirements.txt
```

## 目录怎么读

- `data/`：按 `input -> raw -> processed` 的数据生命周期分层，并在每一层内按平台拆分
- `data/runtime/`：浏览器 profile 和其他本地运行态目录，不纳入版本库
- `docs/`：过程记录、核验说明和方法笔记
- `papers/`：论文 PDF 正文和下载记录；这里只放论文文件，不放研究笔记
- `src/platforms/`：按站点拆分的正式抓取与解析方案
- `src/tools/`：跨平台辅助脚本
- `src/clean_jobs.py`：各平台 RAW 统一清洗入口
- `src/common.py`：共享工具函数

## README 索引

- `data/README.md`：数据目录总说明
- `data/input/README.md`：输入种子、配置文件和来源登记
- `data/raw/README.md`：原始抓取产物、manifest 和 HTML 快照
- `data/processed/README.md`：清洗后结果表和主用口径
- `src/README.md`：脚本入口总说明
- `src/platforms/job51/README.md`：51job 校招与社招方案
- `src/platforms/ncss/README.md`：NCSS 主线脚本说明
- `src/tools/README.md`：跨平台辅助脚本说明
- `docs/README.md`：项目文档索引
- `papers/README.md`：论文库说明
