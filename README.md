# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

## 当前仓库能直接提供什么

- `data/processed/ncss/ncss_jobs_all_areas_clean.csv`：当前主线 JD 主表，`30985` 条清洗后职位
- `data/processed/ncss/ncss_listings_all_areas_flat.csv`：全国职位覆盖底表，`41407` 个唯一职位
- `data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`：全国 NCSS 详情 RAW，`41400` 条有效详情
- `data/processed/51job/51job_campus_jobs_clean.csv`：51job 校招专题页验证样本，`245` 条带真实 JD 的职位
- `data/processed/51job/51job_social_jobs_clean.csv`：51job 社招阶段性 clean 快照，当前 `13989` 条
- `data/raw/51job/records/51job_social_jobs_raw.jsonl`：51job 社招阶段性 RAW 快照，当前 `14141` 条
- `src/platforms/job51/`：51job 社招浏览器顺序抓取方案，支持全国顺序续跑、页级断点和 watcher 进度条
- `docs/NCSS全量数据核验与交付说明-2026-04-16.md`：当前 NCSS 主线数据最终核验说明

说明：

- `NCSS` 仍然是当前仓库里最完整、最稳定、最适合直接进入论文主体分析的数据源
- `51job` 社招链路已经跑通；RAW 与 clean 可按阶段提交为分析快照，`progress`、`cursor` 和调度日志仍然只保留在本地
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
  --manual-verify `
  --manual-verify-wait 120 `
  --workers 1 `
  --page-size 50 `
  --specific-only
```

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

运行时会持续写出以下本地产物：

- `data/raw/51job/manifests/51job_social_cursor.json`
- `data/raw/51job/manifests/51job_social_progress.json`
- `data/raw/51job/records/51job_social_jobs_raw.jsonl`
- `data/processed/51job/51job_social_jobs_clean.csv`

现在如果你开启了 `--manual-verify`，脚本会自动拉起一个本地可见浏览器并附着过去，不再要求你手动先准备 `http://127.0.0.1:9222`。

其中 `cursor`、`progress` 和调度日志属于运行态文件，默认不提交；`raw` 与 `clean` 则可以按阶段固化成数据快照，推送给组员直接分析。

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
