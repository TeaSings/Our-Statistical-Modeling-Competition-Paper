# src/platforms/job51 说明

这个目录存放前程无忧校招专题页的结构化脚本。

## 文件

- `coapi.py`：51job 校招 `coapi` 官方接口签名与请求封装
- `fetch_campus_jobs.py`：抓取并解析 51job 校招专题页，自动兼容三类模板
- `watch_51job_progress.py`：查看 51job 校招专题页种子处理、RAW 和 Clean 输出进度

## 当前支持的页面模板

1. `coapi` 动态职位页
2. `job.js` / 内联脚本内嵌职位数组页
3. 静态公告页 / 折叠职位详情页

## 推荐命令

```bash
python src/platforms/job51/fetch_campus_jobs.py --workers 12
```

```bash
python src/clean_jobs.py ^
  --input data/raw/51job/records/51job_campus_jobs_raw.jsonl ^
  --output data/processed/51job/51job_campus_jobs_clean.csv
```

```bash
python src/platforms/job51/watch_51job_progress.py --once
```
