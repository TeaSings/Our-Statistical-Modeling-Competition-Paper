# 51job 脚本

本目录保存 51job 校招和社招数据采集脚本。

| 文件 | 内容 |
| --- | --- |
| `fetch_campus_jobs.py` | 采集并解析校园招聘页面 |
| `fetch_social_jobs.py` | 按职能和地区采集社招搜索结果 |
| `run_sequential_social_crawl.py` | 支持断点续跑的社招顺序抓取器 |
| `browser_search_client.py` | 支持人工验证的浏览器搜索客户端 |
| `watch_51job_progress.py` | 抓取进度监控 |
| `search_taxonomy.py` | 地区和职能分类提取 |
| `import_search_har.py` | HAR 文件导入 JSONL |

大体量抓取产物的恢复位置见 `data/raw/51job/`。
