# src 说明

这个目录存放项目脚本，当前默认围绕 `NCSS` 主线组织。

## 主流程脚本

| 文件 | 作用 |
| --- | --- |
| `fetch_ncss_jobs.py` | 抓取 NCSS 列表接口，输出原始列表、详情页种子和查询摘要 |
| `extract_ncss_area_codes.py` | 从 NCSS 列表页 HTML 提取全部地区代码 |
| `build_balanced_ncss_detail_seeds.py` | 从 NCSS 列表结果中均衡抽样详情页种子 |
| `fetch_pages.py` | 根据种子抓取详情页或参考页面 HTML |
| `parse_details.py` | 按选择器解析一个或多个 manifest 对应的详情页字段 |
| `clean_jobs.py` | 清洗、标准化、去重并输出分析用 CSV |
| `watch_ncss_progress.py` | 实时显示 NCSS 全量抓取/解析进度、分片速度和 ETA |
| `rebuild_local_manifest.py` | 当 manifest 中断或跨机器失效时，根据种子和本地 HTML 重建可用 manifest |

## 辅助脚本

| 文件 | 作用 |
| --- | --- |
| `common.py` | 公共工具函数 |
| `extract_links.py` | 从列表页 HTML 中提取详情页链接 |
| `build_manual_seed_sheet.py` | 生成手工搜集链接的任务表 |
| `parse_job_pages_by_text.py` | 用文本规则解析详情页，适合调试非 NCSS 页面 |

## 推荐顺序

1. 先跑 `fetch_ncss_jobs.py` 生成全地区列表和详情种子
2. 再用 `fetch_pages.py` 抓详情页 HTML
3. 用 `parse_details.py` 从种子或 manifest 解析详情
4. 必要时用 `rebuild_local_manifest.py` 从本地 HTML 重建 manifest
5. 最后用 `clean_jobs.py` 输出清洗表

实时查看进度时直接运行：

```bash
python src/watch_ncss_progress.py
```

只看一次当前快照时运行：

```bash
python src/watch_ncss_progress.py --once
```

## 当前推荐全量命令

```bash
python src/fetch_pages.py ^
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv ^
  --manifest data/raw/ncss/manifests/ncss_detail_manifest_all_areas.jsonl ^
  --output-dir data/raw/ncss/html ^
  --config data/input/ncss/platform_ncss_detail.json ^
  --skip-existing ^
  --workers 16
```

```bash
python src/parse_details.py ^
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv ^
  --config data/input/ncss/platform_ncss_detail.json ^
  --output data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl ^
  --overwrite ^
  --workers 16
```

```bash
python src/clean_jobs.py ^
  --input data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl ^
  --output data/processed/ncss/ncss_jobs_all_areas_clean.csv
```

## 默认路径约定

- `data/input/ncss/`：NCSS 配置、地区码、种子和分片种子
- `data/raw/ncss/`：NCSS 原始列表、HTML 和 manifest
- `data/processed/ncss/`：NCSS 清洗后表和查询摘要
