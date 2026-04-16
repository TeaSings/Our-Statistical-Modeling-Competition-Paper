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
| `watch_ncss_progress.py` | 实时显示 NCSS 详情抓取总进度和各分片进度条 |

## 辅助脚本

| 文件 | 作用 |
| --- | --- |
| `common.py` | 公共工具函数 |
| `extract_links.py` | 从列表页 HTML 中提取详情页链接 |
| `build_manual_seed_sheet.py` | 生成手工搜集链接的任务表 |
| `parse_job_pages_by_text.py` | 用文本规则解析详情页，适合调试非 NCSS 页面 |

## 推荐顺序

1. 先跑 `fetch_ncss_jobs.py`
2. 再用 `fetch_pages.py` 抓详情页
3. 用 `parse_details.py` 解析
4. 最后用 `clean_jobs.py` 输出清洗表

实时查看进度时直接运行：

```bash
python3 src/watch_ncss_progress.py
```

## 默认路径约定

- `data/input/ncss/`：NCSS 配置、地区码、种子和分片种子
- `data/raw/ncss/`：NCSS 原始列表、HTML 和 manifest
- `data/processed/ncss/`：NCSS 清洗后表和查询摘要
