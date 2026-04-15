# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

## 现在的目标

先把一条最小可行的数据流水线跑通：

1. 准备搜索页种子链接
2. 抓列表页 HTML
3. 从列表页提取详情页链接
4. 抓详情页 HTML
5. 解析招聘字段
6. 清洗和去重

## 目录

- `docs/`: 简短说明文档
- `src/`: 抓取和清洗脚本
- `data/input/`: 手工准备的配置和种子文件
- `data/raw`: 原始网页、链接和记录
- `data/processed`: 清洗后的结果
- `papers/`: 参考论文

## 快速开始

先安装依赖：

```bash
python3 -m pip install -r requirements.txt
```

再按这个顺序跑：

```bash
python3 src/fetch_pages.py --seed-file data/input/seeds_example.csv --manifest data/raw/list_fetch_manifest.jsonl
python3 src/extract_links.py --manifest data/raw/list_fetch_manifest.jsonl --config data/input/platform_template.json --output data/raw/links/detail_urls.csv
python3 src/fetch_pages.py --seed-file data/raw/links/detail_urls.csv --manifest data/raw/detail_fetch_manifest.jsonl
python3 src/parse_details.py --manifest data/raw/detail_fetch_manifest.jsonl --config data/input/platform_template.json --output data/raw/records/jobs_raw.jsonl
python3 src/clean_jobs.py --input data/raw/records/jobs_raw.jsonl --output data/processed/jobs_clean.csv
```

注意：`platform_template.json` 里的 CSS 选择器只是模板，你们需要打开真实网页后自己替换成实际选择器。
