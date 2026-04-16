# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

## 当前交付状态

- 主体可分析 JD 数据：`data/processed/ncss/ncss_jobs_all_areas_clean.csv`，当前快照为 `30985` 条清洗后职位
- 全国职位覆盖底表：`data/processed/ncss/ncss_listings_all_areas_flat.csv`，覆盖 `41407` 个唯一职位
- 全量详情解析原始表：`data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`，当前为 `41400` 条有效详情
- 51job 校招专题页验证样本：`data/processed/51job/51job_campus_jobs_clean.csv`，当前快照为 `245` 条带真实 JD 的清洗后职位
- 最终核验与交付说明：`docs/NCSS全量数据核验与交付说明-2026-04-16.md`

## 项目结构

```text
.
├── data/
│   ├── README.md
│   ├── input/
│   │   ├── README.md
│   │   ├── 51job/
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── sources/
│   │   └── zhaopin/
│   ├── raw/
│   │   ├── README.md
│   │   ├── 51job/
│   │   ├── clds/
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── occupation/
│   │   ├── stats/
│   │   └── zhaopin/
│   └── processed/
│       ├── README.md
│       ├── 51job/
│       └── ncss/
├── docs/
├── papers/
│   └── reference_library/
├── src/
│   └── platforms/
│       └── job51/
├── requirements.txt
└── README.md
```

## 目录说明

- `data/`：项目数据区，按 `input -> raw -> processed` 分层，并在每层内部按站点组织
- `docs/`：研究过程文档、交付核验说明、方法笔记，不再存放论文 PDF
- `papers/`：参考论文原文、论文下载记录和分类文献库
- `src/`：数据获取、解析、清洗相关脚本；其中新的站点逻辑放到 `src/platforms/`
- `requirements.txt`：项目 Python 依赖

## 子目录索引

- `data/README.md`：数据目录总说明
- `data/input/README.md`：输入配置、种子文件和来源登记说明
- `data/raw/README.md`：原始抓取结果、网页快照和 manifest 说明
- `data/processed/README.md`：清洗后数据说明
- `docs/README.md`：文档索引
- `papers/README.md`：论文库说明
- `src/README.md`：脚本索引
