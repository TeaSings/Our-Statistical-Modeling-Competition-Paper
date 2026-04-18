# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

## 当前交付状态

- 主体可分析 JD 数据：`data/processed/ncss/ncss_jobs_all_areas_clean.csv`，当前快照为 `30985` 条清洗后职位
- 全国职位覆盖底表：`data/processed/ncss/ncss_listings_all_areas_flat.csv`，覆盖 `41407` 个唯一职位
- 全量详情解析原始表：`data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`，当前为 `41400` 条有效详情
- 最终核验与交付说明：`docs/NCSS全量数据核验与交付说明-2026-04-16.md`

## 当前新增数据补充说明

- 数据来源：当前新增数据来自实时招聘网站抓取，当前主站点为 `job51`
- 数据组成：整理后的数据包含岗位名称、公司名称、城市、薪资信息、学历要求、经验要求、发布时间、岗位描述，以及技能要求相关字段、技能关键词与技能标签
- 最终研究文件：`project/data/final/master_jobs.csv`
- 研究用途：当前数据主要用于分析岗位需求，尤其关注技能要求信息，后续将与其他论文中的历史数据或研究结果进行对比

## 项目结构

```text
.
├── data/
│   ├── README.md
│   ├── input/
│   │   ├── README.md
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── sources/
│   │   └── zhaopin/
│   ├── raw/
│   │   ├── README.md
│   │   ├── clds/
│   │   ├── mohrss/
│   │   ├── ncss/
│   │   ├── occupation/
│   │   ├── stats/
│   │   └── zhaopin/
│   └── processed/
│       ├── README.md
│       └── ncss/
├── docs/
├── papers/
│   └── reference_library/
├── src/
├── requirements.txt
└── README.md
```

## 目录说明

- `data/`：项目数据区，按 `input -> raw -> processed` 分层，并在每层内部按站点组织
- `docs/`：研究过程文档、交付核验说明、方法笔记，不再存放论文 PDF
- `papers/`：参考论文原文、论文下载记录和分类文献库
- `src/`：数据获取、解析、清洗相关脚本
- `requirements.txt`：项目 Python 依赖

## 子目录索引

- `data/README.md`：数据目录总说明
- `data/input/README.md`：输入配置、种子文件和来源登记说明
- `data/raw/README.md`：原始抓取结果、网页快照和 manifest 说明
- `data/processed/README.md`：清洗后数据说明
- `docs/README.md`：文档索引
- `papers/README.md`：论文库说明
- `src/README.md`：脚本索引
