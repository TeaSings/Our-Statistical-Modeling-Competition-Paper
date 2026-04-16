# 统计建模竞赛项目

当前题目：`生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取`

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
├── src/
├── requirements.txt
└── README.md
```

## 目录说明

- `data/`：项目数据区，按 `input -> raw -> processed` 分层，并在每层内部按站点组织
- `docs/`：研究过程文档、数据来源说明、抓取记录
- `papers/`：参考论文与论文目录说明
- `src/`：数据获取、解析、清洗相关脚本
- `requirements.txt`：项目 Python 依赖

## 子目录索引

- `data/README.md`：数据目录总说明
- `data/input/README.md`：输入配置、种子文件和来源登记说明
- `data/raw/README.md`：原始抓取结果、网页快照和 manifest 说明
- `data/processed/README.md`：清洗后数据说明
- `docs/README.md`：文档索引
- `papers/README.md`：参考论文说明
- `src/README.md`：脚本索引
