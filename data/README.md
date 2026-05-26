# data 目录说明

`data/` 现在只保留可公开归档的输入配置、目录说明和小型来源文件。全量原始抓取结果、清洗后大表和运行缓存已移动到本地归档目录：

```text
_local_archive_not_for_github/data/
```

## 当前结构

```text
data/
├── input/       # 种子、配置、地区树、来源登记
├── raw/         # 原始数据目录说明；大体量 raw 已归档
└── processed/   # 处理后数据目录说明；大体量 CSV 已归档
```

## 为什么不直接提交全量数据

本项目的全量招聘文本、HTML 快照和中间面板数据体量较大，其中部分单文件超过 GitHub 普通仓库限制。为避免未来推送失败，也避免公开泄露省赛前数据，全量数据保留在本机：

```text
_local_archive_not_for_github/data/raw/
_local_archive_not_for_github/data/processed/
```

论文最终用到的小型表格和图已经整理到 `analysis/`：

- `analysis/tables/paper_tables_csv/`
- `analysis/tables/model_inputs/`
- `analysis/tables/extraction_outputs/`
- `analysis/figures/`

如需复现实验，请先从本地归档恢复对应数据文件到原路径。
