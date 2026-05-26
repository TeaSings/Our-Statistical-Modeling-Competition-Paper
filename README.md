# 生成式人工智能与城市就业技能结构重塑

本仓库整理统计建模竞赛论文《生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本的技能抽取与大模型暴露度测度》的代码、文档、图表和可复现结果。

## 仓库结构

| 路径 | 内容 |
| --- | --- |
| `analysis/` | 论文文稿、正文图表、模型输入、写作修订记录和参考材料 |
| `src/` | 数据抓取、清洗、静态抽取和辅助工具脚本 |
| `data/` | 轻量输入配置、数据目录说明和恢复位置 |
| `docs/` | 数据流程、方法口径、质量核验和 Release 数据归档方案 |
| `papers/` | 研究文献与下载记录 |

## 主要结果

- 论文文稿：`analysis/manuscript/`
- 正文图片：`analysis/figures/`
- 正文表格：`analysis/tables/paper_tables_csv/`
- 模型输入：`analysis/tables/model_inputs/`
- 抽取结果：`analysis/tables/extraction_outputs/`

## 数据归档

全量原始抓取、HTML、JSONL 和大体量中间表不进入 Git 历史。仓库只保留代码、小型结果、schema 和说明文档；完整数据包计划通过 GitHub Release 附件分发，方案见 `docs/GitHubRelease数据归档方案.md`。

## 复现方式

```powershell
python -m pip install -r requirements.txt
```

完整复现需要先从对应 GitHub Release 下载数据包，并按压缩包内的相对路径解压回仓库根目录。核心脚本位于 `src/`，关键流程说明位于 `docs/中文数据来源与脚本说明.md`。
