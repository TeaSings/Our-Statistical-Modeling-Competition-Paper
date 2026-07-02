# 生成式人工智能与城市就业技能结构重塑

本仓库公开统计建模竞赛论文《生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本的技能抽取与大模型暴露度测度》的论文材料、核心代码、表格图形和数据归档说明。

研究使用在线招聘文本刻画城市就业技能需求，围绕 51job 社招岗位构建岗位族、城市、技能任务和大模型暴露度指标，分析生成式人工智能对城市就业技能结构的重塑。

## 快速导航

| 需要查看 | 位置 |
| --- | --- |
| 论文原稿、PDF 和附页 | `analysis/manuscript/` |
| 正文图片 | `analysis/figures/` |
| 正文表格和模型输入 | `analysis/tables/` |
| 数据来源、方法和复现说明 | `docs/` |
| 抓取、清洗和抽取脚本 | `src/` |
| 研究文献和竞赛参考材料 | `papers/` |

## 仓库内容

- `analysis/`：论文交付材料，包括 Word 原稿、PDF、正文图表、模型输入和抽取结果。
- `src/`：招聘数据抓取、清洗、城市与岗位族标准化、技能任务抽取和 Release 数据打包脚本。
- `data/`：轻量输入配置和大体量数据的恢复位置说明。
- `docs/`：数据来源、实现流程、质量核验和 Release 数据恢复说明。
- `papers/`：研究文献和往年优秀论文参考。

## 数据获取

Git 仓库不直接保存全量原始抓取、HTML、JSONL 和大体量中间表。完整数据已作为 GitHub Release 附件发布：

https://github.com/TeaSings/Our-Statistical-Modeling-Competition-Paper/releases/tag/data-v1.0

下载所有 zip 附件和 `checksums-sha256.txt` 后，在仓库根目录解压即可恢复原始数据和全量中间结果。附件说明见 `docs/数据Release清单.md`。

## 复现入口

安装 Python 依赖：

```powershell
python -m pip install -r requirements.txt
```

主要脚本入口：

```powershell
python src/analysis_static/a_extraction/build_51job_master_static.py
python src/analysis_static/a_extraction/finalize_51job_master_static.py
```

完整复现需要先恢复 Release 数据包。只阅读论文、查看图表或检查正文表格时，不需要下载大体量数据。

## 归档状态

比赛已结束，仓库以 `main` 分支作为公开归档版本。早期开发分支中的爬虫实验、方法探索和大体量数据快照已收束为当前目录结构与 `data-v1.0` Release 数据包。
