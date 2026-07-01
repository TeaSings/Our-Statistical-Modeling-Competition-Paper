# 文档目录

本目录说明论文从招聘文本到正文图表的可复现流程。

| 路径 | 内容 |
| --- | --- |
| `中文数据来源与脚本说明.md` | 数据来源、脚本入口和完整流程 |
| `数据分析初步方法.md` | 最终分析口径、变量构造和建模边界 |
| `GitHubRelease数据归档方案.md` | 大体量数据的 GitHub Release 归档方案 |
| `数据Release清单.md` | Release 附件清单、恢复路径和打包命令 |
| `analysis_static/a_extraction/` | 51job 静态抽取、城市标准化、岗位族标准化和质量报告 |
| `NCSS*.md` | NCSS 抓取、扩展和核验流程 |
| `analysis-method分支三人并行协作方案.md` | 当前实现模块和接口约定 |

论文成品位于 `analysis/`，代码位于 `src/`。大体量原始和中间数据不进入 Git，完整复现时从 Release 数据包恢复。
