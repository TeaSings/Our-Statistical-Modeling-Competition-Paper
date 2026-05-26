# 数据目录

本目录记录项目数据的轻量输入文件和大体量数据的预期恢复位置。

| 路径 | 内容 |
| --- | --- |
| `input/` | 抓取种子、配置文件、来源登记和地区码 |
| `raw/` | 原始 HTML、JSONL、manifest 和抓取日志的恢复位置 |
| `processed/` | 清洗后数据和分析主数据的恢复位置 |

大体量原始数据和处理后数据不进入 Git 历史。论文直接使用的小型结果位于 `analysis/tables/`；完整数据包按 `docs/GitHubRelease数据归档方案.md` 从 GitHub Release 恢复。
