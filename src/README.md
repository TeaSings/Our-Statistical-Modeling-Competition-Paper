# 源代码

本目录保存数据采集、清洗、抽取和辅助处理脚本。

| 路径 | 内容 |
| --- | --- |
| `clean_jobs.py` | 通用 raw-to-clean 岗位表转换脚本 |
| `common.py` | 共享工具函数 |
| `analysis_static/` | 51job 静态抽取和结果收口流程 |
| `platforms/job51/` | 51job 抓取、导入和进度工具 |
| `platforms/ncss/` | NCSS 抓取、解析和进度工具 |
| `tools/` | 跨平台辅助脚本 |

完整运行需要先恢复 `data/` 中记录的大体量原始和清洗后数据。
