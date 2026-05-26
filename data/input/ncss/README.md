# NCSS 输入

本目录保存 NCSS 抓取、解析和地区映射所需的输入文件。

| 文件或路径 | 内容 |
| --- | --- |
| `ncss_area_codes_all.*` | 全国地区码参考 |
| `ncss_batch_config*.json` | 列表页抓取配置 |
| `platform_ncss_detail.json` | 详情页解析配置 |
| `ncss_detail_urls_*.csv` | 详情页 URL 种子 |
| `shards/` | 并发抓取使用的种子分片 |

这些输入记录了 NCSS 数据集的构建方式，也为 51job 城市标准化提供地区口径参考。
