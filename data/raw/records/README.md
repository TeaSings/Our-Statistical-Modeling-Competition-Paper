# data/raw/records 说明

这个目录存放从接口或 HTML 中直接整理出的原始记录文件，格式通常为 `jsonl`。

## 当前主用文件

| 文件 | 作用 |
| --- | --- |
| `ncss_listings_raw.jsonl` | NCSS 列表接口原始结果 |
| `ncss_jobs_balanced_raw.jsonl` | 均衡样本详情页解析后的原始字段 |

## 使用边界

- 这里的文件已经比 HTML 更结构化
- 但它们还没有完成字段标准化和去重
- 正式分析时仍然优先使用 `data/processed/` 中的文件
