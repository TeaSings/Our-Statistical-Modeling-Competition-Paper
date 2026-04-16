# src 说明

这个目录存放项目脚本，按“主流程脚本”和“辅助脚本”两类理解即可。

## 主流程脚本

| 文件 | 作用 |
| --- | --- |
| `fetch_ncss_jobs.py` | 批量抓取 NCSS 列表接口并导出详情页种子 |
| `build_balanced_ncss_detail_seeds.py` | 对详情页种子做城市与关键词均衡抽样 |
| `fetch_pages.py` | 根据种子批量抓取页面 HTML |
| `parse_details.py` | 按选择器解析职位详情字段 |
| `clean_jobs.py` | 清洗、标准化和去重 |

## 辅助脚本

| 文件 | 作用 |
| --- | --- |
| `common.py` | 公共工具函数 |
| `extract_links.py` | 从列表页 HTML 中提取详情页链接 |
| `build_manual_seed_sheet.py` | 生成手工补充种子的任务表 |
| `parse_job_pages_by_text.py` | 按页面文本规则解析详情页，适合辅助调试 |

## 当前建议

- 日常使用优先围绕“主流程脚本”
- 辅助脚本保留，但不再作为仓库默认入口
