# src/tools 说明

这个目录存放跨平台辅助脚本，不直接隶属于某一个招聘网站。

## 文件

- `build_manual_seed_sheet.py`：根据来源注册表生成手工补链任务表
- `extract_links.py`：从列表页 HTML 中提取详情页链接
- `parse_job_pages_by_text.py`：用文本规则快速解析职位页面，适合调试异常页面

## 什么时候用这些脚本

- 需要把来源登记转成分工任务时，用 `build_manual_seed_sheet.py`
- 需要从一批静态列表页里补提详情链接时，用 `extract_links.py`
- 需要快速定位某些异常详情页为什么解析失败时，用 `parse_job_pages_by_text.py`
