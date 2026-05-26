# data/input 说明

这个目录存放“抓取前就要准备好”的输入文件，按平台拆分。

## 子目录

- `51job/`：校招专题页种子和社招搜索 taxonomy 缓存
- `ncss/`：列表抓取配置、地区码、详情主种子和分片种子
- `mohrss/`：中国公共招聘网样例种子
- `zhaopin/`：智联公开详情页样例种子
- `sources/`：来源登记、参考页面和人工补链任务表

## 常用入口

- `51job/campus_seed_urls.csv`：已验证的 51job 校招专题页种子
- `51job/51job_search_area_tree.json`：51job 全国地区树缓存
- `51job/51job_search_function_codes.json`：51job 职能编码缓存
- `ncss/ncss_batch_config.json`：NCSS 常规列表抓取配置
- `ncss/ncss_batch_config_all_areas.json`：NCSS 全地区列表抓取配置
- `ncss/platform_ncss_detail.json`：NCSS 详情页解析选择器
- `ncss/ncss_detail_urls_all_areas.csv`：NCSS 全国详情主种子，`41407` 个唯一职位
- `ncss/shards/`：从主种子切出的并发抓取分片
- `sources/job_source_registry.json`：项目数据源登记表

## 哪些文件应该长期保留

- 正式主种子
- 正式配置文件
- 跨平台来源登记表
- 51job 这类不常变化、但重建成本较高的 taxonomy 缓存

## 哪些文件不应该长期保留

- 一次性调试用小样本 CSV
- 临时补抓种子
- 已经完成任务的中间拆分表

临时文件如果只是为了补抓一次，任务结束后应回归主种子与主 manifest，而不是长期散落在 `input/` 中。
