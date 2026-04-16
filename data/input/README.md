# data/input 说明

这个目录存放“抓取前就要准备好”的文件，已经按站点拆开。

## 子目录

- `51job/`：前程无忧校招专题页种子
- `ncss/`：NCSS 的抓取配置、地区码、详情页种子和分片种子
- `mohrss/`：中国公共招聘网的示例详情页种子
- `zhaopin/`：智联公开详情页示例种子
- `sources/`：数据源注册表、参考页面和人工补链任务表

## 常用入口

- `51job/campus_seed_urls.csv`：51job 官方专题页主种子，当前包含 `6` 个已验证专题页
- `ncss/ncss_batch_config.json`：NCSS 常规列表抓取配置
- `ncss/ncss_batch_config_all_areas.json`：NCSS 全地区列表抓取配置
- `ncss/platform_ncss_detail.json`：NCSS 详情页解析选择器
- `ncss/ncss_detail_urls_all_areas.csv`：NCSS 全地区详情页主种子，当前覆盖 `41407` 个唯一职位
- `ncss/shards/`：由主种子切分出的并发抓取分片
- `sources/job_source_registry.json`：全项目中文数据源登记表

## 使用建议

- 需要补充 51job 校招页时，优先在 `51job/campus_seed_urls.csv` 中维护正式种子
- 需要扩充 NCSS 覆盖范围时，优先看 `ncss/README.md`
- 需要补充其他平台入口时，优先看 `sources/README.md`
- 手工补链时，不要把结果混写到别的平台目录里
- 临时补抓种子不要长期留在仓库里，核验后只保留正式主种子和分片
