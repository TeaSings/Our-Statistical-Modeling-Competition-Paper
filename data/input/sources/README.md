# data/input/sources 说明

这个目录存放跨站点共用的来源登记和人工补链文件。

## 当前文件

- `job_source_registry.json`：中文招聘和官方统计站点注册表
- `reference_pages.csv`：官方参考页面清单

## 典型用途

- 选数据源时先看 `job_source_registry.json`
- 需要补抓官方参考页时用 `reference_pages.csv`
- 需要人工分工补链时运行 `src/build_manual_seed_sheet.py`
