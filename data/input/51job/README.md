# data/input/51job 说明

这里存放 51job 抓取所需的正式输入文件，分成两类：

- 校招专题页种子
- 社招搜索 taxonomy 缓存

## 当前文件

- `campus_seed_urls.csv`：已验证可抓取、且能返回真实 JD 的校招专题页种子
- `51job_search_area_tree.json`：从 `we.51job.com` 搜索前端提取的全国地区树
- `51job_search_function_codes.json`：从 `we.51job.com` 搜索前端提取的职能编码列表

## 各文件怎么用

- 校招抓取入口读取 `campus_seed_urls.csv`
- 社招顺序调度器读取地区树和职能编码缓存来构建“职业 × 地区”顺序批次
- HAR 导入工具也会复用这两份 taxonomy 文件来补回地区和职能标签

## 维护建议

- 只保留已经人工核验过的正式校招种子
- taxonomy 缓存优先通过 `src/platforms/job51/search_taxonomy.py` 更新，不手工修改 JSON 内容
- 新增校招种子时优先使用 `campus.51job.com/...` 官方页面
- 某个专题页失效时，先在提交说明或文档里记录原因，再统一调整种子表
