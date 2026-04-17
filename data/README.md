# 数据目录说明

`data/` 按两条轴组织：

1. 按生命周期分层：`input -> raw -> processed`
2. 在每一层内部按平台分目录

整体可以理解为：

```text
input/<platform> -> raw/<platform> -> processed/<platform>
```

## 先看什么

- 要找抓取配置、种子和缓存：先读 `input/README.md`
- 要看 HTML、manifest 和原始 JSONL：再读 `raw/README.md`
- 要直接拿可分析表：最后读 `processed/README.md`

## 当前各平台定位

- `ncss/`：当前主线数据源，已经形成全国范围可分析结果
- `51job/`：新增的浏览器顺序抓取方案；校招样本已稳定，社招全量仍以本地滚动抓取为主
- `mohrss/`：中国公共招聘网参考输入与页面快照
- `zhaopin/`：智联公开详情页样例
- `sources/`：跨平台来源登记、参考页面和人工补链表
- `stats/`、`occupation/`、`clds/`：统计口径、职业分类和参考资料快照

## 当前主用文件

- `processed/ncss/ncss_jobs_all_areas_clean.csv`：NCSS 全国主线 JD 表，`30985` 条清洗后职位
- `processed/ncss/ncss_listings_all_areas_flat.csv`：全国职位覆盖底表，`41407` 个唯一职位
- `raw/ncss/records/ncss_jobs_all_areas_raw.jsonl`：NCSS 全国详情 RAW，`41400` 条有效详情
- `processed/51job/51job_campus_jobs_clean.csv`：51job 校招专题页验证样本，`245` 条
- `raw/51job/records/51job_campus_jobs_raw.jsonl`：51job 校招 RAW，`249` 条
- `processed/51job/51job_social_jobs_clean.csv`：51job 社招阶段性 clean 快照，`13989` 条
- `raw/51job/records/51job_social_jobs_raw.jsonl`：51job 社招阶段性 RAW 快照，`14141` 条
- `input/51job/51job_search_area_tree.json`：51job 全国地区树缓存
- `input/51job/51job_search_function_codes.json`：51job 职能编码缓存

## 版本库和本地运行态的边界

- `NCSS` 主线数据和 `51job` 校招样本属于稳定交付物，适合进入版本库
- `51job` 社招顺序抓取的 cursor、progress、rolling RAW 和 rolling clean 表属于本地运行产物，默认不提交
- `data/runtime/` 只放浏览器 profile 和临时运行文件，始终不纳入版本库
