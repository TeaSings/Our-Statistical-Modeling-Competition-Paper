# 00 分支基线核对（A 线 / 51job-only）

## 结论摘要

- 当前 A 线主样本固定为 `data/processed/51job/51job_social_jobs_clean_with_publish.csv`。
- 实际样本量为 `1111676` 行，明显高于仓库 README 中遗留的 `8184` 条旧口径。
- `data/processed/51job/README.md` 仍描述旧文件 `51job_social_jobs_clean.csv`，与当前主样本不一致。
- 协作文档中提到的 `src/analysis/*`、`src/analysis/config/*` 等目录在当前工作区未找到，说明文档基线与实际仓库存在偏差。
- 当前 51job 社招大表的 `job_title_std`、`company_name_std`、`city_std`、`publish_time_std` 和 `jd_text_clean` 覆盖率均可直接支撑 A 线主表构建。

## README/快照差异

- 根目录 `README.md` 记载 `51job_social_jobs_clean_with_publish.csv` 为 `8184` 条。
- `data/processed/README.md` 记载同一文件为 `8184` 条。
- `data/processed/51job/README.md` 仍引用旧文件 `51job_social_jobs_clean.csv`，未切换到 `_with_publish` 版本。

## 当前主样本建议

- 主样本：`data/processed/51job/51job_social_jobs_clean_with_publish.csv`
- 当前执行口径：`51job-only`、`city-job_family` 静态版主线
- 当前不纳入：`NCSS`、其他平台、时间面板主线

## 字段可用性说明

- 可直接进入主表的关键字段：`source_job_id`、`job_title_std`、`company_name_std`、`city_std`、`province_std`、`salary_avg_month`、`education_std`、`experience_std`、`publish_time_std`、`jd_text_clean`
- 需要二次标准化的关键字段：`province_std`、`city_std`、`district_std`
- 需要新增规则衍生的关键字段：`job_family_std`、`skill_list`、`task_list`、`genai_related_skill_list`、`skill_category_list`、`is_ai_native_job`、`is_ai_augmented_job`、`genai_exposure_level`

## 未找到的协作文档预设目录

- `src/analysis/`
- `src/analysis/config/analysis_plan.json`
- `src/analysis/config/model_specs.json`
- `src/analysis/config/variable_dictionary.csv`
