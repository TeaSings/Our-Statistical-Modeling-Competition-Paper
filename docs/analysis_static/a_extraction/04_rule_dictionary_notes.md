# 04 规则词典与抽取说明

## 当前策略

- 技能抽取采用规则词典第一版，不直接调用大模型。
- 规则输入字段为：`job_title_std`、`keyword_seed`、`job_tags_raw`、`jd_text_clean`。
- 词典覆盖 `GenAI`、`Programming`、`Data`、`DevOps`、`Engineering`、`Design`、`ERP/CRM`、`Office/BI` 等类别。
- 任务抽取单独维护 `TASK_RULES`，当前用于生成 `task_list` 与后续人工标注种子。

## 结果摘要

- 有技能命中的职位：`393442`
- 有任务命中的职位：`454793`
- 有 GenAI 相关技能命中的职位：`7721`
- `genai_exposure_level=high`：`32950`
- `genai_exposure_level=medium`：`877`
- `genai_exposure_level=low`：`310250`
- `genai_exposure_level=none`：`767599`

## 产物

- `annotation_seed_500.csv`
- `skill_rule_dictionary_v1.csv`
- `task_rule_dictionary_v1.csv`
- `skill_extraction_table_raw.csv`
- `skill_extraction_table_final.csv`
- `skill_token_norm.csv`
- `skill_category_dict.csv`
- `task_category_dict.csv`
