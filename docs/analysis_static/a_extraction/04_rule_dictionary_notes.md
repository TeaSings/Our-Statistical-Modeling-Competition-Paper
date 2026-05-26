# 技能、任务与 GenAI 规则词典

抽取流程采用规则词典和人工复核清单，不依赖端到端模型训练。规则优先保证可解释性和可复现性，适合论文中说明技能识别口径。

## 输入文本

- `job_title_std`
- `keyword_seed`
- `job_tags_raw`
- `jd_text_clean`

## 技能类别

规则词典覆盖以下主要类别：

- GenAI
- Programming
- Data
- DevOps
- Engineering
- Design
- ERP/CRM
- Office/BI

任务词典通过 `TASK_RULES` 识别岗位职责、业务流程、协作沟通、研发设计、生产运维等任务表达。

## 输出文件

| 文件 | 用途 |
| --- | --- |
| `skill_rule_dictionary_v1.csv` | 技能规则词典 |
| `task_rule_dictionary_v1.csv` | 任务规则词典 |
| `skill_category_dict.csv` | 技能类别说明 |
| `task_category_dict.csv` | 任务类别说明 |
| `skill_token_norm.csv` | 技能短语归一化表 |

## 抽取规模

| 指标 | 数量 |
| --- | ---: |
| 技能命中岗位 | 393442 |
| 任务命中岗位 | 454793 |
| GenAI 相关命中岗位 | 7721 |
| 高暴露记录 | 32950 |
| 中暴露记录 | 877 |
| 低暴露记录 | 310250 |
| 无显著暴露记录 | 767599 |
