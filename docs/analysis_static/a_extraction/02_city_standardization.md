# 02 城市标准化说明

## 方法

- 当前城市标准化只处理 51job 社招主表，不混入 NCSS。
- 主输入字段依次为：`city_std -> city_raw -> city_seed`，辅助字段为 `province_std`、`district_std`。
- 行政区映射底座使用 `data/input/ncss/ncss_area_codes_all.csv` 构建 `city -> province` 与 `district -> city` lookup。
- 规则优先级依次为：`城市-区县拆分`、`城市别名精确匹配`、`区县回推城市`、`仅省级信息保留`、`原值兜底`。

## 结果摘要

- 总样本：`1111676`
- 已映射到城市：`1104641`
- 已映射到省份：`1111384`
- 高置信城市映射：`1104170`
- 中置信城市映射：`67`
- 低置信或需人工复核：`7439`

## 产物

- `data/processed/analysis_static/a_extraction/city_mapping_table.csv`
- `data/processed/analysis_static/a_extraction/city_manual_review.csv`
- `data/processed/analysis_static/a_extraction/job_level_panel_city_std.csv`

## 复核建议

- 优先复核 `city_manual_review.csv` 中的 `province-only-city-source` 和 `fallback-city-source`
- 对于仅有省级信息的职位，当前保留 `province_std_final`，`city_std_final` 允许为空
- 对于 `上海-黄浦区`、`深圳-福田区` 这类记录，当前已回推至主城区城市并保留 `district_std_final`
