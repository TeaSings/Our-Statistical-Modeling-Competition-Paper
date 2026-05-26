# 城市标准化

城市标准化把平台城市字段统一到地级市或直辖市，并补充省份字段，保证后续城市聚合和地图展示口径一致。

## 输入字段

- `city_raw`
- `city_seed`
- `district_raw`
- `data/input/ncss/ncss_area_codes_all.csv`
- `data/input/51job/51job_search_area_tree.json`

## 映射逻辑

1. 清洗平台城市文本，去除多余标点和站内说明。
2. 使用地区码表进行城市、区县和省份精确匹配。
3. 对区县命中记录回填所属城市。
4. 使用 51job 地区树补足平台特有地区名称。
5. 对只命中省份或无法稳定识别的记录输出人工复核清单。

## 输出字段

| 字段 | 含义 |
| --- | --- |
| `city_std_final` | 最终城市 |
| `province_std_final` | 最终省份 |
| `district_std` | 区县匹配结果 |
| `city_match_confidence` | 城市匹配置信度 |
| `city_match_method` | 使用的映射方法 |

## 质量结果

| 指标 | 数量或比例 |
| --- | ---: |
| 总岗位数 | 1111676 |
| 城市映射成功率 | 99.3672% |
| 省份映射成功率 | 99.9737% |
| 高置信记录 | 1104170 |
| 中置信记录 | 67 |
| 低置信/待复核记录 | 7439 |

复核文件为 `analysis/tables/extraction_outputs/city_manual_review.csv`。
