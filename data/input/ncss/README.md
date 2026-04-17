# data/input/ncss 说明

这个目录存放 NCSS 主线所需的正式输入文件。

## 配置文件

- `ncss_batch_config.json`：第一版常规列表抓取配置
- `ncss_batch_config_all_areas.json`：全国扩展列表抓取配置
- `platform_ncss_detail.json`：详情页解析选择器配置，当前正文主节点是 `.mainContent`

## 地区码文件

- `ncss_area_codes_all.json`
- `ncss_area_codes_all.csv`

这两份文件由 `src/platforms/ncss/extract_ncss_area_codes.py` 生成，用于把 NCSS 列表扫描从少量城市扩展到全国地区码。

## 详情主种子

- `ncss_detail_urls_generated.csv`：常规列表任务直接导出的详情种子
- `ncss_detail_urls_balanced.csv`：均衡样本详情种子
- `ncss_detail_urls_expanded_balanced.csv`：扩展均衡样本种子
- `ncss_detail_urls_all_areas.csv`：当前全国主种子，`41407` 个唯一职位
- `shards/`：从全国主种子切出的并发抓取分片

## 调试样例

- `detail_urls_example.csv`：少量公开详情页样例

## 当前建议

- 做全量主流程时，优先围绕 `ncss_detail_urls_all_areas.csv`
- 只做局部验证或回归测试时，再使用均衡样本或调试样例
- 临时 refetch 的小 CSV 不应长期留在仓库中，任务结束后应回归主种子与主 manifest
