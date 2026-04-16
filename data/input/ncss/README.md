# data/input/ncss 说明

这个目录存放 NCSS 主线所需的全部输入文件。

## 配置文件

- `ncss_batch_config.json`：常规 NCSS 列表抓取配置
- `ncss_batch_config_all_areas.json`：全地区列表抓取配置
- `platform_ncss_detail.json`：NCSS 详情页选择器配置

## 地区码文件

- `ncss_area_codes_all.json`
- `ncss_area_codes_all.csv`

这两份文件来自 `src/extract_ncss_area_codes.py`，用于把列表接口从少量城市扩展到全地区扫描。

## 详情页种子

- `ncss_detail_urls_generated.csv`：常规列表抓取直接导出的详情种子
- `ncss_detail_urls_balanced.csv`：均衡抽样后的详情种子
- `ncss_detail_urls_all_areas.csv`：全地区列表抓取后的详情种子
- `ncss_detail_urls_expanded_balanced.csv`：扩展版均衡样本种子
- `shards/`：全地区详情页并发补抓分片

## 调试文件

- `detail_urls_example.csv`：少量 NCSS 详情页公开样例

## 当前建议

- 做常规流程先用 `ncss_batch_config.json`
- 需要尽量榨干 NCSS 时用 `ncss_batch_config_all_areas.json`
- 需要提速抓详情时直接使用 `shards/` 下的分片种子
