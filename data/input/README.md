# data/input 说明

这个目录存放“采集之前就要准备好”的文件，包括数据源登记、接口配置、详情页种子和辅助参考链接。

## 当前保留文件

| 文件 | 作用 | 当前状态 |
| --- | --- | --- |
| `job_source_registry.json` | 中文招聘/统计数据源登记表 | 手工整理，供选源和写方法说明使用 |
| `reference_pages.csv` | 官方页面入口与参考页面清单 | 手工维护，供人工核对 |
| `ncss_batch_config.json` | NCSS 列表接口批量抓取配置 | 主配置文件 |
| `platform_ncss_detail.json` | NCSS 详情页字段选择器配置 | 主配置文件 |
| `ncss_detail_urls_generated.csv` | 从 NCSS 列表接口直接导出的第一轮详情页种子 | 中间产物 |
| `ncss_detail_urls_balanced.csv` | 经城市和关键词均衡抽样后的详情页种子 | 当前主用种子 |
| `detail_urls_example.csv` | 少量公开详情页样本链接 | 辅助调试文件 |

## 重点文件说明

### `ncss_batch_config.json`

用于控制 NCSS 列表接口抓取范围，核心字段包括：

- `list_api`：列表接口地址
- `referer`：请求头中的来源页
- `limit`：单页返回条数
- `max_pages_per_query`：每个城市与关键词组合最多抓多少页
- `cities`：城市名称与 `areaCode`
- `keywords`：本轮采样的岗位关键词

### `platform_ncss_detail.json`

用于解析 NCSS 详情页字段，主要分成两部分：

- `request_headers`：抓取详情页时附带的请求头
- `detail_page`：职位名、公司名、城市、薪资、学历、经验、岗位描述等字段的 CSS 选择器

### `ncss_detail_urls_balanced.csv`

当前主用详情页种子文件，字段如下：

- `platform`
- `page_type`
- `url`
- `source_url`
- `city`
- `keyword`

这份文件相较于 `ncss_detail_urls_generated.csv` 更均衡，优先用于正式抓取。

## 维护原则

- `input/` 只保留真实会用到的配置和种子，不再放通用模板文件
- 需要新增数据源时，优先补 `job_source_registry.json` 和对应配置文件
- 实验性种子如果已经失效或被主流程替代，应及时移出本目录
