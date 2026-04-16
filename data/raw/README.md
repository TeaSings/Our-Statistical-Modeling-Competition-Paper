# data/raw 说明

这个目录存放“已经抓到，但还没有完成标准化”的原始数据。

## 目录结构

- `html/`：原始网页快照
- `records/`：从原始网页或接口直接整理出的原始记录
- `*.jsonl manifest`：抓取过程日志

## 当前主用文件

| 文件 | 作用 |
| --- | --- |
| `records/ncss_listings_raw.jsonl` | NCSS 列表接口的原始返回记录 |
| `ncss_detail_manifest_balanced.jsonl` | 均衡详情页批次的抓取 manifest |
| `records/ncss_jobs_balanced_raw.jsonl` | 详情页字段解析后的原始结果 |
| `html/ncss_jobs/` | NCSS 列表页和详情页 HTML 快照 |

## manifest 文件说明

`manifest` 用来记录每次抓取尝试，常见字段包括：

- `fetched_at`：抓取时间
- `platform`：平台标识
- `page_type`：页面类型
- `url`：原始请求地址
- `local_path`：本地 HTML 存放路径
- `status_code`：HTTP 状态码
- `fetched`：本次是否新抓取
- `error`：抓取异常信息

注意：

- 如果命令带了 `--skip-existing`，那么某些记录会显示 `fetched=false`
- `fetched=false` 不代表失败，可能只是复用了已存在的本地 HTML

## `records/` 文件说明

### `ncss_listings_raw.jsonl`

这是 NCSS 列表接口的直接返回结果，每一行对应一个职位卡片记录，常用于：

- 统计原始职位数
- 生成详情页种子
- 做城市和关键词均衡抽样

### `ncss_jobs_balanced_raw.jsonl`

这是详情页解析后的原始字段表，尚未做统一标准化，但已经能看到核心字段，例如：

- 职位名
- 公司名
- 城市
- 薪资
- 学历
- 经验
- 公司行业
- 公司规模
- 岗位描述

## 使用建议

- 如果要回溯“某条记录最初长什么样”，先看 `raw/`
- 如果要做统计建模，不要直接用 `raw/`，优先使用 `processed/`
- `html/` 中除了 `ncss_jobs/` 之外，可能还包含资料检索阶段留下的参考页面快照
