# data/processed 说明

这个目录存放已经过清洗和标准化、可以直接进入分析环节的数据。

## 当前主用文件

| 文件 | 作用 | 是否主用 |
| --- | --- | --- |
| `ncss_jobs_balanced_clean.csv` | 当前正式使用的招聘文本主数据集 | 是 |
| `ncss_list_query_summary.csv` | NCSS 列表接口查询摘要表 | 是 |

## `ncss_jobs_balanced_clean.csv`

这是当前最重要的结果文件，适合作为后续以下工作的输入：

- 技能词抽取
- AI 相关技能识别
- 城市间技能结构比较
- 岗位群之间的技能差异分析

主要字段可以分成几组：

### 标识与来源

- `job_id`
- `platform`
- `detail_url`
- `source_url`
- `keyword_seed`

### 职位与公司

- `job_title_raw`
- `job_title_std`
- `company_name_raw`
- `company_name_std`

### 城市与薪资

- `city_raw`
- `city_std`
- `salary_raw`
- `salary_min_month`
- `salary_max_month`
- `salary_avg_month`

### 教育与经验

- `education_raw`
- `education_std`
- `experience_raw`
- `experience_std`

### 企业画像与文本

- `company_industry_raw`
- `company_size_raw`
- `job_tags_raw`
- `jd_text_raw`
- `jd_text_clean`

命名约定：

- `*_raw`：网页原始文本
- `*_std`：经过标准化后的字段

## `ncss_list_query_summary.csv`

这张表用于记录列表接口查询的覆盖范围和返回情况，字段包括：

- `city`
- `areaCode`
- `keyword`
- `page`
- `returned_count`
- `total_pages_hint`
- `query_url`

它更适合做以下工作：

- 说明样本从哪些城市与关键词组合中抽得
- 对比不同查询组合的职位返回量
- 在论文中交代抓取范围与样本形成过程

## 使用建议

- 做分析时优先从 `ncss_jobs_balanced_clean.csv` 开始
- 写论文方法部分时，可以同时引用这两个文件
