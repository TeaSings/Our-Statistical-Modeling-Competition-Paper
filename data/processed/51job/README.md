# data/processed/51job 说明

这里存放 51job 清洗后的分析用结果表。

## 当前文件

- `51job_campus_jobs_clean.csv`：校招专题页 clean 表，当前 `245` 条
- `51job_social_jobs_clean.csv`：社招顺序抓取生成的阶段性 clean 快照，当前 `13989` 条

## 如何理解这两份表

- `51job_campus_jobs_clean.csv` 是稳定样本，适合做解析质量验证和校招专题页分析
- `51job_social_jobs_clean.csv` 是浏览器顺序抓取的阶段性 checkpoint，更适合做社招文本探索和方法验证

## 版本管理建议

- 校招 clean 表和社招 clean 快照都可以提交，方便组员直接分析
- 继续抓取时，建议定期固化新的 clean 快照，而不是把进度状态文件一并纳入版本库
