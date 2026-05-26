# data/processed 说明

这个目录原本存放清洗后、标准化后、适合直接分析的大表。整理 GitHub 归档版本时，CSV 大表已移动到本地归档：

```text
_local_archive_not_for_github/data/processed/
```

当前 Git 仓库只保留说明文件。论文最终引用的小型结果表已经整理到：

```text
analysis/tables/
```

其中包括正文表格、模型输入、城市分组、小样本惩罚诊断、技能/任务抽取复核表等。

如需重新运行完整数据链路，请从本地归档恢复相应 CSV 到原路径。例如：

```text
_local_archive_not_for_github/data/processed/51job/51job_social_jobs_clean_with_publish.csv
_local_archive_not_for_github/data/processed/analysis_static/interfaces/job_level_master_static.csv
```
