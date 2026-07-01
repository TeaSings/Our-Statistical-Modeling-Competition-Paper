# GitHub Release 数据归档方案

本项目采用“Git 仓库放代码和小型结果，GitHub Release 放大体量数据包”的方式归档数据。这样可以避免把超过 100 MiB 的 CSV、HTML、JSONL 或压缩包写入 Git 历史，同时保留公开复现入口。

截至 2026-07-01，GitHub 官方文档说明：普通仓库文件超过 50 MiB 会收到警告，超过 100 MiB 会被阻断；Release 单个附件必须小于 2 GiB，单个 Release 最多 1000 个附件。项目数据包按这一限制拆分。

参考链接：

- https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github
- https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases

## 仓库内保留

- 论文、图表和小型结果：`analysis/`
- 抓取、清洗和抽取脚本：`src/`
- 输入配置、schema、README 和文档：`data/`、`docs/`
- 小型模型输入和正文表格：`analysis/tables/`

## Release 附件保留

建议发布一个数据 Release，例如：

- tag：`data-v1.0-province-submission`
- 标题：`省赛提交数据归档包`
- 可见性：随仓库可见性控制，省赛前不发布公开版本

建议附件拆分：

| 附件 | 内容 |
| --- | --- |
| `processed-51job-main.zip` | 51job 清洗后主样本和校招清洗表 |
| `raw-51job-records.zip` | 51job 原始 JSONL 抓取记录 |
| `raw-51job-manifests.zip` | 51job 抓取 manifest、cursor 和进度日志 |
| `analysis-static-full.zip` | 静态抽取全量中间表和完整 job-level 主表 |
| `ncss-raw-and-processed.zip` | NCSS 原始 JSONL、manifest 和清洗表 |
| `analysis-local-supplement.zip` | 不进入 Git 的大体量分析补充表 |
| `checksums-sha256.txt` | 每个附件的 SHA256 校验值 |

每个附件保持小于 2 GiB；更大的目录先按来源或阶段拆包。实际附件清单见 `docs/数据Release清单.md`。

## 恢复方式

1. 下载 Release 附件和 `checksums-sha256.txt`。
2. 在仓库根目录解压，保持压缩包内的相对路径。
3. 确认关键文件回到原位置，例如：
   - `data/processed/51job/51job_social_jobs_clean_with_publish.csv`
   - `data/processed/ncss/ncss_jobs_all_areas_clean.csv`
   - `data/processed/analysis_static/`
4. 运行脚本或直接读取 `analysis/tables/` 中的论文结果。

可用以下命令生成附件：

```powershell
python src/tools/build_github_release_assets.py --dry-run
python src/tools/build_github_release_assets.py
```

## 发布前检查

- 不把 Release 附件加入 Git 暂存区。
- 对附件生成 SHA256 校验文件。
- Release 说明写明数据来源、生成日期、对应 commit 和恢复路径。
- 发布前统一检查数据合规性和隐私风险。

GitHub 对普通仓库文件有 100 MiB 阻断限制；Release 附件更适合分发大数据文件。Git LFS 也可处理大文件，但会把指针纳入 Git 流程，并受 LFS 配额和协作环境影响。本项目优先使用 Release 附件作为数据归档方式。
