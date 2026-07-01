# 数据 Release 清单

本文件记录从本地归档目录生成 GitHub Release 附件的范围。Git 仓库保留论文、图片、代码、小型表格和说明文档；下列大体量数据通过 Release 附件恢复。

## 附件清单

| 附件 | 本地来源 | 解压后路径 | 未压缩规模 |
| --- | --- | --- | ---: |
| `processed-51job-main.zip` | `_local_archive_not_for_github/data/processed/51job/` | `data/processed/51job/` | 约 3.06 GB |
| `raw-51job-records.zip` | `_local_archive_not_for_github/data/raw/51job/records/` | `data/raw/51job/records/` | 约 2.56 GB |
| `raw-51job-manifests.zip` | `_local_archive_not_for_github/data/raw/51job/manifests/` | `data/raw/51job/manifests/` | 约 0.06 GB |
| `analysis-static-full.zip` | `_local_archive_not_for_github/data/processed/analysis_static/` | `data/processed/analysis_static/` | 约 2.99 GB |
| `ncss-raw-and-processed.zip` | `_local_archive_not_for_github/data/raw/ncss/` 与 `_local_archive_not_for_github/data/processed/ncss/` | `data/raw/ncss/` 与 `data/processed/ncss/` | 约 0.18 GB |
| `analysis-local-supplement.zip` | `_local_archive_not_for_github/analysis/job_level_scored.csv` | `analysis/job_level_scored.csv` | 约 0.21 GB |

不纳入 Release 的内容包括浏览器 profile、第三方依赖缓存、临时渲染文件、smoke-test 中间结果和运行态目录。

## 打包命令

先预览附件范围：

```powershell
python src/tools/build_github_release_assets.py --dry-run
```

生成附件和校验文件：

```powershell
python src/tools/build_github_release_assets.py
```

默认输出目录为 `_release_assets/data-v1.0/`。该目录已加入 `.gitignore`，不会进入 Git。

## 发布建议

Release 标签建议使用 `data-v1.0`。上传附件后，在 Release 说明中写明对应 Git commit、数据来源、生成日期和恢复方法。若某个压缩包超过 2 GiB，应进一步按平台、阶段或文件拆分后再上传。
