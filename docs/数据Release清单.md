# 数据 Release 清单

本文件说明 `data-v1.0` Release 附件的内容和恢复路径。Git 仓库保留论文、图片、代码、小型表格和说明文档；下列大体量数据通过 Release 附件恢复。

Release 地址：

https://github.com/TeaSings/Our-Statistical-Modeling-Competition-Paper/releases/tag/data-v1.0

## 附件清单

| 附件 | 内容 | 解压后路径 | 未压缩规模 |
| --- | --- | --- | ---: |
| `processed-51job-main.zip` | 51job 清洗后社招主样本和校招清洗表 | `data/processed/51job/` | 约 3.06 GB |
| `raw-51job-records.zip` | 51job 原始 JSONL 抓取记录 | `data/raw/51job/records/` | 约 2.56 GB |
| `raw-51job-manifests.zip` | 51job 抓取 manifest、cursor 和进度日志 | `data/raw/51job/manifests/` | 约 0.06 GB |
| `analysis-static-full.zip` | 静态抽取全量中间表和完整 job-level 主表 | `data/processed/analysis_static/` | 约 2.99 GB |
| `ncss-raw-and-processed.zip` | NCSS 原始记录、manifest 和清洗表 | `data/raw/ncss/`、`data/processed/ncss/` | 约 0.18 GB |
| `analysis-local-supplement.zip` | 大体量补充分析表 | `analysis/job_level_scored.csv` | 约 0.21 GB |
| `checksums-sha256.txt` | 附件 SHA256 校验值 | 下载目录 | - |

## 恢复和校验

在仓库根目录解压所需 zip 文件，保持压缩包内的相对路径。下载后可使用 `checksums-sha256.txt` 校验附件完整性。

```powershell
Get-FileHash .\processed-51job-main.zip -Algorithm SHA256
```

只阅读论文或检查正文图表时，不需要下载这些附件。
