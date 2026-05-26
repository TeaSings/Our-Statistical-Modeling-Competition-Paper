# 统计建模竞赛项目

题目：`生成式人工智能冲击下城市就业技能结构重塑研究——基于招聘文本的技能抽取与大模型暴露度测度`

当前整理分支：`codex/province-submission-repo-cleanup`

> 省赛前请保持本地使用，不要推送公开远端。这个分支用于整理最终归档结构，便于后续在确认保密边界后再决定是否提交到 GitHub。

## 仓库定位

这个仓库保留三类内容：

- 论文写作与提交材料：`analysis/manuscript/`
- 论文使用的表格、图片、模型输入和抽取结果：`analysis/tables/`、`analysis/figures/`
- 可复现脚本、方法文档、参考文献和优秀论文参考：`src/`、`docs/`、`papers/`、`analysis/references/`

全量原始抓取数据、浏览器运行态、巨型中间表和渲染检查页已转入本地目录：

```text
_local_archive_not_for_github/
```

该目录被 `.gitignore` 忽略，只留在本机，不进入最终 GitHub 版本。

## 目录结构

```text
.
├── analysis/
│   ├── manuscript/              # 最终论文稿、PDF、AI 工具附页、写作模板
│   ├── tables/                  # 论文表格 CSV、模型输入、抽取结果和质量检查表
│   ├── figures/                 # 正文图表
│   ├── notes/                   # 写作修订、公式口径、表格精简等过程说明
│   └── references/              # 历年优秀论文与参赛材料参考
├── data/
│   ├── input/                   # 种子、配置、地区树和来源登记
│   ├── raw/                     # 仅保留目录说明；全量 raw 在本地归档
│   └── processed/               # 仅保留目录说明；大 CSV 在本地归档
├── docs/                        # 数据、抓取、分析管线和质量报告
├── papers/                      # 研究文献 PDF 与下载记录
├── src/                         # 抓取、清洗、静态分析与工具脚本
└── _local_archive_not_for_github/ # 本地保留，不提交
```

## 论文主入口

- Word 终稿：`analysis/manuscript/生成式人工智能冲击下城市就业技能结构重塑研究——基于招聘文本的技能抽取与大模型暴露度测度.docx`
- 渲染 PDF：`analysis/manuscript/final_pdf/genai_employment_exposure_保留原目录格式_修复版(1).pdf`
- AI 工具使用附页：`analysis/manuscript/supplementary/`
- 正文图：`analysis/figures/`
- 正文表：`analysis/tables/paper_tables_csv/`
- 模型输入与城市分类：`analysis/tables/model_inputs/`
- 技能/任务抽取结果与人工复核表：`analysis/tables/extraction_outputs/`

## 数据边界

为了让仓库适合未来上传到 GitHub，以下内容不再进入版本库：

- `data/raw/**/html/`、`records/`、`manifests/`
- `data/processed/**/*.csv`
- `data/runtime/`
- `analysis/job_level_scored.csv`
- 文档渲染页、临时下载/上传目录、第三方依赖缓存

这些内容已经按原路径移动到 `_local_archive_not_for_github/`。例如：

```text
_local_archive_not_for_github/data/processed/analysis_static/interfaces/job_level_master_static.csv
_local_archive_not_for_github/data/processed/51job/51job_social_jobs_clean_with_publish.csv
_local_archive_not_for_github/analysis/job_level_scored.csv
```

如需重新运行全量管线，先从本地归档恢复相应数据到原路径。

## 快速开始

```powershell
python -m pip install -r requirements.txt
```

51job 静态分析管线入口：

```powershell
python src/analysis_static/a_extraction/build_51job_master_static.py
python src/analysis_static/a_extraction/finalize_51job_master_static.py
```

注意：全量输入 CSV 默认不在 Git 版本内，需要从 `_local_archive_not_for_github/` 恢复。

## 重要提醒

- 不要把 `_local_archive_not_for_github/` 推送到远端。
- 参考论文 PDF 与历年优秀论文参考保留在仓库中，方便复盘写作依据。
- 所有分支整理均为本地操作；当前没有执行 `git push`。
