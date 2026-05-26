# data/raw/51job 说明

这里存放 51job 原始抓取结果，目前有两条链路：

- 校招专题页抓取
- 社招搜索结果顺序抓取

## 子目录

- `records/`：标准化前的职位 RAW JSONL
- `manifests/`：校招处理日志，以及社招顺序抓取的 cursor / progress / 调度日志
- `html/pages/`：校招专题页 HTML 快照
- `html/assets/`：校招专题页依赖的本地 JS / HTML 资产快照

## 校招链路

- `records/51job_campus_jobs_raw.jsonl`：校招 RAW，当前 `249` 条
- `manifests/51job_campus_seed_manifest.jsonl`：按专题页记录处理结果
- `html/pages/`、`html/assets/`：专题页和依赖资源快照

## 社招链路

- `records/51job_social_jobs_raw_with_publish.jsonl`：社招带发布时间的 active RAW 检查点，当前 `8294` 条
- `manifests/51job_social_partition_manifest_with_publish.jsonl`：当前续跑主线的全国顺序分批规划与调度日志
- `manifests/51job_social_progress_with_publish.json`：watcher 读取的进度快照
- `manifests/51job_social_cursor_with_publish.json`：顺序调度器断点文件
- 旧的无发布时间 social 快照、旧 `cursor / progress / manifest / scheduler` 日志已经退场，可用清理脚本一次性清走

## 版本管理建议

- `records/51job_social_jobs_raw_with_publish.jsonl` 可以作为阶段性数据快照提交，方便组员直接分析
- `progress`、`cursor` 和调度日志仍然属于本地运行态，默认不提交
- 如果后续继续跑全国全量，建议按阶段固定新快照，而不是把运行中的状态文件一起推上去

## 清理命令

```powershell
python src/platforms/job51/cleanup_social_legacy_data.py --skip-runtime-profiles
```
