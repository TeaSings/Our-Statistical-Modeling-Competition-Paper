# src/platforms/ncss 说明

这个目录存放 `NCSS` 平台的抓取、解析、核验和进度监控脚本。

## 文件

- `fetch_ncss_jobs.py`：抓取 NCSS 列表接口，输出列表 RAW、详情种子和查询摘要
- `extract_ncss_area_codes.py`：从 NCSS 列表页 HTML 提取全部地区码
- `build_balanced_ncss_detail_seeds.py`：从列表结果中构造均衡样本详情种子
- `fetch_pages.py`：根据种子抓取详情页 HTML
- `parse_details.py`：按选择器解析详情页字段
- `rebuild_local_manifest.py`：根据本地 HTML 和主种子重建 manifest
- `watch_ncss_progress.py`：查看全量抓取和解析进度

## 推荐命令

列表抓取：

```powershell
python src/platforms/ncss/fetch_ncss_jobs.py --resume
```

详情页抓取：

```powershell
python src/platforms/ncss/fetch_pages.py `
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv `
  --manifest data/raw/ncss/manifests/ncss_detail_manifest_all_areas.jsonl `
  --output-dir data/raw/ncss/html `
  --config data/input/ncss/platform_ncss_detail.json `
  --skip-existing `
  --workers 16
```

详情页解析：

```powershell
python src/platforms/ncss/parse_details.py `
  --seed-file data/input/ncss/ncss_detail_urls_all_areas.csv `
  --config data/input/ncss/platform_ncss_detail.json `
  --output data/raw/ncss/records/ncss_jobs_all_areas_raw.jsonl `
  --overwrite `
  --workers 16
```

进度查看：

```powershell
python src/platforms/ncss/watch_ncss_progress.py --once
```

## 当前建议

- 现在最重要的不是继续发散更多 NCSS 脚本，而是围绕全国主种子和主 manifest 维护好现有主线
- 如果 manifest 跨机器失效或路径过期，优先用 `rebuild_local_manifest.py` 恢复，而不是直接重跑全量
