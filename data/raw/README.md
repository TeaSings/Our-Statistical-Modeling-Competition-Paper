# data/raw 说明

这个目录原本存放各平台原始抓取结果，包括 HTML 快照、JSONL 记录和 manifest。整理 GitHub 归档版本时，这些大体量文件已从工作树移出并保留在：

```text
_local_archive_not_for_github/data/raw/
```

保留在 Git 中的内容主要是目录说明和抓取脚本。原始数据可按原路径恢复，例如：

```text
_local_archive_not_for_github/data/raw/51job/records/
_local_archive_not_for_github/data/raw/ncss/html/
_local_archive_not_for_github/data/raw/ncss/manifests/
```

做论文复核或重新生成 clean 表时，先把需要的平台目录复制回 `data/raw/<platform>/`，再运行 `src/` 中的清洗或解析脚本。
