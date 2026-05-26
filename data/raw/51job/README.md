# 51job 原始数据

本目录是 51job 原始抓取产物的恢复位置。

预期结构包括：

- `records/`：JSONL 格式原始岗位记录。
- `manifests/`：抓取 manifest、cursor 和进度日志。
- `html/`：保存的页面和相关资源。

这些文件体量较大，不进入 Git；路径保留用于脚本复现。
