# 数据目录说明

`data/` 目录按照数据生命周期拆成三层：

- `input/`：抓取配置、种子链接、来源登记表
- `raw/`：未经清洗的原始抓取结果、网页快照、抓取 manifest
- `processed/`：已经标准化、可直接分析的结果表

推荐把数据流理解成：

```text
input -> raw -> processed
```

当前项目已经实际跑通的主线是 `NCSS`，因此下面三个子目录里的说明都优先围绕 NCSS 数据组织。

## 使用建议

- 需要看“从哪里来”，优先读 `input/README.md`
- 需要看“原始抓了什么”，优先读 `raw/README.md`
- 需要看“最后拿什么建模”，优先读 `processed/README.md`

## 版本控制说明

- `data/input/` 中的小型配置文件适合纳入版本控制
- `data/raw/` 和 `data/processed/` 中的大文件默认按 `.gitignore` 规则忽略
- 即使某些大文件未被 Git 跟踪，也仍然属于项目正式目录的一部分
