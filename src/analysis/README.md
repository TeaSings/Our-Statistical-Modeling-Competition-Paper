# src/analysis

这个目录存放“生成式 AI 城市技能熵研究”对应的代码化研究方案资产，目标是把原本的长篇方案文档拆成可维护、可校验、可生成文档的结构化文件。

## 目录说明

- `config/analysis_plan.json`
  - 研究总思路、数据表、理论机制、假设、处理流程、时空分析、稳健性、论文结构等总配置
- `config/model_specs.json`
  - 所有核心模型的公式、用途、识别逻辑、优缺点、推荐等级
- `config/variable_dictionary.csv`
  - 变量字典，适合后续直接接入代码、表格和附录
- `generate_plan_summary.py`
  - 从配置文件自动生成 Markdown 汇总文档

## 推荐使用方式

先在仓库根目录运行：

```powershell
python src/analysis/generate_plan_summary.py
```

成功后会生成：

- `docs/生成式AI城市技能熵研究方案-代码化汇总.md`

## 适合的协作方式

1. 讨论研究问题和变量时，优先修改 `config/analysis_plan.json`
2. 新增或调整回归模型时，修改 `config/model_specs.json`
3. 变量口径变动时，优先修改 `config/variable_dictionary.csv`
4. 文档需要同步更新时，重新运行 `generate_plan_summary.py`

## 设计原则

1. 尽量用结构化字段表达方案，而不是只写散文式说明
2. 主线方案与备选方案同时保留，方便后续降级或增强
3. 变量名统一用英文缩写，便于 Python / Stata / R 实现
4. 保持“文档即配置、配置可生成文档”的轻量流程
