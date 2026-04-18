# 生成式AI城市技能熵研究方案：代码化说明

这份说明用于说明：如何把“生成式 AI 城市技能熵研究”的长篇方案转成适合仓库协作与 GitHub 分支维护的结构化文件。

## 当前新增内容

- `src/analysis/README.md`
- `src/analysis/config/analysis_plan.json`
- `src/analysis/config/model_specs.json`
- `src/analysis/config/variable_dictionary.csv`
- `src/analysis/generate_plan_summary.py`

## 为什么这样组织

如果只保留一篇长 Markdown，后续会遇到三个问题：

1. 变量口径修改后，正文和附录很容易不同步
2. 模型规格更新后，难以追踪具体改了哪些地方
3. 方案文档不方便直接接入后续脚本开发

因此这里采用“结构化配置 + 自动生成文档”的轻量方式：

- `analysis_plan.json` 管总方案
- `model_specs.json` 管模型
- `variable_dictionary.csv` 管变量口径
- `generate_plan_summary.py` 负责把配置重新生成 Markdown 汇总

## 使用方式

在仓库根目录运行：

```powershell
python src/analysis/generate_plan_summary.py
```

运行成功后会生成：

- `docs/生成式AI城市技能熵研究方案-代码化汇总.md`

## 推荐后续协作规则

1. 研究问题、处理流程、时空分析路径的变动，优先改 `analysis_plan.json`
2. 回归模型、公式、优缺点和推荐级别的变动，优先改 `model_specs.json`
3. 变量含义、构造方法、主线口径和备选口径的变动，优先改 `variable_dictionary.csv`
4. 每次较大改动后，都重新运行一次生成脚本并提交生成后的汇总文档
