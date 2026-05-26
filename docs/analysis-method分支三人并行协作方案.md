# analysis-method 分支三人并行协作方案

基线说明：

- 本文严格以 GitHub 仓库 `TeaSings/Our-Statistical-Modeling-Competition-Paper` 的 `analysis-method` 分支为唯一进度基准。
- 参考提交：`eb8bf6f6ed161f119d3d3eac5bcfe9885f497c11`
- 提交信息：`upload analysis method`
- 本文不把本地未追踪的 `analysis_static/` 等新增目录视为分支既有成果。
- 当前执行口径采用“去时间口径主线”：
  - 主分析单元：`city-job_family`
  - 辅助分析单元：`job`、`city`
  - 原方案中的 `*_cgt` 在当前执行中改写为 `*_cg`
  - 原方案中的 `*_ct` 在当前执行中改写为 `*_c`

---

## 一、analysis-method 分支现状清单

### 1.1 已明确存在的内容

#### 已有脚本

- 数据清洗主脚本：`src/clean_jobs.py`
- NCSS 抓取与解析链路：`src/platforms/ncss/*`
- 51job 抓取与清洗链路：`src/platforms/job51/*`
- 工具脚本：`src/tools/*`
- 研究方案生成脚本：`src/analysis/generate_plan_summary.py`

#### 已有数据快照

- `data/processed/ncss/ncss_jobs_all_areas_clean.csv`
- `data/processed/ncss/ncss_listings_all_areas_flat.csv`
- `data/processed/51job/51job_social_jobs_clean_with_publish.csv`
- `data/processed/51job/51job_campus_jobs_clean.csv`

#### 已有研究方案配置

- `docs/数据分析初步方法.md`
- `docs/生成式AI城市技能熵研究方案-代码化汇总.md`
- `src/analysis/README.md`
- `src/analysis/config/analysis_plan.json`
- `src/analysis/config/model_specs.json`
- `src/analysis/config/variable_dictionary.csv`

### 1.2 已验证的口径特征与版本滞后

- 仓库说明与数据快照之间存在口径滞后。
- `README.md` 与 `data/processed/README.md` 仍保留 51job 社招 active clean 为 `8184` 条的旧描述。
- `data/processed/51job/README.md` 仍提到旧文件 `51job_social_jobs_clean.csv` 以及旧快照规模。
- 但当前分支实际存在、且应优先使用的主文件是：
  - `data/processed/51job/51job_social_jobs_clean_with_publish.csv`
- 该文件按分支快照口径已是更新后的版本，因此协作中必须优先以“当前实际文件”而不是旧 README 数字为准。

### 1.3 分支中还不存在、但接下来必须新增的内容

#### 尚未生成的分析结果表

- `skill_extraction_table`
- 岗位族标准化结果表
- `panel_cg_base`
- `panel_c_base`
- `entropy_panel_cg`
- `entropy_panel_c`
- `sri_panel_cg`
- `genai_index_c`
- `exposure_g`
- `shock_c`
- `city_control_table_static`
- `model_input_cg`
- `model_input_c`
- 正式回归结果表
- 稳健性矩阵与结果汇总表

#### 尚未建设的可视化模块

- 地图前端目录
- 交互式 dashboard 或可视化应用目录
- 地图展示层指标表
- 省内下钻明细层指标表
- 地图数据契约文档
- 动态热力图前端原型

### 1.4 现阶段总判断

当前 `analysis-method` 分支的优势是“数据抓取底座 + 配置化研究方案 + 方法框架”已经较完整，短板是“分析结果表尚未落地、建模链路尚未跑通、可视化模块尚未建设”。因此最合理的组织方式不是继续把三个人都压在同一条清洗链上，而是尽快拆成三条并行但少耦合的执行线。

---

## 二、基于 analysis-method 分支的项目当前阶段判断

项目当前处于“由研究设计转向结果落地”的关键过渡阶段。研究问题、理论机制、变量体系、模型规格、抓取脚本、清洗脚本和数据快照都已经具备，但严格意义上的分析主表、指标面板、回归结果、稳健性结果、图表和可视化产品还没有形成。因此，这不是一个“继续讨论题目”的阶段，而是一个“快速形成标准化分析底表和最小可行结果链”的阶段。

当前最强的是：

- 数据抓取与清洗底座
- 研究框架和理论机制
- 变量字典与模型规格
- NCSS 与 51job 的 processed 数据快照

当前最弱的是：

- 城市口径与岗位族标准化落地
- 技能抽取与技能归一化
- 静态版 `city-job_family` 聚合表
- `GDI_c`、`Exposure_g`、`shock_c`
- 正式计量模型与稳健性
- 交互式地图可视化

---

## 三、为什么当前最适合采用三人并行协作

当前最适合三人并行推进，原因有三点：

- 第一，当前任务天然可拆成三段：从 clean 数据到标准化主表，从主表到指标与模型，从指标到交互式可视化与结果交付。
- 第二，这三段之间的耦合点相对有限，可以通过少量接口文件衔接，而不需要三个人共同改同一组核心脚本。
- 第三，新增的“动态可视化城市技能熵热力图”目标不能等分析结束后再做，否则必然在最后阶段挤压主线建模时间，因此必须从一开始纳入并行任务。

因此，最合理的不是“一个人做数据、一个人等数据、一个人最后出图”，而是：

- A：标准化与抽取线
- B：指标与建模线
- C：可视化与结果封装线

---

## 四、三人分工设计总原则

- 只以 `analysis-method` 分支既有内容为基线，不把本地临时目录算作已完成成果。
- 主线执行口径固定为静态版 `city-job_family`，不在当前阶段再回到完整时间面板主线。
- 三个人尽量不同时修改同一批文件，优先通过“接口文件”协作。
- 原始配置文件 `src/analysis/config/*` 原则上少改；当前静态版执行优先写入新增的说明文档与派生脚本。
- 每个人都必须有独立可提交的阶段性成果。
- 每个人都必须有清晰的 Definition of Done。
- 至少前 2-3 天内保证两人以上可以真实并行推进。
- 动态热力图必须是正式工作包，但不能反向拖慢核心建模主线。

---

## 五、三人分工总表

| 成员 | 核心职责 | 可立即启动的任务 | 主要输入 | 主要输出 | 接口文件 | 对其他人的依赖程度 | 独立性评价 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | 数据基线核对、城市口径、岗位族、技能抽取与归一化 | 分支基线核对、统一主表设计、城市标准化规则、岗位族规则、技能词典与抽取 | 分支现有 processed 数据、`clean_jobs.py`、`docs/数据分析初步方法.md`、变量字典 | 标准化后的 job-level 主表、词典、抽取表、人工复核表 | `job_level_master_static.csv` | 低 | 高 |
| B | 静态指标构建、聚合底表、解释变量、主模型与稳健性 | 静态口径改写、指标 schema 设计、控制变量 inventory、模型脚本骨架 | `analysis_plan.json`、`model_specs.json`、`variable_dictionary.csv`、A 的主表 | `panel_cg_base`、`panel_c_base`、`entropy_panel_*`、`sri_*`、`gdi_c`、`exposure_g`、`shock_c`、回归结果 | `city_metric_drilldown_static.csv` | 中 | 中高 |
| C | 动态热力图、地图数据接口、图表与结果交付骨架 | 地图产品方案、前端脚手架、地图数据契约、MVP 原型 | 原始方案、B 的指标 schema、A 的城市标准化规则 | 交互式地图 MVP、图表模板、结果展示骨架、数据接口适配器 | 消费 `job_level_master_static.csv` 与 `city_metric_drilldown_static.csv` | 低到中 | 高 |

---

## 六、成员 A 详细工作包

### 6.1 责任边界

A 只负责“从分支现有 clean 快照到可建模 job-level 主表”的全过程，不负责正式回归，不负责地图前端。

### 6.2 可立即启动的任务

1. 完成分支基线核对  
   输出文档：
   - `docs/analysis_static/a_extraction/00_branch_baseline_audit.md`  
   内容包括：
   - README、docs、processed README 与实际文件快照的差异
   - 当前主样本建议
   - 字段可用性说明

2. 设计静态版统一主表 schema  
   输出：
   - `data/processed/analysis_static/interfaces/schema_mapping_static.json`
   - `data/processed/analysis_static/interfaces/job_level_panel_union_raw.csv`

3. 城市标准化  
   输出：
   - `city_mapping_table.csv`
   - `city_manual_review.csv`
   - `job_level_panel_city_std.csv`
   - `02_city_standardization.md`

4. 岗位族标准化  
   输出：
   - `job_family_rules.csv`
   - `job_family_manual_review.csv`
   - `job_level_panel_job_family_std.csv`
   - `03_job_family_standardization.md`

5. 规则词典与人工标注样本  
   输出：
   - `annotation_seed_500.csv`
   - `skill_rule_dictionary_v1.csv`
   - `task_rule_dictionary_v1.csv`
   - `04_rule_dictionary_notes.md`

6. 技能/任务抽取与技能归一化  
   输出：
   - `skill_extraction_table_raw.csv`
   - `skill_extraction_table_final.csv`
   - `skill_token_norm.csv`
   - `skill_category_dict.csv`
   - `task_category_dict.csv`
   - 对应质量报告

### 6.3 建议新增目录

以下目录为本次协作建议新增，不属于 `analysis-method` 分支既有目录：

- `src/analysis_static/a_extraction/`
- `data/processed/analysis_static/a_extraction/`
- `docs/analysis_static/a_extraction/`

### 6.4 关键接口

A 的唯一主接口文件建议固定为：

- `data/processed/analysis_static/interfaces/job_level_master_static.csv`

该表应至少包含：

- `source_platform`
- `source_job_id`
- `job_title_std`
- `city_std_final`
- `province_std_final`
- `job_family_std`
- `salary_avg_month`
- `education_std`
- `experience_std`
- `jd_text_clean`
- `skill_list`
- `task_list`
- `genai_related_skill_list`
- `skill_category_list`
- `is_ai_native_job`
- `is_ai_augmented_job`
- `genai_exposure_level`

### 6.5 Definition of Done

- 已形成唯一可供建模与地图 join 使用的 `job_level_master_static.csv`
- 城市口径和岗位族口径均有 manual review 清单
- 技能抽取和技能归一化有明确字段与字典说明
- 关键字段缺失、主键唯一性、样本量变化都有质量说明

### 6.6 上游未完成时的兜底任务

如果 B 或 C 尚未准备好，A 仍可继续：

- 扩充技能与任务词典
- 补做岗位族低置信样本复核
- 完善城市标准化低置信样本复核
- 做 NCSS / 51job 的字段差异与文本质量对比说明

---

## 七、成员 B 详细工作包

### 7.1 责任边界

B 只负责“从标准化 job-level 主表到静态指标、模型输入和回归结果”，不负责最前面的抽取细节，不负责地图前端开发。

### 7.2 可立即启动的任务

1. 静态口径改写  
   输出：
   - `docs/analysis_static/b_modeling/00_static_scope_translation.md`  
   目标：
   - 把原始 `city-job_family-quarter` 方案翻译成当前静态版 `city-job_family`
   - 写清 `*_cgt -> *_cg`、`*_ct -> *_c`

2. 指标 schema 和聚合规则设计  
   输出：
   - `panel_schema_static.md`
   - `metric_spec_static.md`

3. 控制变量 inventory  
   输出：
   - `city_controls_inventory.md`
   - `city_control_table_static.csv` 的字段方案

4. 在 A 交付主接口表后，完成静态版聚合和建模  
   输出：
   - `panel_cg_base.csv`
   - `panel_c_base.csv`
   - `entropy_panel_cg.csv`
   - `entropy_panel_c.csv`
   - `sri_panel_cg.csv`
   - `genai_index_c.csv`
   - `exposure_g.csv`
   - `shock_c.csv`
   - `model_input_cg.csv`
   - `model_input_c.csv`
   - 主模型结果
   - 机制模型结果
   - 稳健性结果矩阵

### 7.3 建议新增目录

以下目录为本次协作建议新增，不属于 `analysis-method` 分支既有目录：

- `src/analysis_static/b_modeling/`
- `data/processed/analysis_static/b_modeling/`
- `docs/analysis_static/b_modeling/`

### 7.4 主模型口径

当前静态版主模型建议固定为：

```text
Y_cg = β1 (GDI_c × Exposure_g) + μ_c + ν_g + ε_cg
```

支持模型：

```text
Y_cg = β1 (GDI_c × Exposure_g) + β2 GDI_c + β3 Exposure_g + β4 X_c + ε_cg
```

机制模型的被解释变量优先为：

- `hos_cg`
- `css_cg`
- `rss_cg`
- `mss_cg`
- `sss_cg`

城市层支持模型：

```text
Y_c = α + β shock_c + γ X_c + ε_c
```

### 7.5 关键接口

B 的下游主接口文件建议固定为：

- `data/processed/analysis_static/interfaces/city_metric_drilldown_static.csv`

该表至少应包含：

- `province_std_final`
- `city_std_final`
- `job_count_c`
- `skill_entropy_c`
- `norm_skill_entropy_c`
- `sri_c`
- `gdi_c`
- `explicit_genai_share_c`
- `ai_native_job_share_c`
- `ai_augmented_job_share_c`

另建议单独输出地图展示层表：

- `province_metric_layer_static.csv`

### 7.6 Definition of Done

- 每张聚合表主键唯一且字段命名稳定
- 形成至少一版主模型、机制模型和稳健性结果
- 所有模型说明中明确写出“相关性/结构性差异”而非强因果
- 输出建模说明文档与稳健性总结文档

### 7.7 上游未完成时的兜底任务

如果 A 尚未完全交付，B 仍可继续：

- 完成静态口径改写文档
- 写好指标构造脚本骨架
- 做控制变量 inventory
- 先用显式 GenAI 词占比设计最小 GDI 原型
- 搭建回归脚本模板和表格导出模板

---

## 八、成员 C 详细工作包

### 8.1 责任边界

C 负责动态热力图、地图交互、图表模板和结果展示骨架，不承担全部论文写作，也不负责底层抽取或正式回归。

### 8.2 可立即启动的任务

1. 地图产品方案与数据契约  
   输出：
   - `docs/analysis_static/c_visual/visual_spec.md`
   - `docs/analysis_static/c_visual/map_data_contract.md`

2. 地图前端脚手架  
   输出：
   - 交互式前端原型
   - mock 数据驱动的省级热力图
   - 指标切换控件

3. 省级点击下钻 MVP  
   输出：
   - 点击某省后展示城市列表、城市指标卡或条形图

4. 与真实数据接口接入  
   在 B 提供真实指标后接入：
   - `province_metric_layer_static.csv`
   - `city_metric_drilldown_static.csv`

5. 图表与结果展示模板  
   输出：
   - 论文/汇报用图表模板
   - 页面说明文档

### 8.3 建议新增目录

以下目录为本次协作建议新增，不属于 `analysis-method` 分支既有目录：

- `apps/city_skill_entropy_map/`
- `data/processed/analysis_static/c_visual/`
- `docs/analysis_static/c_visual/`

### 8.4 动态热力图目标定义

当前阶段把“动态可视化”定义为“交互式动态展示”，而不是时间滑块动画。

当前正式目标：

- 在中国地图上展示省级热力分布
- 支持指标切换
- 点击某省后下钻查看该省内部城市多指标
- 支持 tooltip、排序、图例说明

当前不作为主线必交付的增强项：

- 时间滑块
- 动态季度动画
- 完整城市点状时序播放

### 8.5 热力图 MVP

MVP 应至少做到：

- 支持 5 个以上指标切换
- 默认展示全国省级热力
- 点击省份后展示省内城市指标列表
- 支持：
  - `skill_entropy_c`
  - `norm_skill_entropy_c`
  - `sri_c`
  - `gdi_c`
  - `explicit_genai_share_c`
  - `ai_native_job_share_c`
  - `ai_augmented_job_share_c`
  - `job_count_c`

### 8.6 Definition of Done

- 前端原型可运行
- mock 数据和真实数据都能驱动同一套交互逻辑
- 指标切换与省级下钻已打通
- 有清晰的数据契约文档和使用说明

### 8.7 上游未完成时的兜底任务

如果 B 尚未交付真实指标，C 仍可继续：

- 用 mock schema 跑通地图交互
- 完成地图样式、tooltip、图例、指标选择器
- 写好真实数据适配器
- 准备论文和答辩展示的图形模板

---

## 九、动态可视化热力图如何嵌入三人协作链

### 9.1 主责归属

- 主责：C
- A 提供：城市标准化口径、省市映射、城市 join 稳定性
- B 提供：地图展示层和下钻层真实指标

### 9.2 依赖关系最小化设计

为了避免“等模型全跑完才开始做图”，热力图开发分两阶段推进：

第一阶段：

- C 直接基于 mock 数据完成前端交互壳
- A 同步把城市标准化规则固定下来
- B 同步把指标接口 schema 固定下来

第二阶段：

- B 输出真实指标表
- C 只替换数据源，不重写交互框架

### 9.3 地图数据接口建议

建议固定两张接口表：

1. 地图展示层：

- `province_metric_layer_static.csv`

2. 省内下钻层：

- `city_metric_drilldown_static.csv`

### 9.4 为什么这一目标必须提前启动

如果等全部建模结束后再启动地图前端，会带来两个问题：

- 一是最后阶段前端工作会反向占用建模与论文收尾时间
- 二是地图需要的字段契约会倒逼 B 返工

因此，地图目标必须作为独立工作包并行推进，而不是最后附属装饰。

---

## 十、三人的接口设计与版本冻结方案

### 10.1 基础口径文件

以下文件属于基础口径文件，不建议多人同时修改：

- `src/analysis/config/analysis_plan.json`
- `src/analysis/config/model_specs.json`
- `src/analysis/config/variable_dictionary.csv`

建议规则：

- 原始方案配置默认冻结
- 如必须修改，由 B 在“模型口径冻结窗口”统一提交
- A 和 C 优先通过新增静态版说明文档和派生脚本落地当前执行口径

### 10.2 接口文件

最关键的两个接口文件：

1. `data/processed/analysis_static/interfaces/job_level_master_static.csv`
2. `data/processed/analysis_static/interfaces/city_metric_drilldown_static.csv`

### 10.3 目录责任边界

- A 负责：
  - `src/analysis_static/a_extraction/`
  - `data/processed/analysis_static/a_extraction/`
  - `docs/analysis_static/a_extraction/`
- B 负责：
  - `src/analysis_static/b_modeling/`
  - `data/processed/analysis_static/b_modeling/`
  - `docs/analysis_static/b_modeling/`
- C 负责：
  - `apps/city_skill_entropy_map/`
  - `data/processed/analysis_static/c_visual/`
  - `docs/analysis_static/c_visual/`

共享接口目录：

- `data/processed/analysis_static/interfaces/`
- `docs/analysis_static/interfaces/`

### 10.4 冻结节奏建议

- Day 1：冻结分支基线口径
- Day 3：冻结统一 schema 与城市/岗位族主口径
- Day 5：冻结变量和指标定义
- Day 7：冻结主模型规格
- Day 8：冻结地图数据契约

### 10.5 同步机制

每日同步只需要两类内容：

- 当日新增接口文件是否变化
- 是否触发口径冻结点

不建议把“大量讨论”和“频繁开会”作为主工作方式。

---

## 十一、三人的目标验收清单

### A 验收清单

- 完成 `00_branch_baseline_audit.md`
- 完成统一主表和字段映射
- 完成城市标准化结果表与 manual review
- 完成岗位族标准化结果表与 manual review
- 完成技能/任务词典与标注种子表
- 完成 `skill_extraction_table_final.csv`
- 完成 `job_level_master_static.csv`
- 完成对应质量说明文档

### B 验收清单

- 完成静态口径改写说明
- 完成 `panel_cg_base.csv` 与 `panel_c_base.csv`
- 完成熵指标、SRI、GDI、Exposure、Shock
- 完成最小控制变量表
- 完成 `model_input_cg.csv` 与 `model_input_c.csv`
- 完成主模型、机制模型、稳健性结果表
- 完成模型说明与识别边界文档

### C 验收清单

- 完成地图产品说明和数据契约文档
- 完成前端脚手架和 mock data MVP
- 完成指标切换与省级点击下钻
- 完成真实数据接口适配
- 完成图表模板与结果展示骨架
- 完成地图 MVP 演示版本及使用说明

---

## 十二、风险点、卡点与兜底方案

### 12.1 最大风险：口径漂移

当前最容易导致协作失败的环节不是技术实现，而是口径不一致。尤其是：

- 51job 社招快照在 README 与实际文件间存在描述滞后
- 原始研究方案是 `city-job_family-quarter`
- 当前执行口径是 `city-job_family`

如果不先冻结静态版执行口径，后续三条线会各自形成不同版本。

### 12.2 第二风险：A 与 B 的接口不清

如果 A 一边改列名、B 一边写聚合脚本，就会在聚合阶段频繁返工。

解决办法：

- 先冻结 `job_level_master_static.csv` 的字段契约
- 后续 A 可以改内容，但尽量不再改列名

### 12.3 第三风险：C 被误当成最后出图的人

如果地图任务被放到最后阶段才开始，动态热力图会直接挤占论文收尾和建模稳健性时间。

解决办法：

- C 在第一周就启动 mock 数据地图 MVP
- 把地图接口表当成正式成果文件，而不是临时展示材料

### 12.4 第四风险：把写作完全压给某一个人

论文写作不能单独压给 C，也不能最后才开始。

正确做法：

- A 写口径与标准化说明
- B 写模型与指标说明
- C 写可视化和结果展示说明

---

## 十三、最终建议

当前最推荐的三线拆法是：

- A：标准化与抽取线
- B：指标与模型线
- C：可视化与结果交付线

这是最适合 `analysis-method` 分支当前阶段的拆法，因为该分支已经有数据底座和研究配置，但最缺的是：

- 从 clean 快照走到标准化主表
- 从主表走到静态指标与模型
- 从指标走到交互式展示和可交付材料

### 13.1 哪个人最先开始做什么

- A 第一个启动：先做分支基线核对和统一主表 schema
- B 同步启动：静态口径改写和指标 schema
- C 同步启动：地图数据契约和 mock 前端

### 13.2 哪两个接口文件最关键

- `job_level_master_static.csv`
- `city_metric_drilldown_static.csv`

### 13.3 动态热力图的 MVP 应该由谁先搭什么

由 C 先搭：

- 省级热力图
- 指标切换
- 点击下钻省内城市列表
- mock 数据驱动的前端壳

然后再由 B 提供真实数据接入。

### 13.4 为什么这种分法最适合当前 GitHub 分支阶段

因为当前分支最强的不是结果，而是底座；最需要的不是继续讨论，而是快速把底座转成结果。A 决定底层数据质量，B 决定学术结果强度，C 决定展示与交付效果。三条线互相支撑，但不会把所有关键任务压到一个人身上，也不会让动态热力图拖慢核心建模主线。


