# 生成式人工智能冲击下城市就业技能结构重塑研究：基于招聘文本与大模型信息抽取

- 研究窗口：`2021-2026`
- 主分析单元：`city-job_family-quarter`
- 辅助分析单元：`job`, `city-quarter`
- 文档版本：`v1.0`

## 研究问题

1. 2021-2026 年间，随着 GenAI 扩散，中国城市招聘市场中的技能信息熵是否发生系统性变化。
2. 技能信息熵的变化表现为技能组合扩张，还是在部分岗位中出现标准化与收敛。
3. GenAI 发展对技能信息熵的影响是否在高暴露岗位族与低暴露岗位族之间存在显著差异。
4. GenAI 对技能信息熵的影响是否通过高阶技能提升、互补技能增加和常规技能压缩实现。
5. 技能信息熵的变化是否具有显著空间溢出效应，即高 GenAI 发展城市是否带动邻近城市同步重塑技能结构。

## 理论机制

主链：

- `GenAI发展 -> 岗位任务结构变化 -> 技能需求组合重构 -> 技能信息熵变化 -> 城市间异质性分化与空间扩散`

具体机制：

- `替代效应`：GenAI 替代规则明确、重复性强的任务，压缩常规技能需求，使部分岗位技能结构收敛。
- `互补效应`：GenAI 与 Prompt 工程、审校优化、业务理解、跨部门协同等能力互补，推动技能维度扩展。
- `任务重组效应`：岗位从单点执行转向生成、筛选、审校、优化和交付的链式结构，提升复合技能需求。
- `技能升级效应`：招聘要求更强调高阶认知、沟通、项目管理和 AI 工具使用能力。
- `空间溢出效应`：高数字化与高创新城市率先吸收 GenAI，并通过技术、产业和人才流动影响邻近城市。

## 研究假设

- `H1`：GenAI 发展水平越高，城市-岗位族-季度层面的技能信息熵越高。
  理论依据：互补效应与任务重组效应扩大技能组合维度。
  检验方向：`positive`
- `H2`：GenAI 发展对高暴露岗位族技能信息熵的促进作用显著强于低暴露岗位族。
  理论依据：高暴露岗位更易被技术重组。
  检验方向：`positive_interaction`
- `H3`：GenAI 发展会显著提高高阶认知技能占比。
  理论依据：技能升级效应增强分析、判断和设计能力需求。
  检验方向：`positive`
- `H4`：GenAI 发展会显著提高互补技能占比。
  理论依据：人机协同使 AI 工具使用、审校优化和业务理解能力重要性上升。
  检验方向：`positive`
- `H5`：GenAI 发展会显著压缩常规技能占比。
  理论依据：替代效应使标准化与重复性任务被弱化。
  检验方向：`negative`
- `H6`：高阶技能和互补技能是 GenAI 影响技能信息熵的重要中介机制。
  理论依据：技能熵变化来自结构性技能重组，而非单纯 AI 词频提升。
  检验方向：`mediated_positive`
- `H7`：GenAI 发展与技能信息熵之间存在非线性关系。
  理论依据：扩散初期技能扩张，后期部分岗位技能标准化趋稳。
  检验方向：`positive_then_flatten`
- `H8`：高数字经济、高创新和高高教资源城市中，GenAI 的技能重塑效应更强。
  理论依据：技术吸收能力与人才供给能力更强。
  检验方向：`positive_heterogeneity`
- `H9`：技能信息熵存在显著空间集聚与溢出效应。
  理论依据：技术扩散、产业链联系与人才流动具有区域传导性。
  检验方向：`positive_spillover`

## 变量字典摘要

| 变量名 | 变量组 | 中文含义 | 层级 | 主线口径 | 备选口径 |
|---|---|---|---|---|---|
| `skill_entropy_cgt` | dependent | 城市-岗位族-季度技能信息熵 | city-job_family-quarter | 一级技能类别熵 | 二级技能词熵 |
| `norm_skill_entropy_cgt` | dependent | 标准化技能信息熵 | city-job_family-quarter | 标准化熵 | 最大值归一化熵 |
| `skill_entropy_ct` | dependent | 城市-季度技能信息熵 | city-quarter | 城市层技能类别熵 | 城市层任务熵 |
| `task_entropy_cgt` | dependent | 城市-岗位族-季度任务熵 | city-job_family-quarter | 一级任务类别熵 | 任务词熵 |
| `sri_cgt` | dependent | 技能重塑指数 | city-job_family-quarter | 加权综合指数 | PCA 指数 |
| `hos_cgt` | mechanism | 高阶技能占比 | city-job_family-quarter | 标签均值 | 文本分类概率均值 |
| `css_cgt` | mechanism | 互补技能占比 | city-job_family-quarter | 标签均值 | 关键词比重 |
| `rss_cgt` | mechanism | 常规技能占比 | city-job_family-quarter | 标签均值 | 常规任务关键词比例 |
| `mss_cgt` | mechanism | 管理技能占比 | city-job_family-quarter | 标签均值 | 文本分类分值 |
| `sss_cgt` | mechanism | 社交协作技能占比 | city-job_family-quarter | 标签均值 | 文本分类分值 |
| `gss_cgt` | mechanism | GenAI技能占比 | city-job_family-quarter | 技能词占比 | 显式 GenAI 岗位占比 |
| `salary_premium_cgt` | dependent | 薪资溢价 | city-job_family-quarter | 均值差 | 对数薪资回归系数 |
| `edu_upgrade_cgt` | dependent | 学历升级程度 | city-job_family-quarter | 本科及以上占比 | 硕士及以上占比 |
| `gdi_ct` | core_explanatory | 城市-季度 GenAI 发展指数 | city-quarter | PCA | 熵值法 |
| `gdi_t` | core_explanatory | 全国季度 GenAI 发展指数 | quarter | 全国综合指数 | 显式岗位占比趋势 |
| `explicit_genai_share_ct` | core_explanatory | 城市显式 GenAI 岗位占比 | city-quarter | 规则词典法 | LLM 判别法 |
| `genai_exposure_g` | core_explanatory | 岗位族 GenAI 暴露度 | job_family | 规则+LLM 综合分数 | 文献映射指数 |
| `shock_ct` | core_explanatory | 城市 GenAI 冲击强度 | city-quarter | share_base × exposure × gdi_t | share_base × exposure × gdi_ct |
| `ln_gdp_pc_ct` | control | 人均GDP对数 | city-quarter | 对数形式 | 原值 |
| `tertiary_share_ct` | control | 第三产业占比 | city-quarter | 连续变量 | 高低组虚拟变量 |
| `digital_economy_ct` | control | 数字经济水平 | city-quarter | 连续变量 | 高低组分组 |
| `internet_ct` | control | 网络基础设施水平 | city-quarter | 连续变量 | 分位数组 |
| `college_share_ct` | control | 高教资源水平 | city-quarter | 连续变量 | 高低组虚拟变量 |
| `innovation_index_ct` | control | 创新能力指数 | city-quarter | 连续变量 | 高低组虚拟变量 |
| `city_tier_c` | heterogeneity | 城市等级 | city | 三分类变量 | 分值型变量 |
| `digital_high_c` | heterogeneity | 高数字经济组 | city | 中位数二分 | 三分位分组 |
| `innovation_high_c` | heterogeneity | 高创新组 | city | 中位数二分 | 三分位分组 |
| `college_high_c` | heterogeneity | 高教育资源组 | city | 中位数二分 | 连续交互 |
| `industry_group_g` | heterogeneity | 行业组 | job_family_or_industry | 三大组 | 六大组 |
| `company_type_group` | heterogeneity | 企业性质组 | job_or_aggregate | 民企/国企/外企/事业单位 | 二元变量 |
| `W_skill_entropy_ct` | spatial | 技能熵空间滞后项 | city-quarter | 邻接矩阵 | 距离倒数矩阵 |
| `W_gdi_ct` | spatial | GenAI指数空间滞后项 | city-quarter | 邻接矩阵 | 经济距离矩阵 |
| `moran_i_t` | spatial | 全局 Moran's I | quarter | 邻接权重 | 距离权重 |
| `lisa_cluster_ct` | spatial | 局部空间聚类类型 | city-quarter | LISA 聚类 | 热点/冷点分析 |
| `skill_entropy_alt1` | robustness | 二级技能词熵 | city-job_family-quarter | 二级技能词熵 | N/A |
| `task_entropy_alt` | robustness | 替代任务熵 | city-job_family-quarter | 任务类别熵 | 任务词熵 |
| `gdi_alt_ct` | robustness | 替代 GenAI 指数 | city-quarter | 简化指数 | 等权平均指数 |
| `exposure_alt_g` | robustness | 替代岗位族暴露度 | job_family | 规则法 | 文献映射法 |
| `sri_alt_cgt` | robustness | 替代技能重塑指数 | city-job_family-quarter | PCA | 替代权重法 |

## 数据处理流程

### 步骤 1：多平台岗位数据合并

- 输入：`platform raw csv/json`
- 处理逻辑：统一字段名、编码、平台标识和日期格式。
- 输出：`job_level_panel_union_raw`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 2：岗位正文清洗

- 输入：`jd_text_raw`
- 处理逻辑：删除 HTML、模板文本和乱码，保留职责、要求、技能相关正文。
- 输出：`job_level_panel_text_clean`
- 自动化程度：`high`
- 人工校验强度：`medium`

### 步骤 3：城市标准化

- 输入：`city_raw`, `province_raw`
- 处理逻辑：统一到地级市口径，映射区县和园区到城市级。
- 输出：`city_std_final`
- 自动化程度：`high`
- 人工校验强度：`high`

### 步骤 4：时间标准化

- 输入：`publish_date`
- 处理逻辑：派生月份、季度和年份字段。
- 输出：`job_level_panel_time_std`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 5：岗位族标准化

- 输入：`job_title_std`, `jd_text_clean`, `industry_std`
- 处理逻辑：采用规则词典加 LLM 辅助，把岗位映射到统一岗位族。
- 输出：`job_family_std`
- 自动化程度：`medium`
- 人工校验强度：`high`

### 步骤 6：技能词归一化

- 输入：`skill_list`
- 处理逻辑：做同义词归并、中英文统一和工具类技能标准化。
- 输出：`skill_token_norm`
- 自动化程度：`medium`
- 人工校验强度：`high`

### 步骤 7：技能分类体系构建

- 输入：`skill_token_norm`
- 处理逻辑：构建技能词到一级技能类、二级技能类的映射。
- 输出：`skill_category_dict`
- 自动化程度：`medium`
- 人工校验强度：`high`

### 步骤 8：任务分类体系构建

- 输入：`task_list`
- 处理逻辑：建立统一任务类别，用于任务熵和机制分析。
- 输出：`task_category_dict`
- 自动化程度：`medium`
- 人工校验强度：`medium`

### 步骤 9：双通道抽取融合

- 输入：`rule extraction`, `llm extraction`
- 处理逻辑：规则先抽显式技能与任务，LLM 补充隐性技能、GenAI 标签和任务属性。
- 输出：`skill_extraction_table_final`
- 自动化程度：`medium`
- 人工校验强度：`high`

### 步骤 10：缺失值处理

- 输入：`job_level_panel`, `skill_extraction_table_final`
- 处理逻辑：核心文本和城市字段缺失样本剔除，薪资与学历缺失分层处理。
- 输出：`job_level_panel_missing_handled`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 11：岗位去重

- 输入：`job_level_panel_missing_handled`
- 处理逻辑：按平台、企业、城市、岗位、月份和薪资去重，优先保留文本最完整记录。
- 输出：`job_level_panel_dedup`
- 自动化程度：`high`
- 人工校验强度：`medium`

### 步骤 12：异常值处理

- 输入：`salary_avg_month`, `text length`, `skill count`
- 处理逻辑：薪资 winsorize，异常文本长度与异常技能数样本抽查。
- 输出：`job_level_panel_ready`
- 自动化程度：`high`
- 人工校验强度：`medium`

### 步骤 13：构建城市-岗位族-季度面板

- 输入：`job_level_panel_ready`, `skill_extraction_table_final`
- 处理逻辑：聚合生成岗位数、技能分布、任务分布、薪资和学历指标。
- 输出：`panel_cgt_base`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 14：构建城市-季度面板

- 输入：`panel_cgt_base`
- 处理逻辑：城市层再聚合，用于空间模型与城市层冲击分析。
- 输出：`panel_ct_base`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 15：构建技能熵与任务熵指标

- 输入：`panel_cgt_base`, `panel_ct_base`
- 处理逻辑：计算技能熵、标准化技能熵和任务熵。
- 输出：`entropy_panel_cgt`, `entropy_panel_ct`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 16：构建 GenAI 发展指数

- 输入：`city_quarter_external_table`, `explicit_genai_share_ct`
- 处理逻辑：标准化指标后用 PCA 或熵值法合成城市季度 GenAI 指数和全国季度 GenAI 指数。
- 输出：`genai_index_ct`, `genai_index_t`
- 自动化程度：`high`
- 人工校验强度：`medium`

### 步骤 17：构建岗位族暴露度

- 输入：`task characteristics`, `llm labels`, `external mapping`
- 处理逻辑：构建岗位族 GenAI 暴露度，用于交互模型和 shift-share。
- 输出：`exposure_g`
- 自动化程度：`medium`
- 人工校验强度：`high`

### 步骤 18：构建城市冲击强度

- 输入：`base shares`, `exposure_g`, `genai_index_t`
- 处理逻辑：基于基期岗位结构与暴露度构造城市层 shift-share 冲击。
- 输出：`shock_ct`
- 自动化程度：`high`
- 人工校验强度：`low`

### 步骤 19：拼接空间分析底表

- 输入：`panel_ct_base`, `city_spatial_table`
- 处理逻辑：生成空间权重矩阵和空间计量所需底表。
- 输出：`spatial_panel_ct`
- 自动化程度：`high`
- 人工校验强度：`low`

## 模型规格摘要

### 描述统计模型

- 模型层级：`supporting`
- 公式：`N/A`
- 目的：展示结构事实、时间趋势、城市差异和岗位族差异。
- 识别逻辑：不做因果识别，作为后续计量模型的事实基础。
- 被解释变量：`skill_entropy_ct`, `skill_entropy_cgt`, `gdi_t`, `gdi_ct`
- 优点：
  - 直观
  - 适合答辩展示
  - 便于定位关键拐点
- 局限：
  - 不能识别净效应
  - 无法剥离共同趋势
- 是否推荐：`true`

### 基准双向固定效应模型

- 模型层级：`main`
- 公式：`Y_cgt = alpha + beta1 * GDI_ct + beta2 * X_ct + mu_c + nu_g + tau_t + eps_cgt`
- 目的：检验 GenAI 发展对技能熵和技能重塑的总体影响。
- 识别逻辑：利用城市、岗位族和时间三个维度的面板变化，并控制固定效应。
- 被解释变量：`skill_entropy_cgt`, `task_entropy_cgt`, `sri_cgt`
- 核心解释变量：`GDI_ct`, `X_ct`
- 固定效应：`city_fe`, `job_family_fe`, `quarter_fe`
- 优点：
  - 结构清楚
  - 实现简单
  - 适合作为基准回归
- 局限：
  - 城市层内生性仍可能存在
  - 不能直接体现岗位暴露差异
- 是否推荐：`true`

### GenAI发展指数 × 岗位暴露度交互模型

- 模型层级：`main`
- 公式：`Y_cgt = alpha + beta1 * (GDI_ct * Exposure_g) + beta2 * GDI_ct + beta3 * Exposure_g + beta4 * X_ct + mu_c + nu_g + tau_t + eps_cgt`
- 目的：识别高暴露岗位族是否更快发生技能结构重塑。
- 识别逻辑：利用城市技术发展差异与岗位族暴露差异的交互进行异质化识别。
- 被解释变量：`skill_entropy_cgt`, `task_entropy_cgt`, `sri_cgt`, `hos_cgt`, `css_cgt`, `rss_cgt`
- 核心解释变量：`GDI_ct`, `Exposure_g`, `GDI_ct * Exposure_g`
- 固定效应：`city_fe`, `job_family_fe`, `quarter_fe`
- 优点：
  - 最贴合论文题目
  - 解释力强
  - 非常适合答辩展示边际效应图
- 局限：
  - 暴露度测算质量决定结果上限
  - 岗位族过细时样本会变稀
- 是否推荐：`true`

### 连续型 GenAI 指数模型

- 模型层级：`supporting`
- 公式：`Y_cgt = alpha + beta1 * GDI_ct + beta2 * X_ct + mu_c + nu_g + tau_t + eps_cgt`
- 目的：强调技术扩散是连续进程，而非单一事件冲击。
- 识别逻辑：使用连续指数替代虚拟变量，识别技术扩散随时间累计的影响。
- 被解释变量：`skill_entropy_cgt`, `task_entropy_cgt`
- 核心解释变量：`GDI_ct`
- 优点：
  - 更贴合 2021-2026 现实
  - 与技术发展路径一致
- 局限：
  - 时间趋势共线性问题更强
- 是否推荐：`false`

### 城市层 shift-share 冲击模型

- 模型层级：`main`
- 公式：`Y_ct = alpha + beta1 * Shock_ct + beta2 * X_ct + mu_c + tau_t + eps_ct ; Shock_ct = sum_g(Share_cg0 * Exposure_g * GDI_t)`
- 目的：识别城市基期岗位结构差异导致的异质冲击。
- 识别逻辑：将全国技术前沿视为共同冲击，城市基期结构视为预先给定的传导权重。
- 被解释变量：`skill_entropy_ct`, `norm_skill_entropy_ct`, `sri_ct`
- 核心解释变量：`Shock_ct`
- 固定效应：`city_fe`, `quarter_fe`
- 优点：
  - 识别逻辑更强
  - 劳动经济学范式清晰
  - 能增强论文学术性
- 局限：
  - 对基期结构质量要求高
  - 基期选择敏感
- 是否推荐：`true`

### 中介效应模型

- 模型层级：`supporting`
- 公式：`M_cgt = alpha + theta1 * (GDI_ct * Exposure_g) + theta2 * X_ct + FE + eps ; Y_cgt = alpha + beta1 * (GDI_ct * Exposure_g) + beta2 * M_cgt + beta3 * X_ct + FE + eps`
- 目的：解释 GenAI 如何通过高阶技能、互补技能和常规技能变化传导到技能熵。
- 识别逻辑：检验技术发展对机制变量的影响，以及机制变量对核心结果变量的影响。
- 被解释变量：`hos_cgt`, `css_cgt`, `rss_cgt`, `skill_entropy_cgt`
- 核心解释变量：`GDI_ct * Exposure_g`, `M_cgt`
- 优点：
  - 能把机制讲透
  - 适合论文主体中的解释章节
- 局限：
  - 中介识别的因果解释弱于实验设计
  - 模型层数增加后展示复杂
- 是否推荐：`true`

### 非线性模型

- 模型层级：`enhancement`
- 公式：`Y_cgt = alpha + beta1 * GDI_ct + beta2 * GDI_ct_sq + beta3 * X_ct + mu_c + nu_g + tau_t + eps_cgt`
- 目的：检验技能熵是否存在先扩张后趋稳的阶段性变化。
- 识别逻辑：用二次项刻画技术扩散初期与中后期的差异化影响。
- 被解释变量：`skill_entropy_cgt`, `sri_cgt`
- 核心解释变量：`GDI_ct`, `GDI_ct_sq`
- 优点：
  - 与理论阶段性一致
  - 容易形成有层次的结论
- 局限：
  - 对长时间序列质量要求更高
- 是否推荐：`false`

### 事件研究模型

- 模型层级：`enhancement`
- 公式：`Y_cgt = alpha + sum_{k != -1}(delta_k * D_event_k_t * Exposure_g) + mu_c + nu_g + tau_t + eps_cgt`
- 目的：观察关键事件前后高暴露岗位族技能结构变化的动态路径。
- 识别逻辑：通过相对事件时间哑变量检验前趋势和平行趋势。
- 被解释变量：`skill_entropy_cgt`, `hos_cgt`, `css_cgt`, `rss_cgt`
- 核心解释变量：`relative_time_dummies * Exposure_g`
- 优点：
  - 动态图形强
  - 适合答辩展示
- 局限：
  - 对早期样本要求高
  - 事件窗口过长时估计不稳
- 是否推荐：`false`

### 空间杜宾模型

- 模型层级：`main`
- 公式：`Y_ct = rho * W_Y_ct + beta * GDI_ct + theta * W_GDI_ct + gamma * X_ct + eta * W_X_ct + mu_c + tau_t + eps_ct`
- 目的：识别技能熵的空间依赖与 GenAI 发展的城市间溢出效应。
- 识别逻辑：同时估计本地解释变量效应和邻近城市解释变量溢出效应。
- 被解释变量：`skill_entropy_ct`, `norm_skill_entropy_ct`, `sri_ct`
- 核心解释变量：`GDI_ct`, `W_GDI_ct`, `W_Y_ct`
- 优点：
  - 最适合时空演化主题
  - 兼顾本地效应与外溢效应
- 局限：
  - 模型解释复杂
  - 对权重矩阵设定敏感
- 是否推荐：`true`

### 异质性模型

- 模型层级：`supporting`
- 公式：`Y_cgt = alpha + beta1 * (GDI_ct * Exposure_g * Z_c) + beta2 * X_ct + mu_c + nu_g + tau_t + eps_cgt`
- 目的：识别不同类型城市中 GenAI 技能重塑效应的差异。
- 识别逻辑：通过城市特征分组或三重交互比较高低组差异。
- 被解释变量：`skill_entropy_cgt`, `sri_cgt`
- 核心解释变量：`GDI_ct`, `Exposure_g`, `Z_c`, `three_way_interaction`
- 优点：
  - 政策含义强
  - 便于形成城市分类建议
- 局限：
  - 分组过多会稀释样本
  - 结果展示容易碎片化
- 是否推荐：`true`

## 时空分析

主结果建议：

- 时间趋势分析
- 分阶段演化分析
- 全局空间自相关分析
- 局部空间自相关分析
- 城市分层演化分析

增强结果建议：

- 关键事件前后变化分析
- 时空迁移分析
- 热点/冷点识别
- 空间 Markov 或 ESTDA

## 稳健性与识别增强

- 更换核心解释变量口径：gdi_ct / gdi_t / explicit_genai_share_ct
- 更换熵指标口径：技能类别熵 / 技能词熵 / 任务熵
- 更换时间粒度：季度 / 月度 / 半年
- 更换样本范围：前100城 / 去掉直辖市 / 非AI原生岗位
- 剔除 AI 原生岗位
- 替代事件时间点：2022-11-30 / 2023-03-14 / 2023-08-15
- 滞后项检验：L1.gdi_ct / L2.gdi_ct
- 空间权重矩阵替代：邻接 / 距离倒数 / 经济距离
- 替代暴露度测算：规则法 / LLM 法 / 文献映射法
- 内生性缓解：shift-share、滞后项、固定效应、基期 AI 专利与数字基础设施候选工具变量

## 论文结构建议

1. 绪论
2. 文献综述与理论分析
3. 数据来源、处理流程与指标构建
4. 技能信息熵的时空演化特征
5. GenAI 发展对技能信息熵的实证检验
6. 机制、异质性与空间溢出分析
7. 稳健性检验
8. 结论与政策建议

## 最推荐的 3 个主模型

1. `interaction_main`：最贴合题目，能同时体现城市技术发展差异和岗位暴露差异。
2. `shift_share`：识别逻辑更强，能提升论文的学术说服力。
3. `spatial_durbin`：最能体现时空序列变化与空间溢出机制。

## 数据质量不及预期时的降级方案

- 时间窗口降级：`2022-2026`, `2023-2026`
- 城市范围降级：`前100城`, `前50城`
- 岗位族粒度降级：`12类`, `8类`, `6类`
- 必保模型：`描述统计`, `基准双向固定效应`, `GDI×Exposure 交互模型`
- 可删减模型：`空间杜宾模型`, `复杂中介链`, `事件研究`, `shift-share`
