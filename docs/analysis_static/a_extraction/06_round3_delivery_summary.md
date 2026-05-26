# 06 第三轮精修交付总结

## 适用范围

- 本轮仅使用 `51job` 社招清洗后主文件：
  - `data/processed/51job/51job_social_jobs_clean_with_publish.csv`
- 不混用 `NCSS`，不引入新的外部样本。
- 当前主接口表仍固定为：
  - `data/processed/analysis_static/interfaces/job_level_master_static.csv`

## 本轮完成内容

- 对 `src/analysis_static/a_extraction/build_51job_master_static.py` 进行第三轮岗位族规则精修。
- 在既有规则基础上新增两类强化：
  - 高频精确标题规则
  - 高频 `keyword_seed` 强化规则
- 对易误伤模式进行了二次修正，避免 `管理培训生` 这类 `keyword_seed` 将非管理岗误分到 `综合管理`。
- 已完成全量重跑，并刷新以下正式产物：
  - `data/processed/analysis_static/interfaces/job_level_master_static.csv`
  - `data/processed/analysis_static/interfaces/schema_mapping_static.json`
  - `data/processed/analysis_static/a_extraction/job_family_rules.csv`
  - `data/processed/analysis_static/a_extraction/job_family_manual_review.csv`
  - `data/processed/analysis_static/a_extraction/city_manual_review.csv`
  - `docs/analysis_static/a_extraction/03_job_family_standardization.md`
  - `docs/analysis_static/a_extraction/05_extraction_quality_report.md`

## 核心改进结果

- 总样本量保持不变：`1111676`
- 城市低置信行数保持：`7439`
- 岗位族低置信行数由第二轮的 `268363` 降至 `176930`
  - 绝对减少：`91433`
  - 相对下降：约 `34.1%`
- `其他/待定` 由第二轮的 `126868` 降至 `97791`
  - 绝对减少：`29077`
  - 相对下降：约 `22.9%`
- `job_family_manual_review.csv` 去重复核项由第二轮的 `176067` 降至 `121026`
  - 绝对减少：`55041`
  - 相对下降：约 `31.3%`

## 当前岗位族结构（前 12）

- 销售/商务：`194810`
- 其他/待定：`97791`
- 生产/质量：`97135`
- 技工/操作：`90440`
- 供应链/物流/采购：`89535`
- 机械工程：`89187`
- 市场/运营：`58168`
- 硬件/电气：`43625`
- 人力/行政：`41700`
- 软件开发：`39963`
- 产品/项目：`38385`
- 财务/会计：`37928`

## 本轮重点吃掉的高频岗位

- 销售/商务：
  - `商务助理`
  - `商务主管`
  - `医学信息沟通专员`
- 综合管理：
  - `副总经理`
  - `总经理`
  - `总裁助理`
- 生产/质量：
  - `环保工程师`
  - `体系专员`
  - `品管员`
  - `检测工程师`
- 供应链/物流/采购：
  - `单证员`
  - `库管员`
  - `计划专员`
- 建筑/地产：
  - `土建工程师`
  - `测量工程师`
  - `厂务工程师`
- 硬件/电气：
  - `射频工程师`
  - `FPGA工程师`
  - `电子技术员`
- 研发/科研：
  - `产品开发工程师`
- 服务：
  - `美容师`
  - `诚聘轨道巡检人员+五险一金`

## 仍需人工复核的主要残余类型

- 标题天然泛化、仅靠规则难以安全归类的岗位仍是主残余来源：
  - `文员`
  - `技术员`
  - `实习生`
  - `应用工程师`
  - `工程师`
  - `技术经理`
- 语言支持类岗位尚未单独拆族，当前仍建议保留复核：
  - `英语翻译`
  - `日语翻译`
- 混合型工程岗位仍存在边界：
  - `仿真工程师`
  - `FAE工程师`
  - `视觉工程师`

## 对 B / C 线的可交付结论

- A 线主接口表已经达到可直接消费的交付状态，可供后续 `city-job_family` 聚合、指标构建和地图 join 使用。
- 当前最稳定的口径是：
  - 城市口径使用 `city_std_final`、`province_std_final`
  - 岗位族口径使用 `job_family_std`
  - 对建模时需要保守处理的样本，可结合 `job_family_confidence` 过滤低置信行
- 若 B 线追求更稳健主样本，建议优先使用：
  - `job_family_confidence in {high, medium}`
- 若需要继续压缩复核规模，下一轮最值得投入的是：
  - 泛化标题的半监督补标
  - 翻译/语言支持类单独口径
  - `GenAI` 技能词典扩充
