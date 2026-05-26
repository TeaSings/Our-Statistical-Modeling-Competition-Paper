        # 03 岗位族标准化说明

        ## 方法

        - 岗位族标准化仅基于 51job 的 `job_title_std`、`keyword_seed` 与 `job_tags_raw`。
        - 第一版共设置 `21` 个岗位族代码，其中常用主族包括：`AI/算法`、`软件开发`、`销售/商务`、`市场/运营`、`供应链/物流/采购`、`机械工程`、`生产/质量` 等。
        - 规则命中后按 `score -> priority` 排序，形成 `job_family_std` 与 `job_family_confidence`。

        ## 结果摘要

        - 总样本：`1111676`
        - 高置信岗位族：`789356`
        - 中置信岗位族：`169922`
        - 低置信岗位族：`152398`

        ## 高频岗位族

        - 销售/商务: 194138
- 生产/质量: 102689
- 技工/操作: 90793
- 机械工程: 90658
- 供应链/物流/采购: 89472
- 其他/待定: 85532
- 市场/运营: 60585
- 硬件/电气: 43903
- 人力/行政: 42853
- 软件开发: 40093
- 产品/项目: 38051
- 财务/会计: 37887

        ## 产物

        - `data/processed/analysis_static/a_extraction/job_family_rules.csv`
        - `data/processed/analysis_static/a_extraction/job_family_manual_review.csv`
        - `data/processed/analysis_static/a_extraction/job_level_panel_job_family_std.csv`
