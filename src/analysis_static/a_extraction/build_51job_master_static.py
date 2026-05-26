from __future__ import annotations

import argparse
import csv
import functools
import json
import random
import re
import sys
import textwrap
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

THIS_DIR = Path(__file__).resolve().parent
SRC_DIR = THIS_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from common import ROOT_DIR, clean_text, configure_utf8_stdio

csv.field_size_limit(1024 * 1024 * 64)

DEFAULT_INPUT = Path("data/processed/51job/51job_social_jobs_clean_with_publish.csv")
DEFAULT_OUTPUT_ROOT = Path("data/processed/analysis_static")
DEFAULT_DOCS_ROOT = Path("docs/analysis_static/a_extraction")
AREAS_PATH = Path("data/input/ncss/ncss_area_codes_all.csv")
JOB51_AREA_TREE_PATH = Path("data/input/51job/51job_search_area_tree.json")

DIRECT_MUNICIPALITIES = {"北京", "上海", "天津", "重庆"}
SPECIAL_PROVINCE_ALIASES = {
    "北京市": "北京",
    "天津市": "天津",
    "上海市": "上海",
    "重庆市": "重庆",
    "广西壮族自治区": "广西",
    "宁夏回族自治区": "宁夏",
    "新疆维吾尔自治区": "新疆",
    "西藏自治区": "西藏",
    "内蒙古自治区": "内蒙古",
    "香港特别行政区": "香港",
    "澳门特别行政区": "澳门",
}


@dataclass(frozen=True)
class RuleRecord:
    code: str
    label: str
    category: str
    pattern: str
    source_field: str
    weight: int
    priority: int
    notes: str = ""


@dataclass(frozen=True)
class TermRule:
    norm: str
    category: str
    patterns: tuple[str, ...]
    is_genai: bool = False
    notes: str = ""


JOB_FAMILY_RULES: tuple[RuleRecord, ...] = (
    RuleRecord("ai_algorithm", "AI/算法", "job_family", "人工智能|算法|机器学习|深度学习|自然语言处理|AIGC|大模型|Prompt|智能体|LLM|RAG|计算机视觉|推荐算法|数据科学家", "job_title_std", 6, 1, "AI 原生/算法岗位"),
    RuleRecord("ai_algorithm", "AI/算法", "job_family", "人工智能|算法|机器学习|深度学习|自然语言处理|AIGC|大模型|Prompt|智能体|LLM|RAG|计算机视觉|推荐算法|数据科学家", "keyword_seed", 5, 1, "AI 原生/算法岗位"),
    RuleRecord("ai_algorithm", "AI/算法", "job_family", "^(图像算法工程师)$", "keyword_seed", 6, 1, "高频 AI keyword_seed 精修"),
    RuleRecord("ai_algorithm", "AI/算法", "job_family", "^(机器视觉工程师)$", "job_title_std", 8, 1, "高频机器视觉岗位精修"),
    RuleRecord("data_analytics", "数据分析", "job_family", "数据分析|数据运营|商业分析|BI|数据治理|数据挖掘|数据专员|数据建模|数仓|ETL|报表", "job_title_std", 5, 2, "数据分析与 BI"),
    RuleRecord("data_analytics", "数据分析", "job_family", "数据分析|数据运营|商业分析|BI|数据治理|数据挖掘|数据专员|数据建模|数仓|ETL|报表", "keyword_seed", 4, 2, "数据分析与 BI"),
    RuleRecord("data_analytics", "数据分析", "job_family", "^(数据分析师)$", "keyword_seed", 6, 2, "高频数据分析 keyword_seed 精修"),
    RuleRecord("software_dev", "软件开发", "job_family", "软件|开发工程师|后端|前端|全栈|程序员|架构师|运维工程师|测试开发|Java|Python|Go|PHP|C\\+\\+|C#|安卓|iOS|Web|小程序|嵌入式软件|网络工程师|网络安全工程师|系统工程师|信息安全工程师", "job_title_std", 5, 3, "软件研发岗位"),
    RuleRecord("software_dev", "软件开发", "job_family", "软件|开发工程师|后端|前端|全栈|程序员|架构师|运维工程师|测试开发|Java|Python|Go|PHP|C\\+\\+|C#|安卓|iOS|Web|小程序|嵌入式软件|网络工程师|网络安全工程师|系统工程师|信息安全工程师", "keyword_seed", 4, 3, "软件研发岗位"),
    RuleRecord("software_dev", "软件开发", "job_family", "^(网络工程师|网络安全工程师|系统工程师)$", "job_title_std", 8, 3, "高频 IT/网络岗位精修"),
    RuleRecord("software_dev", "软件开发", "job_family", "^(运维工程师|系统工程师|IT经理 or IT主管)$", "keyword_seed", 6, 3, "高频软件/IT keyword_seed 精修"),
    RuleRecord("software_dev", "软件开发", "job_family", "^(IT工程师)$", "job_title_std", 8, 3, "高频 IT 岗位精修"),
    RuleRecord("software_dev", "软件开发", "job_family", "^(高级软件工程师|电子软件开发 or ARM or 电子软件开发 or MCU|嵌入式硬件开发\\(主板机等\\))$", "keyword_seed", 6, 3, "高频软件 keyword_seed 精修"),
    RuleRecord("software_dev", "软件开发", "job_family", "^(嵌入式工程师)$", "job_title_std", 8, 3, "高频嵌入式岗位精修"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "硬件|电气|自动化|PLC|单片机|电路|仪器仪表|电子工程师|电控|嵌入式硬件|电机|电源", "job_title_std", 5, 4, "硬件、电气与自动化"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "硬件|电气|自动化|PLC|单片机|电路|仪器仪表|电子工程师|电控|嵌入式硬件|电机|电源", "keyword_seed", 4, 4, "硬件、电气与自动化"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "^(电力工程师 or 电力技术员|集成电路IC设计工程师 or 集成电路IC应用工程师|电子工程师 or 电子技术员|通信技术工程师|PCB工程师|激光技术 or 光电子技术|射频工程师|FPGA工程师)$", "keyword_seed", 6, 4, "高频硬件/电气 keyword_seed 精修"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "^(射频工程师|FPGA工程师|电子技术员)$", "job_title_std", 8, 4, "高频硬件岗位精修"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "^(机电工程师|自动控制工程师 or 自动控制技术员|自动化运维工程师)$", "keyword_seed", 6, 4, "高频自动化与机电 keyword_seed 精修"),
    RuleRecord("hardware_electrical", "硬件/电气", "job_family", "^(电机工程师|自控工程师)$", "job_title_std", 8, 4, "高频机电与自控岗位精修"),
    RuleRecord("r_and_d_research", "研发/科研", "job_family", "研发工程师|研发经理|研发总监|研发助理|研究员|科研人员|实验室研究员|技术研发工程师|技术研发经理|技术研发主管|医药技术研发人员", "job_title_std", 6, 5, "研发与科研岗位"),
    RuleRecord("r_and_d_research", "研发/科研", "job_family", "研发工程师|研发经理|研发总监|研发助理|研究员|科研人员|实验室研究员|技术研发工程师|技术研发经理|技术研发主管|医药技术研发人员", "keyword_seed", 5, 5, "研发与科研岗位"),
    RuleRecord("r_and_d_research", "研发/科研", "job_family", "^(技术研发工程师|食品研发 or 饮料研发)$", "keyword_seed", 6, 5, "高频研发 keyword_seed 精修"),
    RuleRecord("r_and_d_research", "研发/科研", "job_family", "^(产品开发工程师)$", "job_title_std", 12, 5, "高频产品研发岗位精修"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "机械|结构工程师|模具|机电|设备工程师|工业工程|材料工程师|热设计|制造工程师|工艺工程师|光学工程师|设备主管|设备技术员|设备员|机修工", "job_title_std", 5, 5, "机械、结构与制造工程"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "机械|结构工程师|结构设计工程师|模具|机电|设备工程师|工业工程|材料工程师|热设计|制造工程师|工艺工程师|工艺技术员|工艺员|精益工程师|PIE工程师|光学工程师|设备主管|设备技术员|设备员|机修工|焊接工程师|液压工程师|线束工程师", "keyword_seed", 4, 5, "机械、结构与制造工程"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "^(IE工程师|PE工程师|NPI工程师|PIE工程师|光学工程师|结构设计工程师|工艺技术员|工艺员|精益工程师|设备主管|设备技术员|机修工|焊接工程师|液压工程师|线束工程师)$", "job_title_std", 8, 5, "高频制造工程岗位精修"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "^(工艺工程师 or 制程工程师|工程工程师 or 设备工程师|机械工程师|汽车设计工程师|工程经理 or 设备经理|半导体工艺工程师|工程绘图员 or 机械绘图员|ME工程师|夹具工程师 or 夹具技师|机械设计)$", "keyword_seed", 6, 5, "高频机械 keyword_seed 精修"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "^(设备经理|设备管理员|设备维护工程师|ME工程师|钣金工程师|工艺/制程工程师)$", "job_title_std", 8, 5, "高频机械岗位精修"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "^(产品规划工程师|内外饰工程师)$", "keyword_seed", 6, 5, "高频机械设计 keyword_seed 精修"),
    RuleRecord("mechanical_engineering", "机械工程", "job_family", "^(机械产品工程师|制冷工程师|汽车内外饰设计工程师|机械装配技术员|结构助理工程师)$", "job_title_std", 12, 5, "高频机械标题精修"),
    RuleRecord("production_quality", "生产/质量", "job_family", "生产|车间|品质|质量|QA|QC|质检|检验|测试工程师|认证|体系工程师|生产主管|生产经理|安全工程师|EHS|安环", "job_title_std", 5, 6, "生产管理与质量"),
    RuleRecord("production_quality", "生产/质量", "job_family", "生产|车间|品质|质量|QA|QC|质检|检验|测试工程师|认证|体系工程师|生产主管|生产经理|安全工程师|EHS|安环|供应商质量|供应商管理|品检员|巡检员|三坐标测量员", "keyword_seed", 4, 6, "生产管理与质量"),
    RuleRecord("production_quality", "生产/质量", "job_family", "^(QE工程师|SQE工程师|DQE工程师|PQE工程师|CQE工程师|QE|SQE|DQE|PQE|CQE|安全工程师|品检员|巡检员|三坐标测量员)$", "job_title_std", 8, 6, "高频质量岗位精修"),
    RuleRecord("production_quality", "生产/质量", "job_family", "^(质检员 or 测试员 or QC|质量管理工程师 or 测试工程师 or QA工程师 or QC工程师|生产领班 or 生产组长|标准化工程师|环保工程师|药品生产 or 质量管理|供应商管理)$", "keyword_seed", 6, 6, "高频生产/质量 keyword_seed 精修"),
    RuleRecord("production_quality", "生产/质量", "job_family", "^(环保工程师|体系专员|品管员|检测工程师)$", "job_title_std", 12, 6, "高频生产/质量岗位精修"),
    RuleRecord("production_quality", "生产/质量", "job_family", "^(系统测试|功能测试|性能测试|质量管理主管 or 测试主管 or QA主管 or QC主管|质量管理经理 or 测试经理 or QA经理 or QC经理|仪器分析师 or 仪表分析师 or 计量分析师|SMT工程师|化学分析测试员)$", "keyword_seed", 6, 6, "高频测试/质量 keyword_seed 精修"),
    RuleRecord("production_quality", "生产/质量", "job_family", "^(测试技术员|SMT工程师|SMT技术员|品控主管|品控经理|计量工程师|化验员|TE工程师|失效分析工程师)$", "job_title_std", 12, 6, "高频测试与质量标题精修"),
    RuleRecord("blue_collar_ops", "技工/操作", "job_family", "普工|操作工|装配工|包装工|焊工|钳工|车工|铣工|磨工|数控|叉车|电工|维修工|技工|安装工|调机|注塑|冲压|喷涂|装卸|机修|维修技师", "job_title_std", 6, 7, "蓝领技术与操作"),
    RuleRecord("blue_collar_ops", "技工/操作", "job_family", "普工|操作工|装配工|包装工|焊工|钳工|车工|铣工|磨工|数控|叉车|电工|维修工|技工|安装工|调机|注塑|冲压|喷涂|装卸|机修|维修技师|折弯工", "keyword_seed", 5, 7, "蓝领技术与操作"),
    RuleRecord("blue_collar_ops", "技工/操作", "job_family", "^(普工 or 操作工 or 组装工 or 包装工)$", "keyword_seed", 6, 7, "高频操作工 keyword_seed 精修"),
    RuleRecord("blue_collar_ops", "技工/操作", "job_family", "^(生产操作员|生产工人|生产作业员|火花机师傅)$", "job_title_std", 8, 7, "高频操作岗位精修"),
    RuleRecord("blue_collar_ops", "技工/操作", "job_family", "^(组装工|锅炉工|油漆工|喷漆工|铆工|钣金工|缝纫工|样衣工|搬运工)$", "job_title_std", 12, 7, "高频蓝领岗位精修"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "采购|供应链|仓库|仓管|仓储|物流|物料|物控|PMC|计划员|海运操作|跟单|报关|船务|配送|司机|外贸", "job_title_std", 5, 8, "供应链、仓储与物流"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "采购|供应链|仓库|仓管|仓储|物流|物料|物控|PMC|计划员|海运操作|跟单|报关|船务|配送|司机|外贸", "keyword_seed", 4, 8, "供应链、仓储与物流"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "^(船务操作 or 空运陆运操作|快递员|分拣员|仓库管理员|生产计划管理 or 物料管理\\(PMC\\)|单证员|关务专员|货代操作|供应链专员)$", "keyword_seed", 6, 8, "高频供应链 keyword_seed 精修"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "^(单证员|库管员|驾驶员)$", "job_title_std", 8, 8, "高频供应链岗位精修"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "^(报关与报检|海关事务管理)$", "keyword_seed", 6, 8, "高频关务 keyword_seed 精修"),
    RuleRecord("supply_chain_logistics", "供应链/物流/采购", "job_family", "^(关务专员|关务|进出口专员|国际贸易专员|国际贸易经理|外贸经理|贸易助理)$", "job_title_std", 8, 8, "高频关务与外贸标题精修"),
    RuleRecord("sales_business", "销售/商务", "job_family", "销售|客户经理|客户代表|商务拓展|BD|业务员|业务经理|商务专员|商务经理|招商主管|招商|渠道经理|渠道销售|置业顾问|大客户|医药销售|店员|营业员|门店经理|店长|招投标|投标|保险康养顾问", "job_title_std", 6, 9, "销售与商务拓展"),
    RuleRecord("sales_business", "销售/商务", "job_family", "销售|客户经理|客户代表|商务拓展|BD|业务员|业务经理|商务专员|商务经理|招商主管|招商|渠道经理|渠道销售|置业顾问|大客户|医药销售|店员|营业员|门店经理|店长|招投标|投标|保险康养顾问", "keyword_seed", 5, 9, "销售与商务拓展"),
    RuleRecord("sales_business", "销售/商务", "job_family", "^(商务专员|商务经理|业务经理|招投标专员|投标专员)$", "job_title_std", 8, 9, "高频商务岗位精修"),
    RuleRecord("sales_business", "销售/商务", "job_family", "^(商务助理|业务拓展经理 or 业务拓展主管|促销员 or 导购员|渠道专员 or 分销专员|保险经纪人 or 保险代理|销售代表 or 客户代表|医药销售人员|理财顾问)$", "keyword_seed", 6, 9, "高频销售/商务 keyword_seed 精修"),
    RuleRecord("sales_business", "销售/商务", "job_family", "^(商务助理|商务主管|医学信息沟通专员)$", "job_title_std", 8, 9, "高频商务岗位精修"),
    RuleRecord("sales_business", "销售/商务", "job_family", "^(销售经理|电话销售|销售总监)$", "keyword_seed", 6, 9, "高频销售 keyword_seed 精修"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "运营|新媒体|市场|营销|推广|投放|社群|活动策划|品牌|用户增长|电商运营|直播运营|SEO|SEM|主播|直播主播|主持人", "job_title_std", 5, 10, "市场与运营"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "运营|新媒体|市场|营销|推广|投放|社群|活动策划|品牌|用户增长|电商运营|直播运营|SEO|SEM|主播|直播主播|主持人", "keyword_seed", 4, 10, "市场与运营"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "^(市场专员 or 营销专员 or 拓展专员|主播工作 or 主持人)$", "keyword_seed", 6, 10, "高频市场/运营 keyword_seed 精修"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "^(营销总监)$", "job_title_std", 8, 10, "高频营销岗位精修"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "^(电商专员|国内电商运营|跨境电商运营|营运经理|营运主管|营运助理|营运专员|内容运营|市场经理 or 营销经理 or 拓展经理)$", "keyword_seed", 6, 10, "高频运营 keyword_seed 精修"),
    RuleRecord("marketing_operations", "市场/运营", "job_family", "^(电商专员|营运经理|营运主管|营运助理|营运专员|社区经理|企划经理|企划主管|企划专员)$", "job_title_std", 12, 10, "高频运营标题精修"),
    RuleRecord("product_project", "产品/项目", "job_family", "产品经理|产品专员|产品主管|产品工程师|项目经理|项目工程师|项目助理|项目管理工程师|项目主管|项目管理|需求分析|实施顾问|解决方案|售前工程师", "job_title_std", 5, 11, "产品与项目岗位"),
    RuleRecord("product_project", "产品/项目", "job_family", "产品经理|产品专员|产品主管|产品工程师|项目经理|项目工程师|项目助理|项目管理工程师|项目主管|项目管理|需求分析|实施顾问|解决方案|售前工程师", "keyword_seed", 4, 11, "产品与项目岗位"),
    RuleRecord("product_project", "产品/项目", "job_family", "^(建筑工程管理 or 项目经理|项目经理|产品经理 or 产品主管|项目执行人员 or 项目协调人员|项目助理)$", "keyword_seed", 6, 11, "高频产品/项目 keyword_seed 精修"),
    RuleRecord("product_project", "产品/项目", "job_family", "^(项目专员|PM)$", "job_title_std", 12, 11, "高频项目岗位精修"),
    RuleRecord("product_project", "产品/项目", "job_family", "^(项目管理|产品助理|项目总监)$", "keyword_seed", 6, 11, "高频项目 keyword_seed 精修"),
    RuleRecord("product_project", "产品/项目", "job_family", "^(产品助理|项目总监)$", "job_title_std", 8, 11, "高频产品与项目标题精修"),
    RuleRecord("finance_accounting", "财务/会计", "job_family", "会计|财务|审计|税务|出纳|成本|预算|核算|投融资|证券|基金|结算", "job_title_std", 5, 12, "财务与会计"),
    RuleRecord("finance_accounting", "财务/会计", "job_family", "会计|财务|审计|税务|出纳|成本|预算|核算|投融资|证券|基金|结算", "keyword_seed", 4, 12, "财务与会计"),
    RuleRecord("finance_accounting", "财务/会计", "job_family", "^(投资经理)$", "job_title_std", 8, 12, "高频投融资岗位精修"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "人力资源|人事|HR|招聘|薪酬|绩效|培训|行政经理|行政主管|行政助理|行政专员|行政文员|人事助理|人事专员|人事经理|招聘助理|招聘专员|招聘经理|薪酬专员|绩效专员|培训专员|秘书|前台|总务|档案|办公室主任", "job_title_std", 5, 13, "人力资源与行政"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "人力资源|人事|HR|招聘|薪酬|绩效|培训|行政经理|行政主管|行政助理|行政专员|行政文员|人事助理|人事专员|人事经理|招聘助理|招聘专员|招聘经理|薪酬专员|绩效专员|培训专员|秘书|前台|总务|档案|办公室主任", "keyword_seed", 4, 13, "人力资源与行政"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "^(行政专员 or 行政助理|人事专员|经理助理 or 秘书)$", "keyword_seed", 6, 13, "高频人力/行政 keyword_seed 精修"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "^(办公室文员|办公室助理|经理助理)$", "job_title_std", 8, 13, "高频行政岗位精修"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "^(行政经理 or 行政主管 or 办公室主任|猎头 or 人才中介)$", "keyword_seed", 6, 13, "高频行政与招聘 keyword_seed 精修"),
    RuleRecord("hr_admin", "人力/行政", "job_family", "^(文员|内勤文员|工程文员|助理文员|猎头顾问)$", "job_title_std", 8, 13, "高频文职与招聘标题精修"),
    RuleRecord("customer_service", "客服/支持", "job_family", "客服|售后|售前支持|技术支持|呼叫中心|热线|客诉|服务顾问|服务工程师", "job_title_std", 5, 14, "客户服务与支持"),
    RuleRecord("customer_service", "客服/支持", "job_family", "客服|售后|售前支持|技术支持|呼叫中心|热线|客诉|服务顾问|服务工程师", "keyword_seed", 4, 14, "客户服务与支持"),
    RuleRecord("customer_service", "客服/支持", "job_family", "^(客服专员 or 客服助理|网络客服 or 在线客服|技术支持经理 or 维护经理|售后技术支持工程师)$", "keyword_seed", 6, 14, "高频客服 keyword_seed 精修"),
    RuleRecord("customer_service", "客服/支持", "job_family", "^(售后服务工程师)$", "job_title_std", 8, 14, "高频售后岗位精修"),
    RuleRecord("customer_service", "客服/支持", "job_family", "^(FAE 现场应用工程师|售前技术支持工程师)$", "keyword_seed", 6, 14, "高频售前与 FAE keyword_seed 精修"),
    RuleRecord("customer_service", "客服/支持", "job_family", "^(FAE工程师|FAE现场应用工程师|FAE 现场应用工程师|fae现场应用工程师)$", "job_title_std", 8, 14, "高频 FAE 标题精修"),
    RuleRecord("customer_service", "客服/支持", "job_family", "^(售后工程师)$", "job_title_std", 8, 14, "高频售后标题精修"),
    RuleRecord("design_content", "设计/内容", "job_family", "设计师|平面设计|UI|UX|视觉|美工|剪辑|视频|文案|编辑|动画|3D设计|工业设计|包装设计|室内设计|摄影|产品设计工程师", "job_title_std", 5, 15, "设计与内容生产"),
    RuleRecord("design_content", "设计/内容", "job_family", "设计师|平面设计|UI|UX|视觉|美工|剪辑|视频|文案|编辑|动画|3D设计|工业设计|包装设计|室内设计|摄影|产品设计工程师", "keyword_seed", 4, 15, "设计与内容生产"),
    RuleRecord("design_content", "设计/内容", "job_family", "^(工业设计 or 产品设计)$", "keyword_seed", 6, 15, "高频设计 keyword_seed 精修"),
    RuleRecord("medical_bio", "医疗/生物", "job_family", "护士|医生|医师|药师|药物|制药|生物|临床|医学|检验师|医疗器械|康复|护理", "job_title_std", 5, 16, "医疗健康与生物"),
    RuleRecord("medical_bio", "医疗/生物", "job_family", "护士|医生|医师|药师|药物|制药|生物|临床|医学|检验师|医疗器械|康复|护理", "keyword_seed", 4, 16, "医疗健康与生物"),
    RuleRecord("medical_bio", "医疗/生物", "job_family", "^(健康管理师)$", "job_title_std", 8, 16, "高频健康管理岗位精修"),
    RuleRecord("education_training", "教育/培训", "job_family", "教师|老师|教研|辅导员|班主任|培训师|讲师|课程顾问|教学|教育咨询", "job_title_std", 5, 17, "教育与培训"),
    RuleRecord("education_training", "教育/培训", "job_family", "教师|老师|教研|辅导员|班主任|培训师|讲师|课程顾问|教学|教育咨询", "keyword_seed", 4, 17, "教育与培训"),
    RuleRecord("construction_real_estate", "建筑/地产", "job_family", "建筑|土木|工程造价|造价工程师|施工|监理|房地产|物业|暖通|给排水|测绘|测量员|安全员|资料员", "job_title_std", 5, 18, "建筑工程与地产"),
    RuleRecord("construction_real_estate", "建筑/地产", "job_family", "建筑|土木|工程造价|造价工程师|施工|监理|房地产|物业|暖通|给排水|测绘|测量员|安全员|资料员", "keyword_seed", 4, 18, "建筑工程与地产"),
    RuleRecord("construction_real_estate", "建筑/地产", "job_family", "^(安全员|工程造价师 or 预结算经理 or 工程造价 or 预结算|厂务)$", "keyword_seed", 6, 18, "高频建筑/地产 keyword_seed 精修"),
    RuleRecord("construction_real_estate", "建筑/地产", "job_family", "^(土建工程师|测量工程师|厂务工程师)$", "job_title_std", 12, 18, "高频建筑工程岗位精修"),
    RuleRecord("construction_real_estate", "建筑/地产", "job_family", "^(建筑工程师)$", "keyword_seed", 6, 18, "高频建筑工程 keyword_seed 精修"),
    RuleRecord("legal_compliance", "法务/合规", "job_family", "法务|合规|律师|合同|知识产权|审查|风控|内控", "job_title_std", 5, 19, "法务与合规"),
    RuleRecord("legal_compliance", "法务/合规", "job_family", "法务|合规|律师|合同|知识产权|审查|风控|内控", "keyword_seed", 4, 19, "法务与合规"),
    RuleRecord("legal_compliance", "法务/合规", "job_family", "^(知识产权 or 专利 or 商标)$", "keyword_seed", 6, 19, "高频法务/合规 keyword_seed 精修"),
    RuleRecord("legal_compliance", "法务/合规", "job_family", "^(专利工程师|项目申报专员)$", "job_title_std", 8, 19, "高频合规岗位精修"),
    RuleRecord("general_management", "综合管理", "job_family", "管培生|储备干部|总经理助理|董事长助理|经营管理|管理培训生", "job_title_std", 4, 20, "综合管理与管培"),
    RuleRecord("general_management", "综合管理", "job_family", "管培生|储备干部|总经理助理|董事长助理|经营管理|管理培训生", "keyword_seed", 3, 20, "综合管理与管培"),
    RuleRecord("general_management", "综合管理", "job_family", "^(董事长助理|总经理助理|储备干部|管培生|管理培训生)$", "job_title_std", 8, 20, "高频综合管理岗位精修"),
    RuleRecord("general_management", "综合管理", "job_family", "^(首席执行官 or CEO or 总裁|工厂经理 or 厂长|总裁助理 or 总经理助理|副总经理 or 副总裁|营业部总经理 or 营业部副总经理|办事处经理 or 分公司经理 or 分支机构经理|首席运营官COO)$", "keyword_seed", 6, 20, "高频综合管理 keyword_seed 精修"),
    RuleRecord("general_management", "综合管理", "job_family", "^(副总经理|总经理|总裁助理|综合管理岗|综合管理)$", "job_title_std", 8, 20, "高频管理岗位精修"),
    RuleRecord("service_support", "后勤/安保/服务", "job_family", "保安|保安员|保洁|保洁员|厨师|后勤|物业服务|宿管|保姆|帮厨|服务员|餐饮服务员|乘务员|地勤人员|安检员|高铁工作人员|铁路工作人员|送餐员", "job_title_std", 7, 21, "后勤安保与生活服务"),
    RuleRecord("service_support", "后勤/安保/服务", "job_family", "保安|保安员|保洁|保洁员|厨师|后勤|物业服务|宿管|保姆|帮厨|服务员|餐饮服务员|乘务员|地勤人员|安检员|高铁工作人员|铁路工作人员|送餐员", "keyword_seed", 6, 21, "后勤安保与生活服务"),
    RuleRecord("service_support", "后勤/安保/服务", "job_family", "^(美容师|诚聘轨道巡检人员\\+五险一金|列车工作人员/包吃住/五险一金)$", "job_title_std", 8, 21, "高频服务岗位精修"),
    RuleRecord("service_support", "后勤/安保/服务", "job_family", "^(餐饮收银员|杂工|配菜员 or 打荷员)$", "keyword_seed", 6, 21, "高频生活服务 keyword_seed 精修"),
    RuleRecord("service_support", "后勤/安保/服务", "job_family", "^(收银员|厨工|消防监控员|消控员|咖啡师)$", "job_title_std", 8, 21, "高频服务标题精修"),
)

SKILL_RULES: tuple[TermRule, ...] = (
    TermRule("ChatGPT", "GenAI", ("chatgpt",), True),
    TermRule("GPT", "GenAI", ("gpt-4", "gpt4", "gpt"), True),
    TermRule("DeepSeek", "GenAI", ("deepseek",), True),
    TermRule("Prompt Engineering", "GenAI", ("prompt", "提示词"), True),
    TermRule("AIGC", "GenAI", ("aigc",), True),
    TermRule("LLM/大模型", "GenAI", ("大模型", "llm"), True),
    TermRule("LangChain", "GenAI", ("langchain",), True),
    TermRule("RAG", "GenAI", ("rag", "检索增强", "向量数据库"), True),
    TermRule("AI Agent", "GenAI", ("智能体", "agent"), True),
    TermRule("Copilot", "GenAI", ("copilot",), True),
    TermRule("Midjourney", "GenAI", ("midjourney",), True),
    TermRule("Stable Diffusion", "GenAI", ("stable diffusion", "sdxl"), True),
    TermRule("Python", "Programming", ("python",)),
    TermRule("Java", "Programming", ("java",)),
    TermRule("JavaScript", "Programming", ("javascript",)),
    TermRule("TypeScript", "Programming", ("typescript",)),
    TermRule("C++", "Programming", ("c++",)),
    TermRule("C#", "Programming", ("c#", "asp.net", ".net")),
    TermRule("Go", "Programming", ("golang", "go语言", "go开发")),
    TermRule("PHP", "Programming", ("php",)),
    TermRule("Rust", "Programming", ("rust",)),
    TermRule("SQL", "Data", ("sql", "mysql", "postgresql", "sqlserver")),
    TermRule("Oracle", "Data", ("oracle",)),
    TermRule("Excel", "Office/BI", ("excel",)),
    TermRule("Power BI", "Office/BI", ("power bi", "powerbi")),
    TermRule("Tableau", "Office/BI", ("tableau",)),
    TermRule("SPSS", "Office/BI", ("spss",)),
    TermRule("SAS", "Office/BI", ("sas",)),
    TermRule("R", "Office/BI", ("r语言",)),
    TermRule("Hadoop", "Data", ("hadoop",)),
    TermRule("Spark", "Data", ("spark",)),
    TermRule("Git", "DevOps", ("git",)),
    TermRule("Linux", "DevOps", ("linux",)),
    TermRule("Docker", "DevOps", ("docker",)),
    TermRule("Kubernetes", "DevOps", ("kubernetes", "k8s")),
    TermRule("Redis", "DevOps", ("redis",)),
    TermRule("Kafka", "DevOps", ("kafka",)),
    TermRule("React", "Programming", ("react",)),
    TermRule("Vue", "Programming", ("vue", "vue3")),
    TermRule("Spring Boot", "Programming", ("spring boot", "springboot")),
    TermRule("REST API", "Programming", ("restful", "api接口", "openapi")),
    TermRule("TensorFlow", "AI Framework", ("tensorflow",)),
    TermRule("PyTorch", "AI Framework", ("pytorch",)),
    TermRule("机器学习", "AI Framework", ("机器学习",)),
    TermRule("深度学习", "AI Framework", ("深度学习",)),
    TermRule("自然语言处理", "AI Framework", ("自然语言处理", "nlp")),
    TermRule("计算机视觉", "AI Framework", ("计算机视觉", "cv算法")),
    TermRule("MATLAB", "Engineering", ("matlab",)),
    TermRule("LabVIEW", "Engineering", ("labview",)),
    TermRule("CAD", "Engineering", ("autocad", "cad制图", "cad", "cad图")),
    TermRule("SolidWorks", "Engineering", ("solidworks",)),
    TermRule("UG/NX", "Engineering", ("ug", "nx", "unigraphics")),
    TermRule("CATIA", "Engineering", ("catia",)),
    TermRule("Eplan", "Engineering", ("eplan",)),
    TermRule("PLC", "Engineering", ("plc",)),
    TermRule("SAP", "ERP/CRM", ("sap",)),
    TermRule("ERP", "ERP/CRM", ("erp",)),
    TermRule("CRM", "ERP/CRM", ("crm",)),
    TermRule("Photoshop", "Design", ("photoshop", "ps设计", "photoshop软件")),
    TermRule("Illustrator", "Design", ("illustrator", "ai软件")),
    TermRule("Figma", "Design", ("figma",)),
    TermRule("Axure", "Design", ("axure",)),
    TermRule("Premiere", "Design", ("premiere", "pr剪辑")),
    TermRule("After Effects", "Design", ("after effects", "ae动画")),
    TermRule("SEO/SEM", "Marketing", ("seo", "sem")),
    TermRule("数据分析", "Business", ("数据分析", "报表分析", "经营分析")),
    TermRule("项目管理", "Business", ("项目管理", "pmp")),
    TermRule("供应链管理", "Business", ("供应链管理",)),
    TermRule("质量管理", "Business", ("质量管理", "qc工具", "qa工具")),
    TermRule("财务核算", "Business", ("财务核算", "账务处理", "总账")),
    TermRule("招聘配置", "Business", ("招聘配置", "人才招聘")),
)

TASK_RULES: tuple[TermRule, ...] = (
    TermRule("需求分析", "产品/分析", ("需求分析",)),
    TermRule("方案设计", "研发/设计", ("方案设计", "架构设计", "系统设计", "产品设计")),
    TermRule("软件开发", "研发/设计", ("软件开发", "系统开发", "功能开发", "代码开发", "二次开发")),
    TermRule("算法建模", "研发/设计", ("算法优化", "模型训练", "模型微调", "建模分析")),
    TermRule("数据分析", "分析/运营", ("数据分析", "报表分析", "经营分析", "数据洞察")),
    TermRule("测试验证", "研发/质控", ("测试验证", "功能测试", "系统测试", "调试优化")),
    TermRule("运维部署", "研发/运维", ("运维部署", "上线部署", "环境部署", "监控运维")),
    TermRule("项目管理", "管理/协同", ("项目管理", "项目推进", "项目协调", "跨部门协同")),
    TermRule("销售拓客", "销售/商务", ("销售拓展", "客户开发", "市场开拓", "商务拓展")),
    TermRule("客户维护", "销售/服务", ("客户维护", "售后支持", "客户服务", "客情维护")),
    TermRule("市场推广", "市场/运营", ("市场推广", "品牌推广", "活动策划", "广告投放")),
    TermRule("内容创作", "市场/内容", ("文案撰写", "内容创作", "视频剪辑", "脚本撰写")),
    TermRule("运营优化", "市场/运营", ("用户运营", "店铺运营", "社群运营", "运营优化")),
    TermRule("生产操作", "制造/生产", ("生产操作", "装配作业", "加工操作", "机台操作")),
    TermRule("质量检验", "制造/质量", ("质量检验", "品质管控", "来料检验", "过程检验")),
    TermRule("设备维护", "制造/设备", ("设备维护", "维修保养", "设备点检", "故障处理")),
    TermRule("采购跟单", "供应链/采购", ("采购执行", "询价比价", "供应商管理", "订单跟进")),
    TermRule("仓储物流", "供应链/物流", ("仓储管理", "发货配送", "物流调度", "库存管理")),
    TermRule("财务核算", "财务", ("财务核算", "账务处理", "报税申报", "审计支持")),
    TermRule("招聘培训", "人力资源", ("招聘面试", "培训组织", "员工关系", "绩效管理")),
    TermRule("教学授课", "教育", ("授课教学", "备课教研", "课程辅导", "教学管理")),
    TermRule("医疗护理", "医疗", ("临床诊疗", "护理服务", "医学检验", "用药指导")),
    TermRule("合规审查", "法务/风控", ("合同审核", "合规审查", "风险控制", "内控管理")),
)

MASTER_FIELDS = [
    "job_id",
    "source_platform",
    "source_job_id",
    "detail_url",
    "job_title_std",
    "job_family_code",
    "job_family_std",
    "job_family_confidence",
    "company_name_std",
    "company_type_raw",
    "company_industry_raw",
    "province_std_final",
    "city_std_final",
    "district_std_final",
    "city_std_confidence",
    "salary_avg_month",
    "education_std",
    "experience_std",
    "publish_time_std",
    "publish_date_std",
    "keyword_seed",
    "job_tags_raw",
    "jd_text_clean",
    "skill_list",
    "task_list",
    "genai_related_skill_list",
    "skill_category_list",
    "is_ai_native_job",
    "is_ai_augmented_job",
    "genai_exposure_level",
    "skill_hit_count",
    "task_hit_count",
]

UNION_RAW_FIELDS = [
    "job_id",
    "source_platform",
    "source_job_id",
    "detail_url",
    "city_seed",
    "city_raw",
    "province_std_source",
    "city_std_source",
    "district_std_source",
    "job_title_std",
    "company_name_std",
    "company_type_raw",
    "company_industry_raw",
    "salary_avg_month",
    "education_std",
    "experience_std",
    "publish_time_std",
    "keyword_seed",
    "job_tags_raw",
]

CITY_STD_FIELDS = [
    "job_id",
    "source_job_id",
    "job_title_std",
    "city_std_source",
    "province_std_source",
    "district_std_source",
    "city_std_final",
    "province_std_final",
    "district_std_final",
    "city_std_confidence",
    "city_std_reason",
]

JOB_FAMILY_STD_FIELDS = [
    "job_id",
    "source_job_id",
    "job_title_std",
    "keyword_seed",
    "company_industry_raw",
    "city_std_final",
    "province_std_final",
    "job_family_code",
    "job_family_std",
    "job_family_confidence",
    "job_family_score",
    "job_family_reason",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成 51job A 线静态版主表与配套说明")
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="51job social clean CSV")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="analysis_static 输出根目录",
    )
    parser.add_argument(
        "--docs-root",
        default=str(DEFAULT_DOCS_ROOT),
        help="A 线文档输出目录",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="只处理前 N 行，用于 smoke test",
    )
    parser.add_argument(
        "--skip-rows",
        type=int,
        default=0,
        help="跳过输入表前 N 行数据，用于续跑",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="以追加模式写入现有行级输出文件",
    )
    parser.add_argument(
        "--skip-summary",
        action="store_true",
        help="只生成/追加行级结果，不写汇总表和文档",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50000,
        help="每处理多少行打印一次进度",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260421,
        help="抽样随机种子",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_text(value: str | None) -> str:
    return clean_text(value or "")


def normalize_province_name(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if text in SPECIAL_PROVINCE_ALIASES:
        return SPECIAL_PROVINCE_ALIASES[text]
    text = text.replace("维吾尔自治区", "")
    text = text.replace("回族自治区", "")
    text = text.replace("壮族自治区", "")
    text = text.replace("自治区", "")
    text = text.replace("特别行政区", "")
    if text.endswith("省") or text.endswith("市"):
        text = text[:-1]
    return text


def normalize_city_name(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if text.endswith("市"):
        text = text[:-1]
    elif text.endswith("地区"):
        text = text[:-2]
    return text


def normalize_district_name(value: str | None) -> str:
    return normalize_text(value)


def location_aliases(name: str) -> set[str]:
    text = normalize_text(name)
    if not text:
        return set()
    aliases = {text}
    aliases.add(text.replace(" ", ""))
    province_short = normalize_province_name(text)
    city_short = normalize_city_name(text)
    if province_short:
        aliases.add(province_short)
    if city_short:
        aliases.add(city_short)
    if text.endswith("区") or text.endswith("县") or text.endswith("旗"):
        aliases.add(text[:-1])
    if text.endswith("州") or text.endswith("盟"):
        aliases.add(text[:-1])
    if text.endswith("新区"):
        aliases.add(text[:-2])
    return {item for item in aliases if item}


def load_admin_maps(path: Path, job51_area_tree_path: Path | None = None) -> dict[str, dict[str, str]]:
    province_code_to_name: dict[str, str] = {}
    prefecture_code_to_name: dict[str, str] = {}
    city_to_province: dict[str, str] = {}
    city_alias_to_city: dict[str, str] = {}
    district_alias_to_city: dict[str, str] = {}
    job51_area_alias_to_city: dict[str, str] = {}
    job51_prefix_to_province: dict[str, str] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        if row["level"] != "province":
            continue
        province_code_to_name[row["areaCode"][:2]] = normalize_province_name(row["name"])

    for row in rows:
        if row["level"] != "prefecture":
            continue
        area_code = row["areaCode"]
        province = province_code_to_name.get(area_code[:2], "")
        city = normalize_city_name(row["name"])
        prefecture_code_to_name[area_code] = city
        city_to_province[city] = province
        for alias in location_aliases(row["name"]):
            city_alias_to_city.setdefault(alias, city)

    for municipality in DIRECT_MUNICIPALITIES:
        city_to_province[municipality] = municipality
        city_alias_to_city.setdefault(municipality, municipality)
        city_alias_to_city.setdefault(f"{municipality}市", municipality)

    for row in rows:
        if row["level"] != "district":
            continue
        area_code = row["areaCode"]
        province = province_code_to_name.get(area_code[:2], "")
        pref_code = f"{area_code[:4]}00"
        city = prefecture_code_to_name.get(pref_code, "")
        if not city and province in DIRECT_MUNICIPALITIES:
            city = province
        if not city:
            continue
        for alias in location_aliases(row["name"]):
            district_alias_to_city.setdefault(alias, city)

    if job51_area_tree_path and job51_area_tree_path.exists():
        area_tree = json.loads(job51_area_tree_path.read_text(encoding="utf-8"))
        job51_items: list[tuple[str, str, set[str]]] = []
        for item in area_tree:
            name = normalize_city_name(item.get("name", ""))
            label = normalize_text(item.get("label", ""))
            canonical = name or normalize_city_name(label)
            area_id = normalize_text(item.get("area_id", ""))
            if not canonical:
                continue
            if area_id.endswith("0000") and canonical not in DIRECT_MUNICIPALITIES:
                continue
            aliases = location_aliases(item.get("name", "")) | location_aliases(label)
            prefix = area_id[:2]
            if canonical in city_to_province and prefix:
                job51_prefix_to_province.setdefault(prefix, city_to_province[canonical])
            job51_items.append((canonical, prefix, aliases))

        for canonical, prefix, aliases in job51_items:
            province = city_to_province.get(canonical, "")
            if not province and prefix:
                province = job51_prefix_to_province.get(prefix, "")
            if province and canonical not in city_to_province:
                city_to_province[canonical] = province
            for alias in aliases:
                job51_area_alias_to_city.setdefault(alias, canonical)

    return {
        "city_to_province": city_to_province,
        "city_alias_to_city": city_alias_to_city,
        "district_alias_to_city": district_alias_to_city,
        "job51_area_alias_to_city": job51_area_alias_to_city,
    }


def build_field_text_map(row: dict[str, str]) -> dict[str, str]:
    return {
        "job_title_std": normalize_text(row.get("job_title_std", "")).lower(),
        "keyword_seed": normalize_text(row.get("keyword_seed", "")).lower(),
        "job_tags_raw": normalize_text(row.get("job_tags_raw", "")).lower(),
        "jd_text_clean": normalize_text(row.get("jd_text_clean", "")).lower(),
    }


def canonicalize_city(
    row: dict[str, str],
    admin_maps: dict[str, dict[str, str]],
) -> dict[str, str]:
    city_alias_to_city = admin_maps["city_alias_to_city"]
    district_alias_to_city = admin_maps["district_alias_to_city"]
    city_to_province = admin_maps["city_to_province"]
    job51_area_alias_to_city = admin_maps.get("job51_area_alias_to_city", {})

    city_source = normalize_text(row.get("city_std", "")) or normalize_text(row.get("city_raw", "")) or normalize_text(row.get("city_seed", ""))
    province_source = normalize_text(row.get("province_std", ""))
    district_source = normalize_text(row.get("district_std", ""))

    city_final = ""
    province_final = ""
    district_final = ""
    reason = "unresolved"
    confidence = "low"

    if city_source and "-" in city_source:
        parts = [part for part in re.split(r"[-/·•]", city_source) if part]
        head = normalize_city_name(parts[0]) if parts else ""
        tail = normalize_district_name(parts[1]) if len(parts) > 1 else ""
        mapped_city = city_alias_to_city.get(head, head)
        if mapped_city:
            city_final = mapped_city
            district_final = tail or district_source
            province_final = city_to_province.get(city_final, "")
            confidence = "high" if province_final else "medium"
            reason = "city-hyphen-district"

    if not city_final and city_source in city_alias_to_city:
        city_final = city_alias_to_city[city_source]
        province_final = city_to_province.get(city_final, "")
        confidence = "high" if province_final else "medium"
        reason = "exact-city-alias"

    if not city_final and normalize_city_name(city_source) in city_alias_to_city:
        city_final = city_alias_to_city[normalize_city_name(city_source)]
        province_final = city_to_province.get(city_final, "")
        confidence = "high" if province_final else "medium"
        reason = "normalized-city-alias"

    city_short = normalize_city_name(city_source)
    if not city_final and city_short in district_alias_to_city:
        city_final = district_alias_to_city[city_short]
        district_final = city_source or district_source
        province_final = city_to_province.get(city_final, "")
        confidence = "medium" if province_final else "low"
        reason = "district-to-city"

    if not city_final and city_source in district_alias_to_city:
        city_final = district_alias_to_city[city_source]
        district_final = city_source or district_source
        province_final = city_to_province.get(city_final, "")
        confidence = "medium" if province_final else "low"
        reason = "exact-district-to-city"

    province_from_source = normalize_province_name(province_source)
    if not city_final and city_source in job51_area_alias_to_city:
        city_final = job51_area_alias_to_city[city_source]
        province_final = city_to_province.get(city_final, "") or province_from_source
        confidence = "high" if city_final in city_to_province and province_final else "medium"
        reason = "51job-area-alias"

    if not city_final and city_short in job51_area_alias_to_city:
        city_final = job51_area_alias_to_city[city_short]
        province_final = city_to_province.get(city_final, "") or province_from_source
        confidence = "high" if city_final in city_to_province and province_final else "medium"
        reason = "51job-area-alias-normalized"

    if city_final and not province_final:
        province_final = city_to_province.get(city_final, "") or province_from_source

    if not city_final:
        province_like_city = normalize_province_name(city_source)
        if city_source.endswith("省") or city_source.endswith("自治区") or city_source.endswith("特别行政区"):
            province_final = province_like_city
            city_final = ""
            district_final = district_source
            confidence = "low"
            reason = "province-only-city-source"

    if not city_final and city_short in DIRECT_MUNICIPALITIES:
        city_final = city_short
        province_final = city_short
        confidence = "high"
        reason = "municipality-city"

    if city_final and not province_final:
        province_final = province_from_source
        if province_final:
            confidence = "medium"
            reason = f"{reason}+fallback-province"

    if city_final and city_final in DIRECT_MUNICIPALITIES:
        province_final = city_final

    if not city_final and city_source and not city_source.endswith("省"):
        city_final = city_short
        province_final = province_from_source
        confidence = "low"
        reason = "fallback-city-source"

    if not district_final:
        district_final = district_source

    return {
        "city_std_source": city_source,
        "province_std_source": province_source,
        "district_std_source": district_source,
        "city_std_final": city_final,
        "province_std_final": province_final,
        "district_std_final": district_final,
        "city_std_confidence": confidence,
        "city_std_reason": reason,
    }


def score_job_family(row: dict[str, str]) -> dict[str, str]:
    texts = build_field_text_map(row)
    family_scores: defaultdict[str, int] = defaultdict(int)
    family_reasons: defaultdict[str, list[str]] = defaultdict(list)
    priorities: dict[str, int] = {}
    labels: dict[str, str] = {}

    for rule in JOB_FAMILY_RULES:
        haystack = texts.get(rule.source_field, "")
        if not haystack:
            continue
        if re.search(rule.pattern, haystack, flags=re.IGNORECASE):
            family_scores[rule.code] += rule.weight
            priorities[rule.code] = min(priorities.get(rule.code, 9999), rule.priority)
            labels[rule.code] = rule.label
            family_reasons[rule.code].append(f"{rule.source_field}:{rule.pattern}")

    if not family_scores:
        return {
            "job_family_code": "other",
            "job_family_std": "其他/待定",
            "job_family_confidence": "low",
            "job_family_score": "0",
            "job_family_reason": "no-rule-match",
        }

    ranked = sorted(
        family_scores.items(),
        key=lambda item: (-item[1], priorities.get(item[0], 9999), item[0]),
    )
    top_code, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else -1
    gap = top_score - second_score
    matched_fields = {
        reason.split(":", 1)[0]
        for reason in family_reasons[top_code]
        if ":" in reason
    }

    if top_code != "other" and top_score >= 8 and len(matched_fields) >= 2:
        confidence = "high"
    elif top_score >= 8 and gap >= 2:
        confidence = "high"
    elif top_score >= 5 and gap >= 1:
        confidence = "medium"
    elif top_code != "other" and top_score >= 5 and len(matched_fields) >= 2:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "job_family_code": top_code,
        "job_family_std": labels[top_code],
        "job_family_confidence": confidence,
        "job_family_score": str(top_score),
        "job_family_reason": " | ".join(sorted(set(family_reasons[top_code]))),
    }


def iter_rule_hits(text_map: dict[str, str], rules: Iterable[TermRule]) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for field, text in text_map.items():
        if not text:
            continue
        for rule in rules:
            for pattern in rule.patterns:
                pattern_lower = pattern.lower()
                if pattern_matches(text, pattern):
                    key = (rule.norm, field, pattern_lower)
                    if key in seen:
                        continue
                    seen.add(key)
                    hits.append(
                        {
                            "norm": rule.norm,
                            "category": rule.category,
                            "field": field,
                            "pattern": pattern,
                            "is_genai": "1" if rule.is_genai else "0",
                        }
                    )
    return hits


def sort_unique(items: Iterable[str]) -> list[str]:
    return sorted({item for item in items if item})


def contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


@functools.lru_cache(maxsize=None)
def ascii_pattern_regex(pattern: str) -> re.Pattern[str]:
    escaped = re.escape(pattern.lower())
    return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", flags=re.IGNORECASE)


def pattern_matches(text: str, pattern: str) -> bool:
    pattern_lower = pattern.lower()
    if contains_cjk(pattern_lower):
        return pattern_lower in text
    return bool(ascii_pattern_regex(pattern_lower).search(text))


def classify_ai_flags(
    job_family_code: str,
    genai_skills: list[str],
    skill_categories: list[str],
    title_text: str,
    jd_text: str,
) -> tuple[str, str, str]:
    combined = f"{title_text} {jd_text}".lower()
    ai_native_terms = (
        "人工智能",
        "算法",
        "机器学习",
        "深度学习",
        "自然语言处理",
        "aigc",
        "大模型",
        "prompt",
        "rag",
        "智能体",
        "llm",
        "langchain",
    )
    ai_native = job_family_code == "ai_algorithm" or any(term in combined for term in ai_native_terms)
    ai_augmented = (not ai_native) and bool(genai_skills)

    if ai_native or len(genai_skills) >= 2:
        exposure = "high"
    elif len(genai_skills) == 1:
        exposure = "medium"
    elif job_family_code in {
        "software_dev",
        "data_analytics",
        "design_content",
        "marketing_operations",
        "product_project",
        "finance_accounting",
        "hr_admin",
        "customer_service",
    } or any(category in {"GenAI", "Office/BI", "Data", "Programming", "Design"} for category in skill_categories):
        exposure = "low"
    else:
        exposure = "none"

    return (
        "1" if ai_native else "0",
        "1" if ai_augmented else "0",
        exposure,
    )


def join_pipe(values: Iterable[str]) -> str:
    items = sort_unique(values)
    return " | ".join(items)


def safe_int(value: str | None) -> int:
    try:
        return int(value or 0)
    except ValueError:
        return 0


def update_reservoir(
    reservoir_state: dict[str, object],
    key: str,
    limit: int,
    row_data: dict[str, str],
    rng: random.Random,
) -> None:
    seen_key = f"{key}_seen"
    rows_key = f"{key}_rows"
    seen = safe_int(str(reservoir_state.get(seen_key, 0))) + 1
    reservoir_state[seen_key] = seen
    rows = list(reservoir_state.setdefault(rows_key, []))
    if len(rows) < limit:
        rows.append(row_data)
    else:
        slot = rng.randint(1, seen)
        if slot <= limit:
            rows[slot - 1] = row_data
    reservoir_state[rows_key] = rows


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_outdated_count(doc_path: Path, needle: str) -> str:
    if not doc_path.exists():
        return "missing"
    text = doc_path.read_text(encoding="utf-8")
    pattern = re.escape(needle) + r".*?`(\d+)`"
    match = re.search(pattern, text, flags=re.DOTALL)
    if match:
        return match.group(1)
    return "not-found"


def build_schema_mapping(total_rows: int) -> dict[str, object]:
    return {
        "scope": "analysis_static.a_extraction.51job_only",
        "source_dataset": {
            "path": str(DEFAULT_INPUT).replace("\\", "/"),
            "platform_filter": ["51job_social"],
            "row_count": total_rows,
            "notes": [
                "当前 A 线仅使用 51job_social_jobs_clean_with_publish.csv。",
                "城市标准化先对齐到 city / province 静态口径，不再引入 NCSS。",
            ],
        },
        "union_raw_fields": {
            "job_id": "job_id",
            "source_platform": "platform",
            "source_job_id": "source_job_id",
            "detail_url": "detail_url",
            "city_seed": "city_seed",
            "city_raw": "city_raw",
            "province_std_source": "province_std",
            "city_std_source": "city_std",
            "district_std_source": "district_std",
            "job_title_std": "job_title_std",
            "company_name_std": "company_name_std",
            "company_type_raw": "company_type_raw",
            "company_industry_raw": "company_industry_raw",
            "salary_avg_month": "salary_avg_month",
            "education_std": "education_std",
            "experience_std": "experience_std",
            "publish_time_std": "publish_time_std",
            "keyword_seed": "keyword_seed",
            "job_tags_raw": "job_tags_raw",
        },
        "master_interface_fields": MASTER_FIELDS,
        "city_standardization": {
            "inputs": ["city_std", "city_raw", "city_seed", "province_std", "district_std"],
            "fallback_order": [
                "hyphen city-district split",
                "exact city alias",
                "district to city lookup",
                "province-only fallback",
                "raw source fallback",
            ],
        },
        "job_family_taxonomy": sorted(
            {rule.label for rule in JOB_FAMILY_RULES}
        ),
    }


def main() -> None:
    configure_utf8_stdio()
    args = parse_args()
    input_path = ROOT_DIR / args.input
    output_root = ROOT_DIR / args.output_root
    docs_root = ROOT_DIR / args.docs_root
    extraction_root = output_root / "a_extraction"
    interfaces_root = output_root / "interfaces"

    ensure_dir(extraction_root)
    ensure_dir(interfaces_root)
    ensure_dir(docs_root)

    rng = random.Random(args.seed)
    admin_maps = load_admin_maps(
        ROOT_DIR / AREAS_PATH,
        ROOT_DIR / JOB51_AREA_TREE_PATH,
    )

    union_raw_path = interfaces_root / "job_level_panel_union_raw.csv"
    city_std_path = extraction_root / "job_level_panel_city_std.csv"
    job_family_std_path = extraction_root / "job_level_panel_job_family_std.csv"
    master_path = interfaces_root / "job_level_master_static.csv"
    city_mapping_path = extraction_root / "city_mapping_table.csv"
    city_manual_review_path = extraction_root / "city_manual_review.csv"
    job_family_rules_path = extraction_root / "job_family_rules.csv"
    job_family_manual_review_path = extraction_root / "job_family_manual_review.csv"
    skill_rule_dict_path = extraction_root / "skill_rule_dictionary_v1.csv"
    task_rule_dict_path = extraction_root / "task_rule_dictionary_v1.csv"
    skill_extraction_raw_path = extraction_root / "skill_extraction_table_raw.csv"
    skill_extraction_final_path = extraction_root / "skill_extraction_table_final.csv"
    skill_token_norm_path = extraction_root / "skill_token_norm.csv"
    skill_category_path = extraction_root / "skill_category_dict.csv"
    task_category_path = extraction_root / "task_category_dict.csv"
    annotation_seed_path = extraction_root / "annotation_seed_500.csv"
    schema_mapping_path = interfaces_root / "schema_mapping_static.json"

    if not args.append:
        write_csv(
            job_family_rules_path,
            ["job_family_code", "job_family_std", "pattern", "source_field", "weight", "priority", "notes"],
            (
                {
                    "job_family_code": rule.code,
                    "job_family_std": rule.label,
                    "pattern": rule.pattern,
                    "source_field": rule.source_field,
                    "weight": rule.weight,
                    "priority": rule.priority,
                    "notes": rule.notes,
                }
                for rule in JOB_FAMILY_RULES
            ),
        )
        write_csv(
            skill_rule_dict_path,
            ["skill_norm", "skill_category", "is_genai", "match_value", "notes"],
            (
                {
                    "skill_norm": rule.norm,
                    "skill_category": rule.category,
                    "is_genai": "1" if rule.is_genai else "0",
                    "match_value": pattern,
                    "notes": rule.notes,
                }
                for rule in SKILL_RULES
                for pattern in rule.patterns
            ),
        )
        write_csv(
            task_rule_dict_path,
            ["task_norm", "task_category", "match_value", "notes"],
            (
                {
                    "task_norm": rule.norm,
                    "task_category": rule.category,
                    "match_value": pattern,
                    "notes": rule.notes,
                }
                for rule in TASK_RULES
                for pattern in rule.patterns
            ),
        )
        write_csv(
            skill_category_path,
            ["skill_category", "is_genai_category"],
            (
                {
                    "skill_category": category,
                    "is_genai_category": "1" if category == "GenAI" else "0",
                }
                for category in sorted({rule.category for rule in SKILL_RULES})
            ),
        )
        write_csv(
            task_category_path,
            ["task_category"],
            (
                {"task_category": category}
                for category in sorted({rule.category for rule in TASK_RULES})
            ),
        )

    city_mapping_stats: dict[tuple[str, ...], dict[str, object]] = {}
    job_family_review_stats: dict[tuple[str, ...], dict[str, object]] = {}
    skill_token_norm_counter: Counter[tuple[str, str, str, str]] = Counter()
    quality_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    exposure_counter: Counter[str] = Counter()
    reservoir_state: dict[str, object] = {}

    total_rows = 0

    with (
        input_path.open("r", encoding="utf-8-sig", newline="") as input_f,
        union_raw_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as union_f,
        city_std_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as city_f,
        job_family_std_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as family_f,
        master_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as master_f,
        skill_extraction_raw_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as skill_raw_f,
        skill_extraction_final_path.open("a" if args.append else "w", encoding="utf-8-sig", newline="") as skill_final_f,
    ):
        reader = csv.DictReader(input_f)
        union_writer = csv.DictWriter(union_f, fieldnames=UNION_RAW_FIELDS)
        city_writer = csv.DictWriter(city_f, fieldnames=CITY_STD_FIELDS)
        family_writer = csv.DictWriter(family_f, fieldnames=JOB_FAMILY_STD_FIELDS)
        master_writer = csv.DictWriter(master_f, fieldnames=MASTER_FIELDS)
        skill_raw_writer = csv.DictWriter(
            skill_raw_f,
            fieldnames=[
                "job_id",
                "source_job_id",
                "skill_norm",
                "skill_category",
                "match_field",
                "match_value",
                "is_genai_skill",
            ],
        )
        skill_final_writer = csv.DictWriter(
            skill_final_f,
            fieldnames=[
                "job_id",
                "source_job_id",
                "skill_norm",
                "skill_category",
                "is_genai_skill",
            ],
        )
        if not args.append:
            union_writer.writeheader()
            city_writer.writeheader()
            family_writer.writeheader()
            master_writer.writeheader()
            skill_raw_writer.writeheader()
            skill_final_writer.writeheader()

        source_row_index = 0
        for row in reader:
            source_row_index += 1
            if source_row_index <= args.skip_rows:
                continue
            if args.limit and total_rows >= args.limit:
                break
            total_rows += 1
            if args.progress_every and total_rows % args.progress_every == 0:
                print(
                    f"processed_rows={total_rows} source_row_index={source_row_index}",
                    flush=True,
                )

            publish_time_std = normalize_text(row.get("publish_time_std", ""))
            publish_date_std = publish_time_std[:10] if publish_time_std else ""

            union_row = {
                "job_id": normalize_text(row.get("job_id", "")),
                "source_platform": normalize_text(row.get("platform", "")),
                "source_job_id": normalize_text(row.get("source_job_id", "")),
                "detail_url": normalize_text(row.get("detail_url", "")),
                "city_seed": normalize_text(row.get("city_seed", "")),
                "city_raw": normalize_text(row.get("city_raw", "")),
                "province_std_source": normalize_text(row.get("province_std", "")),
                "city_std_source": normalize_text(row.get("city_std", "")),
                "district_std_source": normalize_text(row.get("district_std", "")),
                "job_title_std": normalize_text(row.get("job_title_std", "")),
                "company_name_std": normalize_text(row.get("company_name_std", "")),
                "company_type_raw": normalize_text(row.get("company_type_raw", "")),
                "company_industry_raw": normalize_text(row.get("company_industry_raw", "")),
                "salary_avg_month": normalize_text(row.get("salary_avg_month", "")),
                "education_std": normalize_text(row.get("education_std", "")),
                "experience_std": normalize_text(row.get("experience_std", "")),
                "publish_time_std": publish_time_std,
                "keyword_seed": normalize_text(row.get("keyword_seed", "")),
                "job_tags_raw": normalize_text(row.get("job_tags_raw", "")),
            }
            union_writer.writerow(union_row)

            city_info = canonicalize_city(row, admin_maps)
            city_row = {
                "job_id": union_row["job_id"],
                "source_job_id": union_row["source_job_id"],
                "job_title_std": union_row["job_title_std"],
                **city_info,
            }
            city_writer.writerow(city_row)
            quality_counter[f"city_conf_{city_info['city_std_confidence']}"] += 1
            if city_info["city_std_final"]:
                quality_counter["city_mapped"] += 1
            if city_info["province_std_final"]:
                quality_counter["province_mapped"] += 1
            if not city_info["city_std_final"] or city_info["city_std_confidence"] == "low":
                quality_counter["city_manual_review_rows"] += 1

            city_key = (
                city_info["province_std_source"],
                city_info["city_std_source"],
                city_info["district_std_source"],
                city_info["province_std_final"],
                city_info["city_std_final"],
                city_info["district_std_final"],
                city_info["city_std_confidence"],
                city_info["city_std_reason"],
            )
            city_stat = city_mapping_stats.setdefault(
                city_key,
                {
                    "record_count": 0,
                    "example_job_id": union_row["job_id"],
                    "example_detail_url": union_row["detail_url"],
                },
            )
            city_stat["record_count"] = safe_int(str(city_stat["record_count"])) + 1

            family_info = score_job_family(row)
            family_row = {
                "job_id": union_row["job_id"],
                "source_job_id": union_row["source_job_id"],
                "job_title_std": union_row["job_title_std"],
                "keyword_seed": union_row["keyword_seed"],
                "company_industry_raw": union_row["company_industry_raw"],
                "city_std_final": city_info["city_std_final"],
                "province_std_final": city_info["province_std_final"],
                **family_info,
            }
            family_writer.writerow(family_row)
            family_counter[family_info["job_family_std"]] += 1
            quality_counter[f"family_conf_{family_info['job_family_confidence']}"] += 1
            if family_info["job_family_confidence"] == "low":
                quality_counter["job_family_manual_review_rows"] += 1

            family_review_key = (
                union_row["job_title_std"],
                union_row["keyword_seed"],
                family_info["job_family_code"],
                family_info["job_family_std"],
                family_info["job_family_confidence"],
                family_info["job_family_reason"],
            )
            review_stat = job_family_review_stats.setdefault(
                family_review_key,
                {
                    "record_count": 0,
                    "example_source_job_id": union_row["source_job_id"],
                    "example_detail_url": union_row["detail_url"],
                },
            )
            review_stat["record_count"] = safe_int(str(review_stat["record_count"])) + 1

            text_map = build_field_text_map(row)
            skill_hits = iter_rule_hits(text_map, SKILL_RULES)
            task_hits = iter_rule_hits(text_map, TASK_RULES)

            skill_norms = sort_unique(hit["norm"] for hit in skill_hits)
            skill_categories = sort_unique(hit["category"] for hit in skill_hits)
            genai_skills = sort_unique(hit["norm"] for hit in skill_hits if hit["is_genai"] == "1")
            task_norms = sort_unique(hit["norm"] for hit in task_hits)

            if skill_norms:
                quality_counter["jobs_with_skill_hit"] += 1
            if task_norms:
                quality_counter["jobs_with_task_hit"] += 1
            if genai_skills:
                quality_counter["jobs_with_genai_skill_hit"] += 1

            for hit in skill_hits:
                skill_raw_writer.writerow(
                    {
                        "job_id": union_row["job_id"],
                        "source_job_id": union_row["source_job_id"],
                        "skill_norm": hit["norm"],
                        "skill_category": hit["category"],
                        "match_field": hit["field"],
                        "match_value": hit["pattern"],
                        "is_genai_skill": hit["is_genai"],
                    }
                )
                skill_token_norm_counter[
                    (
                        hit["pattern"],
                        hit["norm"],
                        hit["category"],
                        hit["is_genai"],
                    )
                ] += 1

            for skill_norm in skill_norms:
                matched = next(hit for hit in skill_hits if hit["norm"] == skill_norm)
                skill_final_writer.writerow(
                    {
                        "job_id": union_row["job_id"],
                        "source_job_id": union_row["source_job_id"],
                        "skill_norm": skill_norm,
                        "skill_category": matched["category"],
                        "is_genai_skill": matched["is_genai"],
                    }
                )

            is_ai_native_job, is_ai_augmented_job, genai_exposure_level = classify_ai_flags(
                family_info["job_family_code"],
                genai_skills,
                skill_categories,
                union_row["job_title_std"],
                normalize_text(row.get("jd_text_clean", "")),
            )
            exposure_counter[genai_exposure_level] += 1

            master_row = {
                "job_id": union_row["job_id"],
                "source_platform": union_row["source_platform"],
                "source_job_id": union_row["source_job_id"],
                "detail_url": union_row["detail_url"],
                "job_title_std": union_row["job_title_std"],
                "job_family_code": family_info["job_family_code"],
                "job_family_std": family_info["job_family_std"],
                "job_family_confidence": family_info["job_family_confidence"],
                "company_name_std": union_row["company_name_std"],
                "company_type_raw": union_row["company_type_raw"],
                "company_industry_raw": union_row["company_industry_raw"],
                "province_std_final": city_info["province_std_final"],
                "city_std_final": city_info["city_std_final"],
                "district_std_final": city_info["district_std_final"],
                "city_std_confidence": city_info["city_std_confidence"],
                "salary_avg_month": union_row["salary_avg_month"],
                "education_std": union_row["education_std"],
                "experience_std": union_row["experience_std"],
                "publish_time_std": publish_time_std,
                "publish_date_std": publish_date_std,
                "keyword_seed": union_row["keyword_seed"],
                "job_tags_raw": union_row["job_tags_raw"],
                "jd_text_clean": normalize_text(row.get("jd_text_clean", "")),
                "skill_list": join_pipe(skill_norms),
                "task_list": join_pipe(task_norms),
                "genai_related_skill_list": join_pipe(genai_skills),
                "skill_category_list": join_pipe(skill_categories),
                "is_ai_native_job": is_ai_native_job,
                "is_ai_augmented_job": is_ai_augmented_job,
                "genai_exposure_level": genai_exposure_level,
                "skill_hit_count": str(len(skill_norms)),
                "task_hit_count": str(len(task_norms)),
            }
            master_writer.writerow(master_row)

            annotation_row = {
                "job_id": master_row["job_id"],
                "source_job_id": master_row["source_job_id"],
                "job_title_std": master_row["job_title_std"],
                "city_std_final": master_row["city_std_final"],
                "province_std_final": master_row["province_std_final"],
                "job_family_std": master_row["job_family_std"],
                "job_family_confidence": master_row["job_family_confidence"],
                "city_std_confidence": master_row["city_std_confidence"],
                "skill_list": master_row["skill_list"],
                "task_list": master_row["task_list"],
                "genai_exposure_level": master_row["genai_exposure_level"],
                "jd_text_clean": master_row["jd_text_clean"],
                "review_job_family": "",
                "review_skill_list": "",
                "review_task_list": "",
                "review_notes": "",
            }

            if genai_exposure_level == "high":
                update_reservoir(reservoir_state, "high", 120, annotation_row, rng)
            elif genai_exposure_level == "medium":
                update_reservoir(reservoir_state, "medium", 120, annotation_row, rng)
            if family_info["job_family_confidence"] == "low":
                update_reservoir(reservoir_state, "family_low", 130, annotation_row, rng)
            if city_info["city_std_confidence"] == "low":
                update_reservoir(reservoir_state, "city_low", 80, annotation_row, rng)
            update_reservoir(reservoir_state, "general", 120, annotation_row, rng)

    if args.skip_summary:
        print(
            f"chunk complete: appended_rows={total_rows} skip_rows={args.skip_rows} append={args.append}",
            flush=True,
        )
        return

    city_mapping_rows = sorted(
        (
            {
                "province_std_source": key[0],
                "city_std_source": key[1],
                "district_std_source": key[2],
                "province_std_final": key[3],
                "city_std_final": key[4],
                "district_std_final": key[5],
                "city_std_confidence": key[6],
                "city_std_reason": key[7],
                "record_count": value["record_count"],
                "example_job_id": value["example_job_id"],
                "example_detail_url": value["example_detail_url"],
            }
            for key, value in city_mapping_stats.items()
        ),
        key=lambda row: (-safe_int(str(row["record_count"])), row["city_std_source"], row["province_std_source"]),
    )
    write_csv(
        city_mapping_path,
        [
            "province_std_source",
            "city_std_source",
            "district_std_source",
            "province_std_final",
            "city_std_final",
            "district_std_final",
            "city_std_confidence",
            "city_std_reason",
            "record_count",
            "example_job_id",
            "example_detail_url",
        ],
        city_mapping_rows,
    )
    write_csv(
        city_manual_review_path,
        [
            "province_std_source",
            "city_std_source",
            "district_std_source",
            "province_std_final",
            "city_std_final",
            "district_std_final",
            "city_std_confidence",
            "city_std_reason",
            "record_count",
            "example_job_id",
            "example_detail_url",
        ],
        (
            row
            for row in city_mapping_rows
            if row["city_std_confidence"] == "low" or not row["city_std_final"] or not row["province_std_final"]
        ),
    )

    family_review_rows = sorted(
        (
            {
                "job_title_std": key[0],
                "keyword_seed": key[1],
                "assigned_job_family_code": key[2],
                "assigned_job_family_std": key[3],
                "job_family_confidence": key[4],
                "job_family_reason": key[5],
                "record_count": value["record_count"],
                "example_source_job_id": value["example_source_job_id"],
                "example_detail_url": value["example_detail_url"],
            }
            for key, value in job_family_review_stats.items()
            if key[4] == "low"
        ),
        key=lambda row: (-safe_int(str(row["record_count"])), row["assigned_job_family_code"], row["job_title_std"]),
    )
    write_csv(
        job_family_manual_review_path,
        [
            "job_title_std",
            "keyword_seed",
            "assigned_job_family_code",
            "assigned_job_family_std",
            "job_family_confidence",
            "job_family_reason",
            "record_count",
            "example_source_job_id",
            "example_detail_url",
        ],
        family_review_rows,
    )

    write_csv(
        skill_token_norm_path,
        ["match_value", "skill_norm", "skill_category", "is_genai_skill", "hit_count"],
        (
            {
                "match_value": key[0],
                "skill_norm": key[1],
                "skill_category": key[2],
                "is_genai_skill": key[3],
                "hit_count": count,
            }
            for key, count in sorted(
                skill_token_norm_counter.items(),
                key=lambda item: (-item[1], item[0][1], item[0][0]),
            )
        ),
    )

    annotation_rows: list[dict[str, str]] = []
    for bucket in ("high_rows", "medium_rows", "family_low_rows", "city_low_rows", "general_rows"):
        for row in reservoir_state.get(bucket, []):
            if row not in annotation_rows:
                annotation_rows.append(row)
            if len(annotation_rows) >= 500:
                break
        if len(annotation_rows) >= 500:
            break
    write_csv(
        annotation_seed_path,
        [
            "job_id",
            "source_job_id",
            "job_title_std",
            "city_std_final",
            "province_std_final",
            "job_family_std",
            "job_family_confidence",
            "city_std_confidence",
            "skill_list",
            "task_list",
            "genai_exposure_level",
            "jd_text_clean",
            "review_job_family",
            "review_skill_list",
            "review_task_list",
            "review_notes",
        ],
        annotation_rows[:500],
    )

    schema_mapping = build_schema_mapping(total_rows)
    ensure_dir(schema_mapping_path.parent)
    schema_mapping_path.write_text(
        json.dumps(schema_mapping, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    root_readme = ROOT_DIR / "README.md"
    processed_readme = ROOT_DIR / "data/processed/README.md"
    job51_readme = ROOT_DIR / "data/processed/51job/README.md"
    docs_root_text = textwrap.dedent(
        f"""\
        # 00 分支基线核对（A 线 / 51job-only）

        ## 结论摘要

        - 当前 A 线主样本固定为 `data/processed/51job/51job_social_jobs_clean_with_publish.csv`。
        - 实际样本量为 `{total_rows}` 行，明显高于仓库 README 中遗留的 `8184` 条旧口径。
        - `data/processed/51job/README.md` 仍描述旧文件 `51job_social_jobs_clean.csv`，与当前主样本不一致。
        - 协作文档中提到的 `src/analysis/*`、`src/analysis/config/*` 等目录在当前工作区未找到，说明文档基线与实际仓库存在偏差。
        - 当前 51job 社招大表的 `job_title_std`、`company_name_std`、`city_std`、`publish_time_std` 和 `jd_text_clean` 覆盖率均可直接支撑 A 线主表构建。

        ## README/快照差异

        - 根目录 `README.md` 记载 `51job_social_jobs_clean_with_publish.csv` 为 `{read_outdated_count(root_readme, '51job_social_jobs_clean_with_publish.csv')}` 条。
        - `data/processed/README.md` 记载同一文件为 `{read_outdated_count(processed_readme, '51job_social_jobs_clean_with_publish.csv')}` 条。
        - `data/processed/51job/README.md` 仍引用旧文件 `51job_social_jobs_clean.csv`，未切换到 `_with_publish` 版本。

        ## 当前主样本建议

        - 主样本：`data/processed/51job/51job_social_jobs_clean_with_publish.csv`
        - 当前执行口径：`51job-only`、`city-job_family` 静态版主线
        - 当前不纳入：`NCSS`、其他平台、时间面板主线

        ## 字段可用性说明

        - 可直接进入主表的关键字段：`source_job_id`、`job_title_std`、`company_name_std`、`city_std`、`province_std`、`salary_avg_month`、`education_std`、`experience_std`、`publish_time_std`、`jd_text_clean`
        - 需要二次标准化的关键字段：`province_std`、`city_std`、`district_std`
        - 需要新增规则衍生的关键字段：`job_family_std`、`skill_list`、`task_list`、`genai_related_skill_list`、`skill_category_list`、`is_ai_native_job`、`is_ai_augmented_job`、`genai_exposure_level`

        ## 未找到的协作文档预设目录

        - `src/analysis/`
        - `src/analysis/config/analysis_plan.json`
        - `src/analysis/config/model_specs.json`
        - `src/analysis/config/variable_dictionary.csv`
        """
    )
    (docs_root / "00_branch_baseline_audit.md").write_text(docs_root_text, encoding="utf-8")

    city_doc = textwrap.dedent(
        f"""\
        # 02 城市标准化说明

        ## 方法

        - 当前城市标准化只处理 51job 社招主表，不混入 NCSS。
        - 主输入字段依次为：`city_std -> city_raw -> city_seed`，辅助字段为 `province_std`、`district_std`。
        - 行政区映射底座使用 `data/input/ncss/ncss_area_codes_all.csv` 构建 `city -> province` 与 `district -> city` lookup。
        - 规则优先级依次为：`城市-区县拆分`、`城市别名精确匹配`、`区县回推城市`、`仅省级信息保留`、`原值兜底`。

        ## 结果摘要

        - 总样本：`{total_rows}`
        - 已映射到城市：`{quality_counter['city_mapped']}`
        - 已映射到省份：`{quality_counter['province_mapped']}`
        - 高置信城市映射：`{quality_counter['city_conf_high']}`
        - 中置信城市映射：`{quality_counter['city_conf_medium']}`
        - 低置信或需人工复核：`{quality_counter['city_conf_low']}`

        ## 产物

        - `data/processed/analysis_static/a_extraction/city_mapping_table.csv`
        - `data/processed/analysis_static/a_extraction/city_manual_review.csv`
        - `data/processed/analysis_static/a_extraction/job_level_panel_city_std.csv`

        ## 复核建议

        - 优先复核 `city_manual_review.csv` 中的 `province-only-city-source` 和 `fallback-city-source`
        - 对于仅有省级信息的职位，当前保留 `province_std_final`，`city_std_final` 允许为空
        - 对于 `上海-黄浦区`、`深圳-福田区` 这类记录，当前已回推至主城区城市并保留 `district_std_final`
        """
    )
    (docs_root / "02_city_standardization.md").write_text(city_doc, encoding="utf-8")

    top_families_text = "\n".join(
        f"- {label}: {count}"
        for label, count in family_counter.most_common(12)
    )
    family_doc = textwrap.dedent(
        f"""\
        # 03 岗位族标准化说明

        ## 方法

        - 岗位族标准化仅基于 51job 的 `job_title_std`、`keyword_seed` 与 `job_tags_raw`。
        - 第一版共设置 `21` 个岗位族代码，其中常用主族包括：`AI/算法`、`软件开发`、`销售/商务`、`市场/运营`、`供应链/物流/采购`、`机械工程`、`生产/质量` 等。
        - 规则命中后按 `score -> priority` 排序，形成 `job_family_std` 与 `job_family_confidence`。

        ## 结果摘要

        - 总样本：`{total_rows}`
        - 高置信岗位族：`{quality_counter['family_conf_high']}`
        - 中置信岗位族：`{quality_counter['family_conf_medium']}`
        - 低置信岗位族：`{quality_counter['family_conf_low']}`

        ## 高频岗位族

        {top_families_text}

        ## 产物

        - `data/processed/analysis_static/a_extraction/job_family_rules.csv`
        - `data/processed/analysis_static/a_extraction/job_family_manual_review.csv`
        - `data/processed/analysis_static/a_extraction/job_level_panel_job_family_std.csv`
        """
    )
    (docs_root / "03_job_family_standardization.md").write_text(family_doc, encoding="utf-8")

    rule_doc = textwrap.dedent(
        f"""\
        # 04 规则词典与抽取说明

        ## 当前策略

        - 技能抽取采用规则词典第一版，不直接调用大模型。
        - 规则输入字段为：`job_title_std`、`keyword_seed`、`job_tags_raw`、`jd_text_clean`。
        - 词典覆盖 `GenAI`、`Programming`、`Data`、`DevOps`、`Engineering`、`Design`、`ERP/CRM`、`Office/BI` 等类别。
        - 任务抽取单独维护 `TASK_RULES`，当前用于生成 `task_list` 与后续人工标注种子。

        ## 结果摘要

        - 有技能命中的职位：`{quality_counter['jobs_with_skill_hit']}`
        - 有任务命中的职位：`{quality_counter['jobs_with_task_hit']}`
        - 有 GenAI 相关技能命中的职位：`{quality_counter['jobs_with_genai_skill_hit']}`
        - `genai_exposure_level=high`：`{exposure_counter['high']}`
        - `genai_exposure_level=medium`：`{exposure_counter['medium']}`
        - `genai_exposure_level=low`：`{exposure_counter['low']}`
        - `genai_exposure_level=none`：`{exposure_counter['none']}`

        ## 产物

        - `annotation_seed_500.csv`
        - `skill_rule_dictionary_v1.csv`
        - `task_rule_dictionary_v1.csv`
        - `skill_extraction_table_raw.csv`
        - `skill_extraction_table_final.csv`
        - `skill_token_norm.csv`
        - `skill_category_dict.csv`
        - `task_category_dict.csv`
        """
    )
    (docs_root / "04_rule_dictionary_notes.md").write_text(rule_doc, encoding="utf-8")

    quality_doc = textwrap.dedent(
        f"""\
        # 05 抽取质量报告

        ## 基础规模

        - 总职位数：`{total_rows}`
        - 输出主接口表：`data/processed/analysis_static/interfaces/job_level_master_static.csv`

        ## 城市标准化

        - 已映射城市占比：`{quality_counter['city_mapped'] / total_rows:.4%}`
        - 已映射省份占比：`{quality_counter['province_mapped'] / total_rows:.4%}`
        - 需人工复核城市行数：`{quality_counter['city_manual_review_rows']}`

        ## 岗位族标准化

        - 低置信岗位族行数：`{quality_counter['job_family_manual_review_rows']}`
        - 最常见岗位族：`{family_counter.most_common(1)[0][0] if family_counter else 'NA'}`

        ## 技能与任务抽取

        - 技能命中占比：`{quality_counter['jobs_with_skill_hit'] / total_rows:.4%}`
        - 任务命中占比：`{quality_counter['jobs_with_task_hit'] / total_rows:.4%}`
        - GenAI 命中占比：`{quality_counter['jobs_with_genai_skill_hit'] / total_rows:.4%}`

        ## 建议

        - 下一轮优先扩充 `GenAI`、`供应链/物流`、`销售/商务`、`财务/会计` 的软技能词典。
        - 对 `job_family_manual_review.csv` 中高频低置信标题优先做人工归类，再把规则回灌脚本。
        - 对 `city_manual_review.csv` 中仅省级定位样本，可按后续研究需要决定是否剔除或单独标记。
        """
    )
    (docs_root / "05_extraction_quality_report.md").write_text(quality_doc, encoding="utf-8")

    print(
        f"generated A-line 51job outputs: rows={total_rows}, "
        f"master={master_path}, city_map={city_mapping_path}, "
        f"family_review={job_family_manual_review_path}"
    )


if __name__ == "__main__":
    main()
