from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = Path(__file__).resolve().parent / "config"
OUTPUT_PATH = ROOT_DIR / "docs" / "生成式AI城市技能熵研究方案-代码化汇总.md"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def push(lines: list[str], *items: str) -> None:
    lines.extend(items)


def render_meta(lines: list[str], meta: dict) -> None:
    push(
        lines,
        f"# {meta['title']}",
        "",
        f"- 研究窗口：`{meta['window']}`",
        f"- 主分析单元：`{meta['primary_unit']}`",
        f"- 辅助分析单元：{', '.join(f'`{item}`' for item in meta['secondary_units'])}",
        f"- 文档版本：`{meta['document_version']}`",
        ""
    )


def render_questions(lines: list[str], questions: list[str]) -> None:
    push(lines, "## 研究问题", "")
    for idx, question in enumerate(questions, start=1):
        push(lines, f"{idx}. {question}")
    push(lines, "")


def render_mechanisms(lines: list[str], plan: dict) -> None:
    push(lines, "## 理论机制", "", "主链：", "")
    for item in plan["theory_chain"]:
        push(lines, f"- `{item}`")
    push(lines, "", "具体机制：", "")
    for mechanism in plan["mechanisms"]:
        push(lines, f"- `{mechanism['label']}`：{mechanism['description']}")
    push(lines, "")


def render_hypotheses(lines: list[str], hypotheses: list[dict]) -> None:
    push(lines, "## 研究假设", "")
    for hypothesis in hypotheses:
        push(
            lines,
            f"- `{hypothesis['id']}`：{hypothesis['statement']}",
            f"  理论依据：{hypothesis['basis']}",
            f"  检验方向：`{hypothesis['direction']}`"
        )
    push(lines, "")


def render_variables(lines: list[str], rows: list[dict[str, str]]) -> None:
    push(lines, "## 变量字典摘要", "")
    push(lines, "| 变量名 | 变量组 | 中文含义 | 层级 | 主线口径 | 备选口径 |")
    push(lines, "|---|---|---|---|---|---|")
    for row in rows:
        push(
            lines,
            f"| `{row['variable_name']}` | {row['group']} | {row['cn_label']} | {row['level']} | {row['primary_spec']} | {row['alternate_spec']} |"
        )
    push(lines, "")


def render_pipeline(lines: list[str], steps: list[dict]) -> None:
    push(lines, "## 数据处理流程", "")
    for step in steps:
        push(lines, f"### 步骤 {step['step']}：{step['label']}", "")
        push(lines, f"- 输入：{', '.join(f'`{item}`' for item in step['input'])}")
        push(lines, f"- 处理逻辑：{step['logic']}")
        output = step["output"]
        if isinstance(output, list):
            push(lines, f"- 输出：{', '.join(f'`{item}`' for item in output)}")
        else:
            push(lines, f"- 输出：`{output}`")
        push(lines, f"- 自动化程度：`{step['automation']}`")
        push(lines, f"- 人工校验强度：`{step['manual_review']}`")
        push(lines, "")


def render_models(lines: list[str], models: list[dict]) -> None:
    push(lines, "## 模型规格摘要", "")
    for model in models:
        push(lines, f"### {model['label']}", "")
        push(lines, f"- 模型层级：`{model['tier']}`")
        push(lines, f"- 公式：`{model['formula']}`")
        push(lines, f"- 目的：{model['purpose']}")
        push(lines, f"- 识别逻辑：{model['identification_logic']}")
        push(lines, f"- 被解释变量：{', '.join(f'`{item}`' for item in model.get('dependent_variables', []))}")
        if model.get("core_regressors"):
            push(lines, f"- 核心解释变量：{', '.join(f'`{item}`' for item in model['core_regressors'])}")
        if model.get("fixed_effects"):
            push(lines, f"- 固定效应：{', '.join(f'`{item}`' for item in model['fixed_effects'])}")
        push(lines, "- 优点：")
        for advantage in model.get("advantages", []):
            push(lines, f"  - {advantage}")
        push(lines, "- 局限：")
        for limitation in model.get("limitations", []):
            push(lines, f"  - {limitation}")
        push(lines, f"- 是否推荐：`{str(model.get('recommended', False)).lower()}`", "")


def render_spatiotemporal(lines: list[str], section: dict) -> None:
    push(lines, "## 时空分析", "", "主结果建议：", "")
    for item in section["main_results"]:
        push(lines, f"- {item}")
    push(lines, "", "增强结果建议：", "")
    for item in section["enhancement_results"]:
        push(lines, f"- {item}")
    push(lines, "")


def render_robustness(lines: list[str], items: list[str]) -> None:
    push(lines, "## 稳健性与识别增强", "")
    for item in items:
        push(lines, f"- {item}")
    push(lines, "")


def render_structure(lines: list[str], items: list[str]) -> None:
    push(lines, "## 论文结构建议", "")
    for idx, item in enumerate(items, start=1):
        push(lines, f"{idx}. {item}")
    push(lines, "")


def render_top3(lines: list[str], items: list[dict]) -> None:
    push(lines, "## 最推荐的 3 个主模型", "")
    for idx, item in enumerate(items, start=1):
        push(lines, f"{idx}. `{item['model_id']}`：{item['reason']}")
    push(lines, "")


def render_fallback(lines: list[str], fallback: dict) -> None:
    push(lines, "## 数据质量不及预期时的降级方案", "")
    push(lines, f"- 时间窗口降级：{', '.join(f'`{item}`' for item in fallback['time_window'])}")
    push(lines, f"- 城市范围降级：{', '.join(f'`{item}`' for item in fallback['city_scope'])}")
    push(lines, f"- 岗位族粒度降级：{', '.join(f'`{item}`' for item in fallback['job_family_granularity'])}")
    push(lines, f"- 必保模型：{', '.join(f'`{item}`' for item in fallback['must_keep_models'])}")
    push(lines, f"- 可删减模型：{', '.join(f'`{item}`' for item in fallback['optional_models_to_drop'])}")
    push(lines, "")


def build_markdown() -> str:
    analysis_plan = load_json(CONFIG_DIR / "analysis_plan.json")
    model_specs = load_json(CONFIG_DIR / "model_specs.json")
    variables = load_csv(CONFIG_DIR / "variable_dictionary.csv")

    lines: list[str] = []
    render_meta(lines, analysis_plan["study_meta"])
    render_questions(lines, analysis_plan["research_questions"])
    render_mechanisms(lines, analysis_plan)
    render_hypotheses(lines, analysis_plan["hypotheses"])
    render_variables(lines, variables)
    render_pipeline(lines, analysis_plan["pipeline_steps"])
    render_models(lines, model_specs["models"])
    render_spatiotemporal(lines, analysis_plan["spatiotemporal_analysis"])
    render_robustness(lines, analysis_plan["robustness"])
    render_structure(lines, analysis_plan["paper_structure"])
    render_top3(lines, model_specs["recommended_top3"])
    render_fallback(lines, analysis_plan["fallback_plan"])
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    markdown = build_markdown()
    OUTPUT_PATH.write_text(markdown, encoding="utf-8")
    print(f"Generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
