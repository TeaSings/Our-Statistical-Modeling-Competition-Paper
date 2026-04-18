from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from common import DEFAULT_HEADERS, clean_text, ensure_parent


SEARCH_PAGE_URL = "https://we.51job.com/pc/search?jobArea=000000"


def _prepare_session(session: requests.Session | None = None) -> requests.Session:
    session = session or requests.Session()
    session.trust_env = False
    session.headers.update(DEFAULT_HEADERS)
    return session


@dataclass
class AreaNode:
    area_id: str
    name: str
    label: str
    children: list["AreaNode"] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "area_id": self.area_id,
            "name": self.name,
            "label": self.label,
            "children": [child.to_dict() for child in self.children],
        }


def _extract_js_array(text: str, start_index: int) -> str:
    open_index = start_index if 0 <= start_index < len(text) and text[start_index] == "[" else text.find("[", start_index)
    if open_index < 0:
        raise ValueError("array start '[' not found")

    depth = 0
    in_string = False
    escaped = False
    for index in range(open_index, len(text)):
        ch = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return text[open_index : index + 1]

    raise ValueError("array closing ']' not found")


def _js_object_array_to_json_text(array_text: str) -> str:
    return re.sub(r'([{\[,])([A-Za-z_]\w*):', r'\1"\2":', array_text)


def discover_search_bundles(
    session: requests.Session | None = None,
    *,
    timeout: float = 30.0,
) -> list[str]:
    session = _prepare_session(session)
    response = session.get(SEARCH_PAGE_URL, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    script_urls: list[str] = []
    for tag in soup.find_all("script", src=True):
        src = clean_text(tag["src"])
        if "we/static/js/app-" not in src:
            continue
        if src.startswith("//"):
            src = f"https:{src}"
        script_urls.append(src)
    return script_urls


def find_taxonomy_bundle_urls(
    script_urls: list[str],
    session: requests.Session | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[str, str]:
    session = _prepare_session(session)

    area_url = ""
    function_url = ""
    for script_url in script_urls:
        text = session.get(script_url, timeout=timeout).text
        if not area_url and 'id:"010000",c:"北京",l:"北京"' in text:
            area_url = script_url
        if not function_url and 'value:"3000",label:"销售人员"' in text:
            function_url = script_url
        if area_url and function_url:
            break

    if not area_url:
        raise RuntimeError("could not locate 51job area taxonomy bundle")
    if not function_url:
        raise RuntimeError("could not locate 51job function taxonomy bundle")
    return area_url, function_url


def extract_area_tree_from_bundle(bundle_text: str) -> list[AreaNode]:
    anchor = 'id:"010000",c:"北京",l:"北京"'
    anchor_index = bundle_text.find(anchor)
    if anchor_index < 0:
        raise RuntimeError("area tree anchor not found in bundle")

    start_index = bundle_text.rfind("var i=[", 0, anchor_index)
    if start_index < 0:
        raise RuntimeError("area tree start marker not found in bundle")

    array_text = _extract_js_array(bundle_text, start_index + len("var i="))
    payload = json.loads(_js_object_array_to_json_text(array_text))

    def to_node(row: dict[str, Any]) -> AreaNode:
        return AreaNode(
            area_id=str(row.get("id", "")),
            name=clean_text(row.get("c", "")),
            label=clean_text(row.get("l", "")) or clean_text(row.get("c", "")),
            children=[to_node(child) for child in row.get("sub", []) or []],
        )

    nodes: list[AreaNode] = []
    for group in payload:
        for row in group.get("sub", []) or []:
            nodes.append(to_node(row))
    return nodes


def extract_function_codes_from_bundle(bundle_text: str) -> list[dict[str, str]]:
    anchor = 'value:"3000",label:"销售人员"'
    anchor_index = bundle_text.find(anchor)
    if anchor_index < 0:
        raise RuntimeError("function code anchor not found in bundle")

    start_index = bundle_text.rfind('a["default"]=[', 0, anchor_index)
    if start_index < 0:
        raise RuntimeError("function code start marker not found in bundle")

    array_text = _extract_js_array(bundle_text, start_index + len('a["default"]='))
    rows = re.findall(r'value:"([^"]+)",label:"([^"]*)"', array_text)

    seen: set[str] = set()
    codes: list[dict[str, str]] = []
    for value, label in rows:
        value = clean_text(value)
        label = clean_text(label)
        if not value or value in seen:
            continue
        seen.add(value)
        codes.append({"function_code": value, "function_label": label})
    return codes


def fetch_search_taxonomies(
    *,
    session: requests.Session | None = None,
    area_output: Path | None = None,
    function_output: Path | None = None,
    timeout: float = 30.0,
) -> tuple[list[AreaNode], list[dict[str, str]]]:
    session = _prepare_session(session)
    script_urls = discover_search_bundles(session, timeout=timeout)
    area_url, function_url = find_taxonomy_bundle_urls(script_urls, session, timeout=timeout)

    area_bundle = session.get(area_url, timeout=timeout).text
    function_bundle = session.get(function_url, timeout=timeout).text

    area_tree = extract_area_tree_from_bundle(area_bundle)
    function_codes = extract_function_codes_from_bundle(function_bundle)

    if area_output is not None:
        ensure_parent(area_output)
        area_output.write_text(
            json.dumps([node.to_dict() for node in area_tree], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if function_output is not None:
        ensure_parent(function_output)
        function_output.write_text(
            json.dumps(function_codes, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return area_tree, function_codes


def flatten_area_index(area_tree: list[AreaNode]) -> dict[str, AreaNode]:
    index: dict[str, AreaNode] = {}

    def visit(node: AreaNode) -> None:
        current = index.get(node.area_id)
        if current is None or len(node.children) > len(current.children):
            index[node.area_id] = node
        for child in node.children:
            visit(child)

    for root in area_tree:
        visit(root)
    return index
