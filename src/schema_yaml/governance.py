from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
import yaml


def _dbt_tests_from_rules(rules: Dict[str, Any]) -> List[Any]:
    tests: List[Any] = []
    if not rules:
        return tests
    if rules.get("not_null"):
        tests.append("not_null")
    if rules.get("unique"):
        tests.append("unique")
    if "accepted_range" in rules:
        r = rules["accepted_range"] or {}
        params: Dict[str, Any] = {}
        if "min" in r:
            params["min_value"] = r["min"]
        if "max" in r:
            params["max_value"] = r["max"]
        tests.append({"dbt_expectations.expect_column_values_to_be_between": params})
    if "regex" in rules:
        tests.append({"dbt_expectations.expect_column_values_to_match_regex": {"regex": rules["regex"]}})
    return tests


def governance_to_dbt(doc: Dict[str, Any]) -> str:
    ds = doc.get("dataset", {})
    cols = doc.get("columns", [])
    root_key = "sources" if ds.get("kind") == "source" else "models"
    out: Dict[str, Any] = {"version": 2, root_key: []}

    if root_key == "sources":
        src: Dict[str, Any] = {
            "name": ds.get("domain"),
            "tables": [
                {
                    "name": ds.get("name"),
                    "columns": [
                        {
                            "name": c.get("name"),
                            "description": c.get("description", ""),
                            "tests": _dbt_tests_from_rules(c.get("rules", {})),
                        }
                        for c in cols
                    ],
                }
            ],
        }
        if ds.get("database"):
            src["database"] = ds["database"]
        if ds.get("schema"):
            src["schema"] = ds["schema"]
        out[root_key].append(src)
    else:
        model = {
            "name": ds.get("name"),
            "columns": [
                {
                    "name": c.get("name"),
                    "description": c.get("description", ""),
                    "tests": _dbt_tests_from_rules(c.get("rules", {})),
                }
                for c in cols
            ],
        }
        out[root_key].append(model)

    return yaml.safe_dump(out, sort_keys=False, allow_unicode=True)


def governance_to_ge(doc: Dict[str, Any]) -> str:
    ds = doc.get("dataset", {})
    cols = doc.get("columns", [])
    expectations: List[Dict[str, Any]] = []

    for c in cols:
        name = c.get("name")
        rules = c.get("rules", {})
        if rules.get("not_null"):
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": name},
            })
        if rules.get("unique"):
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": name},
            })
        if "accepted_range" in rules:
            r = rules["accepted_range"] or {}
            kwargs = {"column": name}
            if "min" in r:
                kwargs["min_value"] = r["min"]
            if "max" in r:
                kwargs["max_value"] = r["max"]
            expectations.append({
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": kwargs,
            })
        if "regex" in rules:
            expectations.append({
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": name, "regex": rules["regex"]},
            })

    suite = {
        "expectation_suite_name": ds.get("name"),
        "expectations": expectations,
    }
    return yaml.safe_dump(suite, sort_keys=False, allow_unicode=True)


def emit_from_governance(path: Path, out_dir: Path, emit: List[str]) -> Path:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)
    if "dbt" in emit:
        dbt_dir = out_dir / "dbt"
        dbt_dir.mkdir(parents=True, exist_ok=True)
        fname = "sources.yml" if doc.get("dataset", {}).get("kind") == "source" else "schema.yml"
        dbt_dir.joinpath(fname).write_text(governance_to_dbt(doc), encoding="utf-8")
    if "ge" in emit:
        ge_dir = out_dir / "ge"
        ge_dir.mkdir(parents=True, exist_ok=True)
        suite_name = f"{doc.get('dataset', {}).get('name')}_suite.yml"
        ge_dir.joinpath(suite_name).write_text(governance_to_ge(doc), encoding="utf-8")
    return out_dir

