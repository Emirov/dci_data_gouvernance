from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Tuple
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


def governance_to_dbt(doc: Dict[str, Any]) -> Tuple[str, str]:
    """Return (yaml_text, filename) for dbt from governance-style doc.

    Supports two shapes:
      1) Multi-table governance file:
         { "tables": [ { "name": ..., "columns": [...]} ] }
         -> returns schema.yml (models entries)
      2) Single dataset governance file:
         {
           "dataset": {"kind": "source"|"model", "name": ..., "domain": ..., "database": ..., "schema": ...},
           "columns": [...]
         }
         -> returns sources.yml (if kind == source) or schema.yml (if kind != source)
    """
    # Case 1: Multi-table governance "tables" (treat as dbt models)
    if "tables" in doc:
        models = []
        for table in doc.get("tables", []):
            models.append(
                {
                    "name": table.get("name"),
                    "columns": [
                        {
                            "name": c.get("name"),
                            "description": c.get("description", ""),
                            "tests": _dbt_tests_from_rules(c.get("rules", {})),
                        }
                        for c in table.get("columns", [])
                    ],
                }
            )
        out = {"version": 2, "models": models}
        return yaml.safe_dump(out, sort_keys=False, allow_unicode=True), "schema.yml"

    # Case 2: Single dataset governance
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

    fname = "sources.yml" if root_key == "sources" else "schema.yml"
    return yaml.safe_dump(out, sort_keys=False, allow_unicode=True), fname


def _ge_for_columns(name: str, columns: List[Dict[str, Any]]) -> str:
    expectations: List[Dict[str, Any]] = []
    for c in columns:
        col_name = c.get("name")
        rules = c.get("rules", {})
        if rules.get("not_null"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col_name},
                }
            )
        if rules.get("unique"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": col_name},
                }
            )
        if "accepted_range" in rules:
            r = rules["accepted_range"] or {}
            kwargs: Dict[str, Any] = {"column": col_name}
            if "min" in r:
                kwargs["min_value"] = r["min"]
            if "max" in r:
                kwargs["max_value"] = r["max"]
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": kwargs,
                }
            )
        if "regex" in rules:
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": col_name, "regex": rules["regex"]},
                }
            )
    suite = {"expectation_suite_name": name, "expectations": expectations}
    return yaml.safe_dump(suite, sort_keys=False, allow_unicode=True)


def governance_to_ge(doc: Dict[str, Any]) -> Dict[str, str]:
    """Return mapping of table name -> Great Expectations suite YAML.

    Supports both:
      - Multi-table governance (returns one suite per table)
      - Single dataset governance (returns one suite for dataset name)
    """
    if "tables" in doc:
        out: Dict[str, str] = {}
        for table in doc.get("tables", []):
            out[table.get("name")] = _ge_for_columns(table.get("name"), table.get("columns", []))
        return out

    ds = doc.get("dataset", {})
    return {ds.get("name"): _ge_for_columns(ds.get("name"), doc.get("columns", []))}


def emit_from_governance(path: Path, out_dir: Path, emit: List[str]) -> Path:
    """Read a governance YAML and emit dbt and/or GE YAML files.

    Args:
        path: Path to governance YAML input.
        out_dir: Output directory.
        emit: List including "dbt" and/or "ge".
    """
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    if "dbt" in emit:
        dbt_dir = out_dir / "dbt"
        dbt_dir.mkdir(parents=True, exist_ok=True)
        dbt_text, fname = governance_to_dbt(doc)
        dbt_dir.joinpath(fname).write_text(dbt_text, encoding="utf-8")

    if "ge" in emit:
        ge_dir = out_dir / "ge"
        ge_dir.mkdir(parents=True, exist_ok=True)
        for name, text in governance_to_ge(doc).items():
            ge_dir.joinpath(f"{name}_suite.yml").write_text(text, encoding="utf-8")

    return out_dir
