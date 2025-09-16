"""Helpers to translate governance YAML into dbt and Great Expectations outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


KNOWN_RANGE_KEYS = (
    "accepted_range",
    "range",
    "between",
    "expect_column_values_to_be_between",
    "dbt_expectations.expect_column_values_to_be_between",
)

KNOWN_REGEX_KEYS = ("regex", "pattern", "match", "matches", "expression")

KNOWN_NOT_NULL_KEYS = (
    "not_null",
    "notnull",
    "expect_column_values_to_not_be_null",
)

KNOWN_UNIQUE_KEYS = (
    "unique",
    "distinct",
    "expect_column_values_to_be_unique",
)


def _first_value(mapping: Dict[str, Any], keys: Iterable[str]) -> Any:
    """Return the first non-null value within ``mapping`` for the given keys."""
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _merge_range(target: Dict[str, Any], value: Any) -> None:
    """Normalize range-like inputs and merge them into ``target``."""
    if isinstance(value, dict):
        min_val = _first_value(value, ("min", "min_value", "gte", "lower_bound"))
        max_val = _first_value(value, ("max", "max_value", "lte", "upper_bound"))
    elif isinstance(value, (list, tuple)) and len(value) == 2:
        min_val, max_val = value
    else:
        min_val = max_val = None

    range_spec: Dict[str, Any] = {}
    if min_val is not None:
        range_spec["min"] = min_val
    if max_val is not None:
        range_spec["max"] = max_val

    if not range_spec:
        return

    existing = target.get("accepted_range", {})
    existing.update(range_spec)
    target["accepted_range"] = existing


def _apply_rule(result: Dict[str, Any], key: str, value: Any) -> None:
    """Map a rule key/value pair into the normalized ``result`` dictionary."""
    if key is None:
        return

    lowered = str(key).lower()
    if lowered in KNOWN_NOT_NULL_KEYS:
        if value is not False:
            result["not_null"] = True
        return

    if lowered in KNOWN_UNIQUE_KEYS:
        if value is not False:
            result["unique"] = True
        return

    if lowered in KNOWN_RANGE_KEYS:
        _merge_range(result, value)
        return

    if lowered in KNOWN_REGEX_KEYS:
        if isinstance(value, dict):
            regex_value = _first_value(value, KNOWN_REGEX_KEYS)
        else:
            regex_value = value
        if regex_value:
            result["regex"] = regex_value
        return


def _normalize_rules(column: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten rule variants on ``column`` into a canonical dictionary."""
    result: Dict[str, Any] = {}

    # First, check for explicit rule collections.
    raw_rules = None
    for candidate in ("rules", "tests", "constraints"):
        if candidate in column and column[candidate]:
            raw_rules = column[candidate]
            break

    if raw_rules:
        if isinstance(raw_rules, dict):
            for key, value in raw_rules.items():
                _apply_rule(result, key, value)
        elif isinstance(raw_rules, list):
            for item in raw_rules:
                if isinstance(item, str):
                    _apply_rule(result, item, True)
                elif isinstance(item, dict):
                    for key, value in item.items():
                        _apply_rule(result, key, value)

    # Fall back to column-level hints when no rule collection is provided.
    if column.get("unique") or column.get("distinct"):
        result["unique"] = True
    if column.get("not_null") or column.get("nullable") is False:
        result["not_null"] = True

    range_hint = {
        "min": _first_value(column, ("min", "min_value", "gte", "lower_bound")),
        "max": _first_value(column, ("max", "max_value", "lte", "upper_bound")),
    }
    range_hint = {k: v for k, v in range_hint.items() if v is not None}
    if range_hint:
        existing = result.get("accepted_range", {})
        existing.update(range_hint)
        result["accepted_range"] = existing

    regex_hint = column.get("regex") or column.get("pattern")
    if regex_hint:
        result["regex"] = regex_hint

    return result


def _dbt_tests_from_rules(rules: Dict[str, Any]) -> List[Any]:
    """Create a dbt test configuration list for the given ``rules``."""
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
        if params:
            tests.append(
                {"dbt_expectations.expect_column_values_to_be_between": params}
            )

    if "regex" in rules:
        tests.append(
            {
                "dbt_expectations.expect_column_values_to_match_regex": {
                    "regex": rules["regex"]
                }
            }
        )
    return tests


def _dbt_columns(columns: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build the list of dbt column dictionaries with attached tests."""
    rendered: List[Dict[str, Any]] = []
    for column in columns:
        entry: Dict[str, Any] = {
            "name": column.get("name"),
            "description": column.get("description", ""),
        }
        tests = _dbt_tests_from_rules(_normalize_rules(column))
        if tests:
            entry["tests"] = tests
        rendered.append(entry)
    return rendered


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
                    "columns": _dbt_columns(table.get("columns", [])),
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
                    "columns": _dbt_columns(cols),
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
            "columns": _dbt_columns(cols),
        }
        out[root_key].append(model)

    fname = "sources.yml" if root_key == "sources" else "schema.yml"
    return yaml.safe_dump(out, sort_keys=False, allow_unicode=True), fname


def _ge_for_columns(name: str, columns: List[Dict[str, Any]]) -> str:
    """Build a Great Expectations suite YAML for the supplied ``columns``."""
    expectations: List[Dict[str, Any]] = []
    for column in columns:
        column_name = column.get("name")
        rules = _normalize_rules(column)

        if rules.get("not_null"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": column_name},
                }
            )
        if rules.get("unique"):
            expectations.append(
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": column_name},
                }
            )

        if "accepted_range" in rules:
            r = rules["accepted_range"] or {}
            kwargs: Dict[str, Any] = {"column": column_name}
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
                    "kwargs": {"column": column_name, "regex": rules["regex"]},
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
            out[table.get("name")] = _ge_for_columns(
                table.get("name"), table.get("columns", [])
            )
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
