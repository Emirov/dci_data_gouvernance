"""Helpers to translate governance YAML into dbt and Great Expectations outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

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
    range_hint = {key: value for key, value in range_hint.items() if value is not None}
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
        range_values = rules["accepted_range"] or {}
        params: Dict[str, Any] = {}
        if "min" in range_values:
            params["min_value"] = range_values["min"]
        if "max" in range_values:
            params["max_value"] = range_values["max"]
        if params:
            tests.append(
                {
                    "dbt_expectations.expect_column_values_to_be_between": params
                }
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


def governance_to_dbt(doc: Dict[str, Any]) -> tuple[str, str]:
    """Return the dbt YAML payload and filename for a governance document."""

    if "tables" in doc:
        models = []
        for table in doc.get("tables", []):
            models.append(
                {
                    "name": table.get("name"),
                    "columns": _dbt_columns(table.get("columns", [])),
                }
            )
        payload = {"version": 2, "models": models}
        return (
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
            "schema.yml",
        )

    dataset = doc.get("dataset", {})
    columns = doc.get("columns", [])
    root_key = "sources" if dataset.get("kind") == "source" else "models"
    payload: Dict[str, Any] = {"version": 2, root_key: []}

    if root_key == "sources":
        source: Dict[str, Any] = {
            "name": dataset.get("domain"),
            "tables": [
                {
                    "name": dataset.get("name"),
                    "columns": _dbt_columns(columns),
                }
            ],
        }
        if dataset.get("database"):
            source["database"] = dataset["database"]
        if dataset.get("schema"):
            source["schema"] = dataset["schema"]
        payload[root_key].append(source)
    else:
        payload[root_key].append(
            {
                "name": dataset.get("name"),
                "columns": _dbt_columns(columns),
            }
        )

    filename = "sources.yml" if root_key == "sources" else "schema.yml"
    return (
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        filename,
    )


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
            range_values = rules["accepted_range"] or {}
            kwargs = {"column": column_name}
            if "min" in range_values:
                kwargs["min_value"] = range_values["min"]
            if "max" in range_values:
                kwargs["max_value"] = range_values["max"]
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
                    "kwargs": {
                        "column": column_name,
                        "regex": rules["regex"],
                    },
                }
            )

    suite = {"expectation_suite_name": name, "expectations": expectations}
    return yaml.safe_dump(suite, sort_keys=False, allow_unicode=True)


def governance_to_ge(doc: Dict[str, Any]) -> Dict[str, str]:
    """Return mapping of table names to Great Expectations suite YAML."""

    if "tables" in doc:
        suites: Dict[str, str] = {}
        for table in doc.get("tables", []):
            suites[table.get("name")] = _ge_for_columns(
                table.get("name"), table.get("columns", [])
            )
        return suites

    dataset = doc.get("dataset", {})
    return {
        dataset.get("name"): _ge_for_columns(
            dataset.get("name"), doc.get("columns", [])
        )
    }


def emit_from_governance(path: Path, out_dir: Path, emit: List[str]) -> Path:
    """Read a governance file and write dbt and/or GE outputs."""

    # Load the governance spec once for shared downstream use.
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Generate dbt assets when requested by the caller.
    if "dbt" in emit:
        dbt_dir = out_dir / "dbt"
        dbt_dir.mkdir(parents=True, exist_ok=True)
        dbt_text, filename = governance_to_dbt(document)
        dbt_dir.joinpath(filename).write_text(dbt_text, encoding="utf-8")

    # Generate Great Expectations suites when requested by the caller.
    if "ge" in emit:
        ge_dir = out_dir / "ge"
        ge_dir.mkdir(parents=True, exist_ok=True)
        for name, text in governance_to_ge(document).items():
            ge_dir.joinpath(f"{name}_suite.yml").write_text(text, encoding="utf-8")

    return out_dir
