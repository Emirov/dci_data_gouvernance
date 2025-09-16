"""Tests for governance emission utilities."""

from pathlib import Path

import yaml

from schema_yaml.governance import emit_from_governance


def sample_governance() -> str:
    """Return a governance YAML snippet used by multiple tests."""

    return yaml.safe_dump(
        {
            "version": 1,
            "tables": [
                {
                    "name": "customers",
                    "columns": [
                        {
                            "name": "customer_id",
                            "type": "integer",
                            "description": "Unique customer id",
                            "rules": {"not_null": True, "unique": True},
                        },
                        {
                            "name": "age",
                            "type": "integer",
                            "rules": {"accepted_range": {"min": 0, "max": 120}},
                        },
                    ],
                }
            ],
        },
        sort_keys=False,
        allow_unicode=True,
    )


def test_emit_from_governance(tmp_path: Path):
    """Emit both dbt and GE outputs and validate expectations mapping."""

    gpath = tmp_path / "governance.yaml"
    gpath.write_text(sample_governance(), encoding="utf-8")

    out_dir = tmp_path / "out"
    emit_from_governance(gpath, out_dir, ["dbt", "ge"])

    dbt_file = out_dir / "dbt" / "schema.yml"
    assert dbt_file.exists()
    dbt_doc = yaml.safe_load(dbt_file.read_text(encoding="utf-8"))
    cols = dbt_doc["models"][0]["columns"]
    cid = next(c for c in cols if c["name"] == "customer_id")
    assert "not_null" in cid["tests"] and "unique" in cid["tests"]
    age = next(c for c in cols if c["name"] == "age")
    assert {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0, "max_value": 120}} in age["tests"]

    ge_file = out_dir / "ge" / "customers_suite.yml"
    assert ge_file.exists()
    ge_doc = yaml.safe_load(ge_file.read_text(encoding="utf-8"))
    exp_types = {e["expectation_type"] for e in ge_doc["expectations"]}
    assert "expect_column_values_to_not_be_null" in exp_types
    assert "expect_column_values_to_be_unique" in exp_types
    between = next(e for e in ge_doc["expectations"] if e["expectation_type"] == "expect_column_values_to_be_between")
    assert between["kwargs"]["min_value"] == 0 and between["kwargs"]["max_value"] == 120


def test_emit_accepts_rule_variants(tmp_path: Path):
    """Ensure alternate rule syntaxes merge into the canonical outputs."""

    doc = {
        "version": 1,
        "tables": [
            {
                "name": "customers",
                "columns": [
                    {"name": "customer_id", "tests": ["not_null", "unique"]},
                    {
                        "name": "age",
                        "tests": [
                            {
                                "dbt_expectations.expect_column_values_to_be_between": {
                                    "min_value": 0,
                                    "max_value": 120,
                                }
                            }
                        ],
                    },
                    {
                        "name": "score",
                        "constraints": [{"range": {"min": 0, "max": 1}}],
                    },
                    {
                        "name": "email",
                        "nullable": False,
                        "constraints": [{"regex": {"pattern": "^[^@\\s]+@[^@\\s]+$"}}],
                    },
                    {
                        "name": "height",
                        "min": 0,
                        "max": 250,
                    },
                ],
            }
        ],
    }

    gpath = tmp_path / "governance.yaml"
    gpath.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")

    out_dir = tmp_path / "out"
    emit_from_governance(gpath, out_dir, ["dbt", "ge"])

    dbt_doc = yaml.safe_load((out_dir / "dbt" / "schema.yml").read_text(encoding="utf-8"))
    columns = {c["name"]: c for c in dbt_doc["models"][0]["columns"]}

    assert set(columns["customer_id"]["tests"]) == {"not_null", "unique"}

    age_tests = columns["age"]["tests"]
    assert {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0, "max_value": 120}} in age_tests

    score_tests = columns["score"]["tests"]
    assert {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0, "max_value": 1}} in score_tests

    email_tests = columns["email"]["tests"]
    assert any(
        isinstance(test, dict)
        and test.get("dbt_expectations.expect_column_values_to_match_regex", {}).get("regex") == "^[^@\\s]+@[^@\\s]+$"
        for test in email_tests
    )
    assert "not_null" in email_tests

    height_tests = columns["height"]["tests"]
    assert {"dbt_expectations.expect_column_values_to_be_between": {"min_value": 0, "max_value": 250}} in height_tests

    ge_doc = yaml.safe_load((out_dir / "ge" / "customers_suite.yml").read_text(encoding="utf-8"))
    exp_by_column = {}
    for exp in ge_doc["expectations"]:
        exp_by_column.setdefault(exp["kwargs"]["column"], []).append(exp["expectation_type"])

    assert set(exp_by_column["customer_id"]) == {
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_unique",
    }
    assert "expect_column_values_to_be_between" in exp_by_column["age"]
    assert "expect_column_values_to_be_between" in exp_by_column["score"]
    assert "expect_column_values_to_match_regex" in exp_by_column["email"]
    assert "expect_column_values_to_not_be_null" in exp_by_column["email"]
    assert "expect_column_values_to_be_between" in exp_by_column["height"]

