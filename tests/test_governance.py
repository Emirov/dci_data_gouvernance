from pathlib import Path
import yaml

from schema_yaml.governance import emit_from_governance


def sample_governance() -> str:
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

