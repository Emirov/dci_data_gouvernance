
from pathlib import Path
import yaml
import pandas as pd
from schema_yaml.inspector import inspect_folder, render_yaml, write_outputs


def test_inspect_folder(tmp_path: Path):
    work = tmp_path / "data"
    work.mkdir()
    (work / "customers.csv").write_text("customer_id,email\n1,test@example.com\n", encoding="utf-8")
    (work / "orders.csv").write_text("order_id,amount\n1,10.0\n", encoding="utf-8")

    pairs = inspect_folder(work)
    names = [t for t, _ in pairs]
    assert "customers" in names and "orders" in names
    cust = dict(pairs)["customers"]
    assert "customer_id" in cust and "email" in cust

def test_render_yaml_roundtrip():
    schema = {"id": "integer", "email": "string", "created_at": "datetime"}
    ytext = render_yaml("users", schema)
    doc = yaml.safe_load(ytext)
    assert doc["tables"][0]["name"] == "users"
    cols = {c["name"]: c["type"] for c in doc["tables"][0]["columns"]}
    assert cols == schema

def test_write_outputs(tmp_path: Path):
    pairs = [
        ("customers", {"customer_id": "integer", "email": "string"}),
        ("orders", {"order_id": "integer", "amount": "float"}),
    ]
    out_dir = write_outputs(pairs, tmp_path / "out")
    combined = yaml.safe_load((out_dir / "_all_schemas.yaml").read_text(encoding="utf-8"))
    assert len(combined["tables"]) == 2
    assert (out_dir / "customers.schema.yaml").exists()
    assert (out_dir / "orders.schema.yaml").exists()


def test_config_mode(tmp_path: Path):
    tmp_data = tmp_path / "data"
    tmp_data.mkdir()
    # create sample csv and xlsx
    (tmp_data / "customers.csv").write_text("customer_id,email\n1,test@example.com\n", encoding="utf-8")
    df = pd.DataFrame({"customer_id": [1], "email": ["a@b.com"]})
    df.to_excel(tmp_data / "customers.xlsx", index=False)
    (tmp_data / "orders.csv").write_text("order_id,amount\n1,5.0\n", encoding="utf-8")

    cfg = {
        "version": 1,
        "base_dir": str(tmp_data),
        "sources": [
            {"name": "cust_csv", "path": "customers.csv", "format": "csv", "table": "customers"},
            {"name": "cust_xlsx", "path": "customers.xlsx", "format": "xlsx", "sheet": "Sheet1", "table": "customers_xlsx"},
            {"name": "orders_any", "glob": "orders*.csv", "format": "csv", "table_from_stem": True},
        ]
    }
    cfg_path = tmp_path / "sources.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True), encoding="utf-8")

    from schema_yaml.inspector import inspect_from_config
    pairs = inspect_from_config(cfg_path)
    names = [t for t, _ in pairs]
    assert "customers" in names
    assert "customers_xlsx" in names
    assert "orders" in names
