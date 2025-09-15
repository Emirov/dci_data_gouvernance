# schema-yaml-starter

Utility to either:

* scan `./data`, infer column names & types using Polars (fallback to Pandas for Excel), and write schema YAMLs
* or read an authoritative schema YAML and emit dbt/Great Expectations YAML for data-quality tests

## Quickstart – infer schemas from data

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m schema_yaml.cli --data ./data --out ./out
# or after editable install:
# schema-yaml --data ./data --out ./out
```

### Output
* One YAML per table: `./out/<table>.schema.yaml`
* One combined YAML: `./out/_all_schemas.yaml`

### Supported inputs
* CSV, XLSX
* (Parquet supported if your env has pyarrow/fastparquet)

## Schema governance → dbt/GE emission

Add validation rules to the combined `_all_schemas.yaml` (or any schema file):

```yaml
version: 1
tables:
  - name: customers
    columns:
      - name: customer_id
        rules:
          not_null: true
          unique: true
      - name: age
        rules:
          accepted_range: {min: 0, max: 120}
```

Generate dbt v2 YAML (schema.yml) and Great Expectations suites:

```bash
python -m schema_yaml.cli --governance out/_all_schemas.yaml --emit dbt,ge --out ./out
```

This writes `out/dbt/` and `out/ge/` directories containing the respective YAML files.

