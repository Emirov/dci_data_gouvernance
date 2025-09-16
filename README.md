# schema-yaml-starter

Toolkit for managing dataset schemas and data-quality rules in a single
place. The CLI supports two main workflows:

1. **Infer schemas from data files** – scan `./data` and write YAML
   descriptions of tables.
2. **Emit dbt and Great Expectations (GE) artifacts** – read an
   authoritative schema/governance YAML and translate its rules into dbt
   tests and GE expectation suites.

## 1. Infer schemas from raw data

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1  # or `source .venv/bin/activate` on Unix
pip install -r requirements.txt
python -m schema_yaml.cli --data ./data --out ./out
# after editable install you can call:
# schema-yaml --data ./data --out ./out
```

### Output
* One YAML per table: `./out/<table>.schema.yaml`
* One combined YAML aggregating all tables: `./out/_all_schemas.yaml`

### Supported inputs
* CSV, XLSX
* Parquet (when `pyarrow` or `fastparquet` is installed)

## 2. Author schema rules and generate checks

Edit the combined `_all_schemas.yaml` (or any schema file) to append
validation rules for each column. Example:

```yaml
version: 1
tables:
  - name: customers
    columns:
      - name: customer_id
        type: integer
        rules:
          not_null: true
          unique: true
      - name: email
        type: string
        rules:
          regex: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
      - name: age
        type: integer
        rules:
          accepted_range: {min: 0, max: 120}
```

Then emit dbt v2 YAML and GE expectation suites from this single
definition:

```bash
python -m schema_yaml.cli \
  --governance out/_all_schemas.yaml \
  --emit dbt,ge \
  --out ./out
```

Artifacts are written to:

* `out/dbt/` – dbt `schema.yml` with tests mapped from rules
* `out/ge/` – one GE expectation suite per table

### Rule mapping cheat‑sheet

| Rule in schema YAML              | dbt test                                           | GE expectation                               |
|----------------------------------|----------------------------------------------------|----------------------------------------------|
| `not_null: true`                 | `not_null`                                         | `expect_column_values_to_not_be_null`        |
| `unique: true`                   | `unique`                                           | `expect_column_values_to_be_unique`          |
| `accepted_range: {min, max}`     | `dbt_expectations.expect_column_values_to_be_between` | `expect_column_values_to_be_between`   |
| `regex: '<pattern>'`             | `dbt_expectations.expect_column_values_to_match_regex` | `expect_column_values_to_match_regex` |

This workflow keeps your validation logic in one neutral YAML file
while producing artifacts for both warehouse‑side (dbt) and landing
zone (GE) checks.

