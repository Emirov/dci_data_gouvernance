
# schema-yaml-starter

Simple tool that scans `./data`, infers column names & types using Polars (fallback to Pandas for Excel), and writes YAML schema(s) to `./out`.

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:PYTHONPATH = "$PWD\src"
python -m schema_yaml.cli --data ./data --out ./out
# or after editable install:
# schema-yaml --data ./data --out ./out
```

## Output
- One YAML per table: `./out/<table>.schema.yaml`
- One combined YAML: `./out/_all_schemas.yaml`

## Supported inputs
- CSV, XLSX
- (Parquet supported if your env has pyarrow/fastparquet)
