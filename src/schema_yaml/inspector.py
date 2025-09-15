
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Any
import yaml

try:
    import polars as pl
    HAS_POLARS = True
except Exception:
    pl = None
    HAS_POLARS = False

import pandas as pd


_POLARS_MAP = {
    "Int8": "integer", "Int16": "integer", "Int32": "integer", "Int64": "integer",
    "UInt8": "integer", "UInt16": "integer", "UInt32": "integer", "UInt64": "integer",
    "Float32": "float", "Float64": "float",
    "Boolean": "boolean",
    "String": "string",
    "Date": "date",
    "Datetime": "datetime",
    "Time": "time",
    "Duration": "duration",
    "Decimal": "decimal",
    "Binary": "binary",
    "Categorical": "string",
    "Enum": "string",
    "Object": "string",
    "List": "array",
    "Struct": "struct",
}

def pandas_dtype_to_friendly(dtype: Any) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(dtype) or pd.api.types.is_datetime64tz_dtype(dtype):
        return "datetime"
    if pd.api.types.is_timedelta64_dtype(dtype):
        return "duration"
    if pd.api.types.is_categorical_dtype(dtype) or pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
        return "string"
    return "string"


def infer_with_polars(path: Path) -> Dict[str, str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pl.read_csv(path, try_parse_dates=True, infer_schema_length=1000)
    elif suffix in (".xlsx", ".xls"):
        raise RuntimeError("excel")
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    result = {}
    for name, dtype in zip(df.columns, df.dtypes):
        dname = dtype.__class__.__name__
        if dname.startswith("Datetime"):
            friendly = "datetime"
        else:
            friendly = _POLARS_MAP.get(dname, "string")
        result[name] = friendly
    return result


def infer_with_pandas(path: Path) -> Dict[str, str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")
    return {col: pandas_dtype_to_friendly(df[col].dtype) for col in df.columns}


def infer_schema(path: Path) -> Dict[str, str]:
    if HAS_POLARS:
        try:
            return infer_with_polars(path)
        except RuntimeError as e:
            if str(e) == "excel":
                return infer_with_pandas(path)
            raise
        except Exception:
            return infer_with_pandas(path)
    else:
        return infer_with_pandas(path)


def to_table_name(filename: str) -> str:
    return Path(filename).stem.lower().replace(" ", "_")


def render_yaml(table_name: str, schema_map: Dict[str, str]) -> str:
    data = {
        "version": 1,
        "tables": [
            {
                "name": table_name,
                "columns": [
                    {"name": col, "type": ctype, "description": ""}
                    for col, ctype in schema_map.items()
                ],
            }
        ],
    }
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def inspect_folder(data_dir: Path) -> List[Tuple[str, Dict[str, str]]]:
    supported = (".csv", ".xlsx", ".xls")
    found: List[Tuple[str, Dict[str, str]]] = []
    for p in sorted(Path(data_dir).glob("*")):
        if p.is_file() and p.suffix.lower() in supported:
            schema_map = infer_schema(p)
            found.append((to_table_name(p.name), schema_map))
    return found


def write_outputs(pairs: List[Tuple[str, Dict[str, str]]], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    combined = {"version": 1, "tables": []}

    for table_name, schema_map in pairs:
        yml = render_yaml(table_name, schema_map)
        (out_dir / f"{table_name}.schema.yaml").write_text(yml, encoding="utf-8")
        combined["tables"].append({
            "name": table_name,
            "columns": [{"name": c, "type": t, "description": ""} for c, t in schema_map.items()],
        })

    combined_text = yaml.safe_dump(combined, sort_keys=False, allow_unicode=True)
    (out_dir / "_all_schemas.yaml").write_text(combined_text, encoding="utf-8")
    return out_dir


def infer_excel_sheet(path: Path, sheet: str) -> Dict[str, str]:
    df = pd.read_excel(path, sheet_name=sheet)
    return {col: pandas_dtype_to_friendly(df[col].dtype) for col in df.columns}


def resolve_base_dir(config_path: Path, base_dir: str | None) -> Path:
    if base_dir is None:
        return config_path.parent
    p = Path(base_dir)
    if not p.is_absolute():
        p = (config_path.parent / p).resolve()
    return p


def inspect_from_config(config_path: Path) -> List[Tuple[str, Dict[str, str]]]:
    import yaml as _yaml
    doc = _yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or "sources" not in doc:
        raise ValueError("Invalid config: expecting a mapping with 'sources' list")
    base_dir = resolve_base_dir(Path(config_path), doc.get("base_dir"))
    out: List[Tuple[str, Dict[str, str]]] = []

    for src in doc["sources"]:
        fmt = (src.get("format") or "").lower()
        sheet = src.get("sheet")
        explicit_table = src.get("table")
        table_from_stem = bool(src.get("table_from_stem"))

        paths: List[Path] = []
        if "path" in src:
            p = Path(src["path"])
            if not p.is_absolute():
                p = (base_dir / p).resolve()
            paths.append(p)
        elif "glob" in src:
            g = src["glob"]
            for p in sorted((base_dir).glob(g)):
                if p.is_file():
                    paths.append(p)
        else:
            raise ValueError("Each source must have either 'path' or 'glob'.")

        for p in paths:
            # Compute table name
            if explicit_table and not table_from_stem:
                table_name = explicit_table
            elif table_from_stem:
                table_name = p.stem.lower().replace(" ", "_")
            else:
                table_name = to_table_name(p.name)

            # Infer depending on format and sheet
            if fmt in ("xlsx", "xls") and sheet:
                schema_map = infer_excel_sheet(p, sheet)
            else:
                schema_map = infer_schema(p)

            out.append((table_name, schema_map))

    return out
