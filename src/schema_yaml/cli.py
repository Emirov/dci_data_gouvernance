
"""Command-line interface for the schema governance utilities."""

from __future__ import annotations

import argparse
from pathlib import Path

from .governance import emit_from_governance
from .inspector import inspect_folder, inspect_from_config, write_outputs


def main() -> None:
    """Parse CLI arguments and execute the requested action."""

    parser = argparse.ArgumentParser(
        description="Infer schemas or emit dbt/GE YAML from governance."
    )
    parser.add_argument("--data", type=str, default="./data", help="Input folder with files")
    parser.add_argument(
        "--config", type=str, default=None, help="YAML config that lists files to scan"
    )
    parser.add_argument(
        "--governance",
        type=str,
        default=None,
        help="Schema/governance YAML to emit from",
    )
    parser.add_argument(
        "--emit", type=str, default="", help="Comma-separated outputs to emit (dbt,ge)"
    )
    parser.add_argument("--out", type=str, default="./out", help="Output folder for YAML")
    args = parser.parse_args()

    out_dir = Path(args.out)

    # Emit dbt and/or GE YAML when a governance file is supplied.
    if args.governance:
        emit = [e.strip() for e in args.emit.split(",") if e.strip()]
        if not emit:
            raise SystemExit("--emit must specify outputs when --governance is used")
        emit_from_governance(Path(args.governance), out_dir, emit)
        print(f"Governance emitted: {', '.join(emit)} -> {out_dir}")
        return

    # Otherwise inspect data sources and materialize inferred schemas.
    data_dir = Path(args.data)
    pairs = (
        inspect_from_config(Path(args.config)) if args.config else inspect_folder(data_dir)
    )
    write_outputs(pairs, out_dir)

    print(f"Scanned: {data_dir}")
    for table_name, schema in pairs:
        print(f"- {table_name}: {len(schema)} columns")
    print(f"YAML written to: {out_dir}")


if __name__ == "__main__":
    main()
