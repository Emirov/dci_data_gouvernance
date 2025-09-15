
from __future__ import annotations

import argparse
from pathlib import Path
from .inspector import inspect_folder, write_outputs, inspect_from_config
from .governance import emit_from_governance

def main():
    parser = argparse.ArgumentParser(description="Infer schemas or emit dbt/GE YAML from governance.")
    parser.add_argument("--data", type=str, default="./data", help="Input folder with files")
    parser.add_argument("--config", type=str, default=None, help="YAML config that lists files to scan")
    parser.add_argument("--emit", type=str, default="", help="Comma-separated outputs to emit (dbt,ge)")
    parser.add_argument("--out", type=str, default="./out", help="Output folder for YAML")
    args = parser.parse_args()

    out_dir = Path(args.out)

    if args.governance:
        emit = [e.strip() for e in args.emit.split(",") if e.strip()]
        if not emit:
            raise SystemExit("--emit must specify outputs when --governance is used")
        emit_from_governance(Path(args.governance), out_dir, emit)
        print(f"Governance emitted: {', '.join(emit)} -> {out_dir}")
        return

    data_dir = Path(args.data)
    pairs = inspect_from_config(Path(args.config)) if args.config else inspect_folder(data_dir)
    write_outputs(pairs, out_dir)

    print(f"Scanned: {data_dir}")
    for t, s in pairs:
        print(f"- {t}: {len(s)} columns")
    print(f"YAML written to: {out_dir}")

if __name__ == "__main__":
    main()
