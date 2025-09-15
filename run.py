import subprocess, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent

def run():
    cfg = BASE / "data" / "sources.yaml"
    out = BASE / "out"
    out.mkdir(parents=True, exist_ok=True)

    if cfg.exists():
        print(f"== Running schema-yaml with config: {cfg} -> {out} ==")
        cmd = [sys.executable, "-m", "schema_yaml.cli",
               "--config", str(cfg),
               "--out", str(out)]
    else:
        print("== No sources.yaml found. Scanning folder ./data -> ./out ==")
        cmd = [sys.executable, "-m", "schema_yaml.cli",
               "--data", str(BASE / "data"),
               "--out", str(out)]

    subprocess.check_call(cmd)
    print("== Done. Files in ./out ==")
    for p in sorted(out.glob("*.yaml")):
        print(" -", p.name)

if __name__ == "__main__":
    run()
