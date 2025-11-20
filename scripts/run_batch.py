#!/usr/bin/env python3
"""Helper to run main.py in small batches to keep VM runtimes short."""
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run main.py for a limited number of rows")
    parser.add_argument("--rows", type=int, default=0, help="Temporarily override MAX_ROWS (0 keeps .env)")
    parser.add_argument("--id-min", type=int, default=None, help="Override ID_MIN")
    parser.add_argument("--id-max", type=int, default=None, help="Override ID_MAX")
    parser.add_argument("--env", default=str(ROOT / ".env"), help=".env file to source")
    args = parser.parse_args()

    env = os.environ.copy()
    if args.rows is not None and args.rows >= 0:
        env["MAX_ROWS"] = str(args.rows)
    if args.id_min is not None:
        env["ID_MIN"] = str(args.id_min)
    if args.id_max is not None:
        env["ID_MAX"] = str(args.id_max)

    cmd = [sys.executable, str(ROOT / "main.py")]
    print(f"Running main.py with MAX_ROWS={env.get('MAX_ROWS')} ID_MIN={env.get('ID_MIN')} ID_MAX={env.get('ID_MAX')}")
    try:
        completed = subprocess.run(cmd, env=env, check=False, cwd=ROOT)
    except KeyboardInterrupt:
        return 130
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
