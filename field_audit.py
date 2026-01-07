#!/usr/bin/env python3
"""
Easy entrypoint for auditing DB fields (like `python main.py`).

Defaults:
- output to /tmp (so you don't accidentally commit)
"""

from __future__ import annotations

import argparse
import os
import time


def main(argv: list[str] | None = None) -> int:
    # Load .env so --db can be omitted when COMPANIES_DB_PATH is set there.
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--status", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-severity", default="medium", choices=["low", "medium", "high"])
    ap.add_argument("--progress", action="store_true", default=False, help="print progress to stderr")
    ap.add_argument("--out", default="", help="default: reports/field_audit_<db>_<ts>.csv")
    args = ap.parse_args(argv)

    from scripts.audit_db_fields import main as audit_main

    if not args.out:
        ts = time.strftime("%Y%m%d_%H%M%S")
        base = os.path.basename(args.db).replace(".db", "")
        os.makedirs("reports", exist_ok=True)
        args.out = os.path.join("reports", f"field_audit_{base}_{ts}.csv")

    cmd = ["--db", args.db, "--min-severity", args.min_severity, "--out", args.out]
    if args.status:
        cmd += ["--status", args.status]
    if args.limit:
        cmd += ["--limit", str(int(args.limit))]
    if args.progress:
        cmd += ["--progress-every", "50"]
    return audit_main(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
