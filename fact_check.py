#!/usr/bin/env python3
"""
Easy entrypoint (like `python main.py`) for DB fact-checking.

Defaults:
- no cache (temporary dir)
- output to /tmp (so you don't accidentally commit)
"""

from __future__ import annotations

import argparse
import os
import tempfile
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
    ap.add_argument("--table", default="companies")
    ap.add_argument("--status", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    ap.add_argument("--max-fetch", type=int, default=6000)
    ap.add_argument("--out", default="", help="default: reports/fact_check_<db>_<ts>.csv")
    ap.add_argument("--cache-dir", default="", help="default: temporary (deleted after run)")
    args = ap.parse_args(argv)

    from scripts.fact_check_db import main as fact_check_main

    ts = time.strftime("%Y%m%d_%H%M%S")
    if not args.out:
        base = os.path.basename(args.db).replace(".db", "")
        os.makedirs("reports", exist_ok=True)
        args.out = os.path.join("reports", f"fact_check_{base}_{ts}.csv")

    if args.cache_dir:
        return fact_check_main(
            [
                "--db",
                args.db,
                "--table",
                args.table,
                "--status",
                args.status,
                "--limit",
                str(int(args.limit)),
                "--out",
                args.out,
                "--cache-dir",
                args.cache_dir,
                "--sleep-sec",
                str(float(args.sleep_sec)),
                "--max-fetch",
                str(int(args.max_fetch)),
            ]
        )

    with tempfile.TemporaryDirectory(prefix="fact_check_cache_") as td:
        return fact_check_main(
            [
                "--db",
                args.db,
                "--table",
                args.table,
                "--status",
                args.status,
                "--limit",
                str(int(args.limit)),
                "--out",
                args.out,
                "--cache-dir",
                td,
                "--sleep-sec",
                str(float(args.sleep_sec)),
                "--max-fetch",
                str(int(args.max_fetch)),
            ]
        )


if __name__ == "__main__":
    raise SystemExit(main())
