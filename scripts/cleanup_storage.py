#!/usr/bin/env python3
"""Utility to prune logs and shrink SQLite WAL files."""
import argparse
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
DB_PATH = ROOT / "data" / "companies.db"
PLAYWRIGHT_CACHE = Path.home() / ".cache" / "ms-playwright"


def prune_logs(keep: int) -> None:
    if keep <= 0 or not LOG_DIR.exists():
        return
    log_files = sorted(LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in log_files[keep:]:
        old.unlink(missing_ok=True)


def checkpoint_db(vacuum: bool) -> None:
    if not DB_PATH.exists():
        return
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        if vacuum:
            con.execute("VACUUM;")
    finally:
        con.close()


def remove_playwright_cache() -> None:
    if PLAYWRIGHT_CACHE.exists():
        shutil.rmtree(PLAYWRIGHT_CACHE)


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean up disk usage without affecting scraping accuracy")
    parser.add_argument("--keep-logs", type=int, default=5, help="How many recent log files to keep")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after checkpoint")
    parser.add_argument("--clear-playwright-cache", action="store_true", help="Remove ~/.cache/ms-playwright")
    args = parser.parse_args()

    prune_logs(args.keep_logs)
    checkpoint_db(args.vacuum)
    if args.clear_playwright_cache:
        remove_playwright_cache()

    print(f"Cleanup done at {datetime.now():%Y-%m-%d %H:%M:%S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
