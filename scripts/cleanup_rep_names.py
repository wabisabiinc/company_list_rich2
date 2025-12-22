#!/usr/bin/env python3
"""
Normalize rep_name column in the companies DB.

Usage:
    python scripts/cleanup_rep_names.py --db data/companies_logistics.db --dry-run
    python scripts/cleanup_rep_names.py --db data/companies_logistics.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.company_scraper import CompanyScraper


def cleanup(db_path: str, dry_run: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("SELECT id, rep_name FROM companies").fetchall()

    updated = 0
    cleared = 0
    for row in rows:
        raw = (row["rep_name"] or "").strip()
        if not raw:
            continue
        cleaned = CompanyScraper.clean_rep_name(raw)
        new_val = raw
        if cleaned and cleaned != raw:
            new_val = cleaned
        elif cleaned is None and raw in {"企業", "法人", "会社"}:
            new_val = ""
        if new_val == raw:
            continue
        updated += 1
        if not new_val:
            cleared += 1
        if not dry_run:
            cur.execute(
                "UPDATE companies SET rep_name=? WHERE id=?",
                (new_val, row["id"]),
            )

    if not dry_run:
        conn.commit()
    conn.close()
    action = "would update" if dry_run else "updated"
    print(f"{action} {updated} rows (cleared={cleared})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize rep_name column.")
    parser.add_argument("--db", default="data/companies.db", help="Path to SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing changes")
    args = parser.parse_args()
    cleanup(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

