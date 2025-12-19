#!/usr/bin/env python3
"""
Normalize address/found_address fields in the companies DB.

Usage:
    python scripts/cleanup_addresses.py --db data/companies_logistics.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from main import normalize_address, sanitize_text_block


def _clean_value(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    normalized = normalize_address(raw)
    if normalized:
        return normalized
    return sanitize_text_block(raw)


def cleanup(db_path: str, dry_run: bool = False) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    rows = cur.execute("SELECT id, address, found_address FROM companies").fetchall()

    updated = 0
    for row in rows:
        addr_new = _clean_value(row["address"])
        found_new = _clean_value(row["found_address"])
        if addr_new == (row["address"] or "") and found_new == (row["found_address"] or ""):
            continue
        updated += 1
        if not dry_run:
            cur.execute(
                "UPDATE companies SET address=?, found_address=? WHERE id=?",
                (addr_new, found_new, row["id"]),
            )

    if not dry_run:
        conn.commit()
    conn.close()
    action = "would update" if dry_run else "updated"
    print(f"{action} {updated} / {len(rows)} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize address/found_address columns.")
    parser.add_argument("--db", default="data/companies.db", help="Path to SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing changes")
    args = parser.parse_args()
    cleanup(args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
