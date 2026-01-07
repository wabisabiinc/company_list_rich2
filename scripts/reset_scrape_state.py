#!/usr/bin/env python3
"""
Reset scraping outputs in a companies SQLite DB to a pre-scrape-like state.

- Keeps import/source identity columns (id, company_name, address/csv_address, employee_count, corporate_number*, hubspot_id, source_csv, reference_*).
- Clears scraped/enriched columns (homepage/phone/found_address/rep_name/description/... and metadata).
- Sets all statuses to 'pending' and clears locks.

This is intentionally conservative and schema-aware: it only updates columns that exist.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import sqlite3
from typing import Iterable


DEFAULT_DB = "data/companies.db"


def _existing_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _backup_db(db_path: str, backup_dir: str) -> str:
    os.makedirs(backup_dir, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(db_path).replace(".db", "")
    dst = os.path.join(backup_dir, f"{base}-before-reset-{ts}.db")
    shutil.copy2(db_path, dst)
    return dst


def _reset_columns(*, wipe_input_address: bool) -> Iterable[tuple[str, object]]:
    # NOTE: some columns are optional depending on migration history; we check existence.
    # Input fields (optional)
    if wipe_input_address:
        # address は company_name と UNIQUE 制約になっているDBがあるため、空文字ではなく NULL にする
        # （SQLite の UNIQUE は複数NULLを許容する）
        yield ("address", None)
        yield ("csv_address", None)

    # Scraped core
    yield ("homepage", "")
    yield ("phone", "")
    yield ("found_address", "")
    yield ("rep_name", "")
    yield ("description", "")
    yield ("listing", "")
    yield ("revenue", "")
    yield ("profit", "")
    yield ("capital", "")
    yield ("fiscal_month", "")
    yield ("founded_year", "")

    # Decision/flags
    yield ("error_code", "")
    yield ("homepage_official_flag", 0)
    yield ("homepage_official_source", "")
    yield ("homepage_official_score", 0.0)
    yield ("official_homepage", "")
    yield ("alt_homepage", "")
    yield ("alt_homepage_type", "")
    yield ("provisional_homepage", "")
    yield ("provisional_reason", "")
    yield ("final_homepage", "")

    # Provenance
    yield ("phone_source", "")
    yield ("address_source", "")
    yield ("source_url_phone", "")
    yield ("source_url_address", "")
    yield ("source_url_rep", "")
    yield ("last_checked_at", "")

    # AI
    yield ("ai_used", 0)
    yield ("ai_model", "")
    yield ("extract_confidence", 0.0)
    yield ("ai_confidence", 0.0)
    yield ("ai_reason", "")
    yield ("address_confidence", 0.0)
    yield ("address_evidence", "")
    yield ("description_evidence", "")

    # Address review fields
    yield ("address_conflict_level", "")
    yield ("address_review_reason", "")

    # Deep crawl / debug
    yield ("deep_pages_visited", 0)
    yield ("deep_fetch_count", 0)
    yield ("deep_fetch_failures", 0)
    yield ("deep_skip_reason", "")
    yield ("deep_urls_visited", "[]")
    yield ("deep_phone_candidates", 0)
    yield ("deep_address_candidates", 0)
    yield ("deep_rep_candidates", 0)
    yield ("deep_enabled", 0)
    yield ("deep_stop_reason", "")
    yield ("timeout_stage", "")
    yield ("page_type_per_url", "")
    yield ("extracted_candidates_count", "")
    yield ("drop_reasons", "")
    yield ("top3_urls", "")
    yield ("exclude_reasons", "")
    yield ("skip_reason", "")
    yield ("pref_match", 0)
    yield ("city_match", 0)

    # Enrichment
    yield ("industry", "")
    yield ("business_tags", "")
    yield ("license", "")
    yield ("employees", "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--backup-dir", default="backups")
    ap.add_argument("--no-backup", action="store_true", default=False)
    ap.add_argument("--dry-run", action="store_true", default=False)
    ap.add_argument("--table", default="companies")
    ap.add_argument(
        "--wipe-input-address",
        action="store_true",
        default=False,
        help="Also clear input address columns (address/csv_address). This may reduce matching accuracy.",
    )
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise FileNotFoundError(args.db)

    if not args.no_backup and not args.dry_run:
        backup_path = _backup_db(args.db, args.backup_dir)
        print(f"Backup created: {backup_path}")

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA busy_timeout=15000;")
    try:
        cols = _existing_columns(con, args.table)
        if "id" not in cols:
            raise RuntimeError(f"Table '{args.table}' not found or has no id column.")

        sets: list[str] = []
        params: list[object] = []

        for col, val in _reset_columns(wipe_input_address=bool(args.wipe_input_address)):
            if col in cols:
                sets.append(f"{col}=?")
                params.append(val)

        # Always reset status + locks if present
        if "status" in cols:
            sets.append("status=?")
            params.append("pending")
        if "locked_by" in cols:
            sets.append("locked_by=NULL")
        if "locked_at" in cols:
            sets.append("locked_at=NULL")

        if not sets:
            print("No matching columns to reset; nothing to do.")
            return 0

        sql = f"UPDATE {args.table} SET " + ", ".join(sets)

        if args.dry_run:
            n = con.execute(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
            print(f"[DRY-RUN] Would run: {sql}")
            print(f"[DRY-RUN] Rows affected: {n}")
            return 0

        cur = con.execute("BEGIN")
        try:
            cur = con.execute(sql, params)
            con.commit()
        except Exception:
            con.rollback()
            raise
        print(f"Reset complete. Rows updated: {cur.rowcount}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
