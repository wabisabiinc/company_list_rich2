#!/usr/bin/env python3
import argparse
import os
import sqlite3

MIN_COLS = [
    "company_name",
    "address",
    "final_homepage",
    "phone",
    "found_address",
    "rep_name",
    "description",
    "listing",
    "revenue",
    "profit",
    "capital",
    "fiscal_month",
    "founded_year",
    "employee_count",
    "employees",
    "industry_major",
    "industry_middle",
    "industry_minor",
    "contact_url",
    "ai_used",
    "status",
]

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    {', '.join([c + ' TEXT' for c in MIN_COLS])}
);
"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Export minimal columns into a new DB.")
    ap.add_argument("--src", required=True, help="Source DB path")
    ap.add_argument("--dst", required=True, help="Destination DB path")
    ap.add_argument("--drop", action="store_true", help="Drop destination DB if exists")
    args = ap.parse_args()

    if args.drop:
        for ext in ("", "-wal", "-shm"):
            p = args.dst + ext
            if os.path.exists(p):
                os.remove(p)

    src = sqlite3.connect(args.src)
    src.row_factory = sqlite3.Row
    dst = sqlite3.connect(args.dst)

    dst.executescript(SCHEMA)
    cols = ["id", *MIN_COLS]
    src_cols = {row[1] for row in src.execute("PRAGMA table_info(companies)")}
    select_cols = [c for c in cols if c in src_cols]
    if "id" not in select_cols:
        select_cols = ["rowid", *select_cols]

    rows = src.execute(f"SELECT {', '.join(select_cols)} FROM companies").fetchall()
    placeholders = ",".join(["?"] * len(select_cols))
    dst.executemany(
        f"INSERT INTO companies ({', '.join(select_cols)}) VALUES ({placeholders})",
        [tuple(r[c] for c in select_cols) for r in rows],
    )
    dst.commit()
    src.close()
    dst.close()
    print(f"exported rows: {len(rows)} -> {args.dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
