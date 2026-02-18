#!/usr/bin/env python3
import argparse
import os
import sqlite3

MIN_COLS = [
    "company_name",
    "address",
    "status",
    "locked_by",
    "locked_at",
    "error_code",
    "homepage",
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
    "final_homepage",
    "contact_url",
    "business_tags",
    "industry_top",
    "industry_sub",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    company_name   TEXT,
    address        TEXT,
    status         TEXT DEFAULT 'pending',
    locked_by      TEXT,
    locked_at      TEXT,
    error_code     TEXT,
    homepage       TEXT,
    phone          TEXT,
    found_address  TEXT,
    rep_name       TEXT,
    description    TEXT,
    listing        TEXT,
    revenue        TEXT,
    profit         TEXT,
    capital        TEXT,
    fiscal_month   TEXT,
    founded_year   TEXT,
    final_homepage TEXT,
    contact_url    TEXT,
    business_tags  TEXT,
    industry_top   TEXT,
    industry_sub   TEXT
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
