#!/usr/bin/env python3
"""
found_address にモジバケ(ã/�など)が含まれるレコードを pending に戻し、
found_address を空にして再取得できるようにするワンショットスクリプト。
"""
import sqlite3
import argparse

DEFAULT_DB = "data/companies.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA busy_timeout=15000;")
    cur = con.cursor()
    cur.execute(
        "SELECT id FROM companies WHERE found_address LIKE '%ã%' OR found_address LIKE '%�%'"
    )
    ids = [row[0] for row in cur.fetchall()]
    if not ids:
        print("No mojibake records found.")
        return
    print(f"Found {len(ids)} records. Requeuing...")
    cur.execute(
        f"UPDATE companies SET status='pending', found_address='', locked_by=NULL, locked_at=NULL "
        f"WHERE id IN ({','.join('?' for _ in ids)})",
        ids,
    )
    con.commit()
    print("Done.")


if __name__ == "__main__":
    main()
