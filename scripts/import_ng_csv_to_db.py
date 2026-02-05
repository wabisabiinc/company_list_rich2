#!/usr/bin/env python3
import argparse
import csv
import os
import re
import sqlite3
from typing import Dict, Any, Iterable

from src.database_manager import DatabaseManager


def _normalize_corp_no(val: str) -> str:
    digits = re.sub(r"\D", "", val or "")
    return digits if len(digits) == 13 else digits


def _compose_address(row: Dict[str, Any]) -> str:
    pref = (row.get("都道府県／地域") or row.get("都道府県") or "").strip()
    zipcode = (row.get("郵便番号") or "").strip().replace("〒", "")
    city = (row.get("市区町村") or "").strip()
    b1 = (row.get("番地") or "").strip()
    b2 = (row.get("番地2") or "").strip()
    body = "".join([city, b1, b2])
    if zipcode and body:
        return f"〒{zipcode} {pref}{body}".strip()
    return f"{pref}{body}".strip()


def iter_rows(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            company_name = (row.get("会社名") or "").strip()
            if not company_name:
                continue
            address = _compose_address(row)
            employee_count = (row.get("従業員数") or "").strip()
            try:
                employee_count_val = int(employee_count) if employee_count else None
            except Exception:
                employee_count_val = None
            homepage = (row.get("ウェブサイトURL") or "").strip()
            phone = (row.get("電話番号") or row.get("電話番号(2)") or "").strip()
            corp_no = (row.get("法人番号（名寄せ）") or "").strip()
            corp_no_norm = _normalize_corp_no(corp_no)
            industry = (row.get("業種") or "").strip()

            yield {
                "company_name": company_name,
                "address": address,
                "employee_count": employee_count_val,
                "homepage": homepage,
                "phone": phone,
                "corporate_number": corp_no,
                "corporate_number_norm": corp_no_norm,
                "industry": industry,
            }


def bulk_insert(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]], source_csv: str) -> int:
    cols = (
        "company_name",
        "address",
        "employee_count",
        "homepage",
        "phone",
        "corporate_number",
        "corporate_number_norm",
        "industry",
        "source_csv",
        "status",
    )
    sql = f"INSERT OR IGNORE INTO companies ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
    buf = []
    inserted = 0
    cur = conn.cursor()
    for item in rows:
        buf.append(
            (
                item.get("company_name"),
                item.get("address"),
                item.get("employee_count"),
                item.get("homepage"),
                item.get("phone"),
                item.get("corporate_number"),
                item.get("corporate_number_norm"),
                item.get("industry"),
                source_csv,
                "pending",
            )
        )
        if len(buf) >= 5000:
            cur.executemany(sql, buf)
            inserted += cur.rowcount
            conn.commit()
            buf.clear()
    if buf:
        cur.executemany(sql, buf)
        inserted += cur.rowcount
        conn.commit()
    return inserted


def main() -> int:
    ap = argparse.ArgumentParser(description="Import NG CSV into companies DB (minimal fields).")
    ap.add_argument("--csv", required=True, help="Input CSV path")
    ap.add_argument("--db", required=True, help="SQLite DB path")
    ap.add_argument("--drop", action="store_true", help="Drop DB before import")
    args = ap.parse_args()

    if args.drop:
        for ext in ("", "-wal", "-shm"):
            p = args.db + ext
            if os.path.exists(p):
                os.remove(p)

    manager = DatabaseManager(db_path=args.db)
    conn = manager.conn
    source_csv = os.path.basename(args.csv)
    inserted = bulk_insert(conn, iter_rows(args.csv), source_csv=source_csv)
    print(f"imported rows: {inserted}")
    manager.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
