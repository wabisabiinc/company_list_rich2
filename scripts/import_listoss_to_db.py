# scripts/import_listoss_to_db.py
import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Optional

DB_PATH_DEFAULT = Path("data/companies.db")
HEADER_SIGN = "会社名"  # Listossのヘッダ開始マーカー

def ensure_table(conn: sqlite3.Connection, reset: bool = False) -> None:
    cur = conn.cursor()
    if reset:
        cur.executescript("""
            DROP TABLE IF EXISTS companies;
            DROP INDEX  IF EXISTS uniq_company;
            DROP INDEX  IF EXISTS idx_companies_status;
            DROP INDEX  IF EXISTS idx_companies_emp;
            DROP INDEX  IF EXISTS idx_companies_corporate_number_norm;
            DROP INDEX  IF EXISTS idx_companies_hubspot_id;
        """)
    # AUTO INCREMENT + UNIQUE(company_name,address)
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name   TEXT    NOT NULL,
            address        TEXT    NOT NULL,
            employee_count INTEGER,
            homepage       TEXT,
            phone          TEXT,
            found_address  TEXT,
            status         TEXT    DEFAULT 'pending',
            locked_by      TEXT,
            locked_at      TEXT,
            rep_name       TEXT,
            description    TEXT,
            listing        TEXT,
            revenue        TEXT,
            profit         TEXT,
            capital        TEXT,
            fiscal_month   TEXT,
            founded_year   TEXT,
            ai_used        INTEGER DEFAULT 0,
            ai_model       TEXT,
            phone_source   TEXT,
            address_source TEXT,
            extract_confidence REAL,
            last_checked_at TEXT,
            hubspot_id     TEXT,
            corporate_number     TEXT,
            corporate_number_norm TEXT,
            source_csv     TEXT,
            reference_homepage TEXT,
            reference_phone TEXT,
            reference_address TEXT,
            accuracy_homepage TEXT,
            accuracy_phone TEXT,
            accuracy_address TEXT,
            homepage_official_flag INTEGER,
            homepage_official_source TEXT,
            homepage_official_score REAL,
            source_url_phone TEXT,
            source_url_address TEXT,
            source_url_rep TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_company
          ON companies(company_name, address);
        CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);
        CREATE INDEX IF NOT EXISTS idx_companies_emp ON companies(employee_count);
        CREATE INDEX IF NOT EXISTS idx_companies_corporate_number_norm ON companies(corporate_number_norm);
        CREATE INDEX IF NOT EXISTS idx_companies_hubspot_id ON companies(hubspot_id);
    """)
    conn.commit()

def iter_listoss_rows(csv_path: Path) -> Iterable[list]:
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip() == HEADER_SIGN:
                break
        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue
            yield row

def build_company_record(row: list) -> Tuple[str, str]:
    name = (row[0] or "").strip()
    pref = (row[2] or "").strip() if len(row) > 2 else ""
    city = (row[3] or "").strip() if len(row) > 3 else ""
    addr = (row[4] or "").strip() if len(row) > 4 else ""
    full_address = f"{pref}{city}{addr}".strip()
    return name, full_address

def import_file(
    conn: sqlite3.Connection,
    csv_path: Path,
    limit: Optional[int] = None,
) -> int:
    cur = conn.cursor()
    inserted = 0
    for row in iter_listoss_rows(csv_path):
        name, full_address = build_company_record(row)
        if not name:
            continue
        # UNIQUE(name,address) により重複は自動的に無視される
        cur.execute(
            """
            INSERT OR IGNORE INTO companies
              (company_name, address, employee_count, status)
            VALUES
              (?, ?, NULL, 'pending')
            """,
            (name, full_address),
        )
        if cur.rowcount == 1:
            inserted += 1
        if inserted % 1000 == 0:
            conn.commit()
        if limit is not None and inserted >= limit:
            break
    conn.commit()
    return inserted

def main():
    ap = argparse.ArgumentParser(description="Import Listoss CSVs into companies.db")
    ap.add_argument("--db", type=Path, default=DB_PATH_DEFAULT, help="SQLite DB path")
    ap.add_argument("--reset", action="store_true", help="Drop & recreate table first")
    ap.add_argument("--limit", type=int, default=None, help="Max rows per run")
    ap.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="CSV files (Listoss format). Default: data/listoss_merged_data_part{1,2}.csv",
    )
    args = ap.parse_args()

    default_candidates = [
        Path("data/listoss_merged_data_part1.csv"),
        Path("data/listoss_merged_data_part2.csv"),
    ]
    files = args.files if args.files else [p for p in default_candidates if p.exists()]
    if not files:
        raise SystemExit("CSVが見つかりません。files 引数でCSVを指定してください。")

    args.db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)
    try:
        ensure_table(conn, reset=args.reset)
        total = 0
        for csv_path in files:
            if not csv_path.exists():
                print(f"[WARN] CSV not found: {csv_path}")
                continue
            print(f"[INFO] Importing: {csv_path}")
            added = import_file(conn, csv_path, limit=args.limit)
            print(f"[INFO]  -> inserted (new): {added}")
            total += added
        print(f"[DONE] total newly inserted: {total}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
