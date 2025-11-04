# scripts/export_to_csv.py
import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

DB_DEFAULT  = Path("data/companies.db")
OUT_DEFAULT = Path("data/output.csv")

def stream_rows(cur: sqlite3.Cursor, arraysize: int = 5000) -> Iterable[Sequence]:
    """大きな結果を分割取得してストリーミング出力"""
    while True:
        chunk = cur.fetchmany(arraysize)
        if not chunk:
            break
        for row in chunk:
            yield row

def main():
    ap = argparse.ArgumentParser(description="Export companies to CSV")
    ap.add_argument("--db", type=Path, default=DB_DEFAULT, help="SQLite DB path")
    ap.add_argument("--out", type=Path, default=OUT_DEFAULT, help="Output CSV path")
    ap.add_argument("--only-done", action="store_true",
                    help="Export only rows with status='done'")
    ap.add_argument("--dedupe-name-addr", action="store_true",
                    help="同一(会社名,住所)の重複があれば最新idだけを出力する")
    ap.add_argument("--chunk", type=int, default=5000, help="fetchmanyのチャンクサイズ")
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    # 読み取り専用接続（ロック競合回避）
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    cur  = conn.cursor()

    where = "WHERE status='done'" if args.only_done else ""

    if args.dedupe_name_addr:
        # 会社名+住所でパーティション → 最新id(rn=1)のみを採用
        query = f"""
        WITH ranked AS (
          SELECT
            id, company_name, address, employee_count,
            homepage, phone, found_address, status,
            ROW_NUMBER() OVER (
              PARTITION BY company_name, address
              ORDER BY id DESC
            ) AS rn
          FROM companies
          {where}
        )
        SELECT
          id, company_name, address, employee_count,
          COALESCE(homepage,'' ) AS homepage,
          COALESCE(phone,''   ) AS phone,
          COALESCE(found_address,'') AS found_address
        FROM ranked
        WHERE rn = 1
        ORDER BY id;
        """
    else:
        query = f"""
        SELECT
          id, company_name, address, employee_count,
          COALESCE(homepage,'' ) AS homepage,
          COALESCE(phone,''   ) AS phone,
          COALESCE(found_address,'') AS found_address
        FROM companies
        {where}
        ORDER BY id;
        """

    cur.execute(query)
    headers = [d[0] for d in cur.description]

    # Excel 互換のため UTF-8-SIG
    with args.out.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        count = 0
        for row in stream_rows(cur, arraysize=args.chunk):
            writer.writerow(row)
            count += 1

    conn.close()
    print(f"[DONE] wrote {count} rows to {args.out}")

if __name__ == "__main__":
    main()
