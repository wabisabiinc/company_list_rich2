"""
done になった行のうち主要フィールドが欠損しているものを pending に戻す補助スクリプト。
- デフォルト対象: phone / found_address / rep_name が空の行
- status は既定で done のみ（必要なら引数で変更）
"""

import argparse
from typing import List

from src.database_manager import DatabaseManager


FIELD_COLUMN_MAP = {
    "phone": "phone",
    "address": "found_address",
    "rep": "rep_name",
    "description": "description",
}


def parse_fields(raw: str) -> List[str]:
    fields: List[str] = []
    for part in (raw or "").split(","):
        name = part.strip().lower()
        if not name:
            continue
        if name not in FIELD_COLUMN_MAP:
            raise ValueError(f"Unknown field: {name} (choose from {', '.join(FIELD_COLUMN_MAP)})")
        fields.append(name)
    return fields


def main() -> None:
    parser = argparse.ArgumentParser(description="Requeue done rows that lack required fields.")
    parser.add_argument("--db", default="data/companies.db", help="Path to the SQLite DB")
    parser.add_argument("--fields", default="phone,address,rep", help="Comma-separated fields to require")
    parser.add_argument("--statuses", default="done", help="Comma-separated statuses to requeue (default: done)")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for rows to update (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Only list rows without updating status")
    args = parser.parse_args()

    fields = parse_fields(args.fields)
    statuses = [s.strip().lower() for s in (args.statuses or "").split(",") if s.strip()]
    if not statuses:
        raise ValueError("At least one status must be specified")

    manager = DatabaseManager(db_path=args.db)
    try:
        conn = manager.conn
        conn.row_factory = manager.conn.row_factory
        placeholders = ",".join("?" for _ in statuses)
        status_clause = f"status IN ({placeholders})"
        conditions = [f"({FIELD_COLUMN_MAP[f]} IS NULL OR {FIELD_COLUMN_MAP[f]}='')" for f in fields]
        where_clause = f"{status_clause} AND ({' OR '.join(conditions)})"
        sql = (
            "SELECT id, company_name, status, homepage, phone, found_address, rep_name "
            f"FROM companies WHERE {where_clause} ORDER BY id"
        )
        if args.limit and args.limit > 0:
            sql += f" LIMIT {int(args.limit)}"

        cur = conn.cursor()
        rows = cur.execute(sql, statuses).fetchall()
        if not rows:
            print("No matching rows found.")
            return

        print(f"Found {len(rows)} rows to requeue (statuses={statuses}, fields={fields})")
        for row in rows:
            print(f"- id={row['id']} status={row['status']} homepage={row['homepage']}")
            if not args.dry_run:
                manager.update_status(int(row["id"]), "pending")

        if not args.dry_run:
            print(f"Requeued {len(rows)} rows to pending.")
    finally:
        manager.close()


if __name__ == "__main__":
    main()
