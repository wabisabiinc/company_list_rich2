"""
既存DBの値を再正規化するクリーニングスクリプト。
- 電話/住所/代表者/description/金額系/決算月/設立年/上場区分/ホームページを
  既存の正規化ロジックで再計算して上書きする。
- 既存値と変化があるものだけUPDATEするため、時間影響は最小。
"""

import argparse
import sqlite3
from typing import Dict, Any

from main import (
    normalize_phone,
    normalize_address,
    clean_description_value,
    clean_amount_value,
    clean_listing_value,
    clean_fiscal_month,
    clean_founded_year,
    looks_like_address,
)
from src.company_scraper import CompanyScraper
from src.database_manager import DatabaseManager


def normalize_homepage(val: str | None) -> str:
    if not val:
        return ""
    try:
        return CompanyScraper.normalize_homepage_url(val)
    except Exception:
        return val.strip()


def clean_row(row: sqlite3.Row, scraper: CompanyScraper) -> Dict[str, Any]:
    """
    1レコードを正規化し、変化があれば更新用dictを返す。
    変化が無ければ空dict。
    """
    updates: Dict[str, Any] = {}

    raw_hp = row["homepage"] or ""
    hp = normalize_homepage(raw_hp).strip()
    if hp != (raw_hp or ""):
        updates["homepage"] = hp

    raw_phone = row["phone"] or ""
    phone = normalize_phone(raw_phone) or ""
    if phone != (raw_phone or ""):
        updates["phone"] = phone

    raw_addr = row["found_address"] or ""
    addr_norm = normalize_address(raw_addr) or ""
    if addr_norm and not looks_like_address(addr_norm):
        addr_norm = ""
    if addr_norm != (raw_addr or ""):
        updates["found_address"] = addr_norm

    raw_rep = row["rep_name"] or ""
    rep = scraper.clean_rep_name(raw_rep) or ""
    if rep != (raw_rep or ""):
        updates["rep_name"] = rep

    raw_desc = row["description"] or ""
    desc = clean_description_value(raw_desc) or ""
    if desc != (raw_desc or ""):
        updates["description"] = desc

    raw_listing = row["listing"] or ""
    listing = clean_listing_value(raw_listing) or ""
    if listing != (raw_listing or ""):
        updates["listing"] = listing

    for field in ("capital", "revenue", "profit"):
        raw_val = row[field] or ""
        cleaned = clean_amount_value(raw_val) or ""
        if cleaned != (raw_val or ""):
            updates[field] = cleaned

    raw_fiscal = row["fiscal_month"] or ""
    fiscal = clean_fiscal_month(raw_fiscal) or ""
    if fiscal != (raw_fiscal or ""):
        updates["fiscal_month"] = fiscal

    raw_founded = row["founded_year"] or ""
    founded = clean_founded_year(raw_founded) or ""
    if founded != (raw_founded or ""):
        updates["founded_year"] = founded

    return updates


def main(db_path: str, dry_run: bool, limit: int | None) -> None:
    manager = DatabaseManager(db_path=db_path)
    cur = manager.conn.cursor()
    scraper = CompanyScraper()
    rows = cur.execute(
        "SELECT id, homepage, phone, found_address, rep_name, description, listing, "
        "capital, revenue, profit, fiscal_month, founded_year FROM companies "
        "ORDER BY id ASC" + (f" LIMIT {limit}" if limit else "")
    ).fetchall()

    updated = 0
    for row in rows:
        changes = clean_row(row, scraper)
        if not changes:
            continue
        updated += 1
        if dry_run:
            print(f"[DRY-RUN] id={row['id']} updates={changes}")
            continue
        placeholders = ", ".join(f"{k}=?" for k in changes.keys())
        values = list(changes.values()) + [row["id"]]
        cur.execute(f"UPDATE companies SET {placeholders} WHERE id=?", values)
    if not dry_run:
        manager.conn.commit()
    print(f"checked={len(rows)} updated={updated} dry_run={dry_run}")
    manager.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean existing company records.")
    parser.add_argument("--db-path", default="data/companies.db", help="Path to SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for sanity checks")
    args = parser.parse_args()
    main(args.db_path, args.dry_run, args.limit)
