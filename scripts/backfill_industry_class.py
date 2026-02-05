#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from src.industry_classifier import IndustryClassifier


def _iter_blocks(
    company_name: str,
    industry: str,
    description: str,
    business_tags: str,
) -> list[str]:
    blocks: list[str] = []
    if company_name:
        blocks.append(company_name)
    if industry:
        blocks.append(industry)
    if description:
        blocks.append(description)
    if business_tags:
        try:
            if business_tags.strip().startswith("["):
                tags = json.loads(business_tags)
                if isinstance(tags, list):
                    blocks.extend([str(t) for t in tags if t])
                else:
                    blocks.append(business_tags)
            else:
                blocks.append(business_tags)
        except Exception:
            blocks.append(business_tags)
    return [b for b in blocks if b]


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill industry major/middle/minor in DB.")
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--min-score", type=int, default=int(os.getenv("INDUSTRY_RULE_MIN_SCORE", "1")))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true", help="overwrite existing industry classification")
    args = ap.parse_args()

    cls = IndustryClassifier(os.getenv("JSIC_CSV_PATH") or "docs/industry_select.csv")
    if not cls.loaded:
        print("JSIC taxonomy not loaded. Check JSIC_CSV_PATH.")
        return 2

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    where = "" if args.force else "WHERE (industry_major IS NULL OR TRIM(industry_major)='')"
    sql = f"""
        SELECT id, company_name, industry, description, business_tags
        FROM companies
        {where}
        ORDER BY id
    """
    if args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    rows = cur.execute(sql).fetchall()
    total = len(rows)
    print(f"target rows: {total}")

    updated = 0
    for cid, company_name, industry, description, business_tags in rows:
        blocks = _iter_blocks(
            company_name or "",
            industry or "",
            description or "",
            business_tags or "",
        )
        res = cls.rule_classify(blocks, min_score=args.min_score)
        if not res:
            continue
        cur.execute(
            """
            UPDATE companies
            SET industry_major_code=?, industry_major=?,
                industry_middle_code=?, industry_middle=?,
                industry_minor_code=?, industry_minor=?,
                industry_class_source=?, industry_class_confidence=?
            WHERE id=?
            """,
            (
                res.get("major_code", ""),
                res.get("major_name", ""),
                res.get("middle_code", ""),
                res.get("middle_name", ""),
                res.get("minor_code", ""),
                res.get("minor_name", ""),
                res.get("source", "rule"),
                float(res.get("confidence") or 0.0),
                cid,
            ),
        )
        updated += 1
        if updated % 1000 == 0:
            con.commit()
            print(f"updated {updated}/{total}")

    con.commit()
    con.close()
    print(f"done. updated={updated}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
