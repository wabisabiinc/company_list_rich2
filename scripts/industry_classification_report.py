#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from typing import Iterable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.text_normalizer import norm_text_compact

IT_HINTS = ["ai", "生成ai", "dx", "ec", "saas", "it", "ict", "iot", "llm", "デジタル", "クラウド", "webサービス"]
IT_HINTS_NORM = [norm_text_compact(t) for t in IT_HINTS if norm_text_compact(t)]


def _iter_tag_values(business_tags: str) -> list[str]:
    text = (business_tags or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return [p.strip() for p in re.split(r"[\n,、/|]+", text) if p.strip()]


def _is_unclassified(source: str, minor_code: str, minor_name: str) -> bool:
    s = str(source or "").strip().lower()
    mcode = str(minor_code or "").strip()
    mname = str(minor_name or "").strip()
    if (not s) or ("unclassified" in s):
        return True
    if not mcode:
        return True
    if mname in {"", "不明"}:
        return True
    return False


def _contains_it_hint(texts: Iterable[str]) -> bool:
    joined = norm_text_compact("\n".join([str(t or "") for t in texts]))
    if not joined:
        return False
    for hint in IT_HINTS_NORM:
        if hint in joined:
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Industry classification health report")
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    total = int(cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0] or 0)

    rows = list(
        cur.execute(
            """
            SELECT id, description, business_tags,
                   industry_class_source,
                   industry_major_code, industry_major,
                   industry_middle_code, industry_middle,
                   industry_minor_code, industry_minor
            FROM companies
            """
        ).fetchall()
    )

    source_counts: dict[str, int] = {}
    unclassified_count = 0
    classified_count = 0
    major_only_count = 0
    it_hint_unclassified = 0

    for row in rows:
        source = str(row["industry_class_source"] or "").strip() or "(empty)"
        source_counts[source] = source_counts.get(source, 0) + 1

        major_code = str(row["industry_major_code"] or "").strip()
        major_name = str(row["industry_major"] or "").strip()
        middle_code = str(row["industry_middle_code"] or "").strip()
        middle_name = str(row["industry_middle"] or "").strip()
        minor_code = str(row["industry_minor_code"] or "").strip()
        minor_name = str(row["industry_minor"] or "").strip()

        is_unclassified = _is_unclassified(source, minor_code, minor_name)
        if is_unclassified:
            unclassified_count += 1
        else:
            classified_count += 1

        has_major = bool(major_code) and major_name not in {"", "不明"}
        has_middle = bool(middle_code) and middle_name not in {"", "不明"}
        has_minor = bool(minor_code) and minor_name not in {"", "不明"}
        if has_major and (not has_middle) and (not has_minor):
            major_only_count += 1

        if is_unclassified:
            desc = str(row["description"] or "")
            tags = _iter_tag_values(str(row["business_tags"] or ""))
            if _contains_it_hint([desc] + tags):
                it_hint_unclassified += 1

    classified_rate = (classified_count / total * 100.0) if total else 0.0

    print("=== Industry Classification Report ===")
    print(f"db: {args.db}")
    print(f"総件数: {total}")
    print(f"classified件数: {classified_count} ({classified_rate:.1f}%)")
    print(f"unclassified件数: {unclassified_count}")
    print(f"majorのみ件数: {major_only_count}")
    print(f"AI/DX/EC/SaaS/IT語あり未分類件数: {it_hint_unclassified}")

    print("\n--- industry_class_source別件数 ---")
    for source, count in sorted(source_counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"{source}\t{count}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
