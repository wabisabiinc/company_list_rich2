#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from typing import Any

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.industry_classifier import IndustryClassifier
from src.text_normalizer import norm_text

TOKEN_RE = re.compile(r"[一-龥ぁ-んァ-ンa-z0-9]{2,32}")
STOPWORDS = {
    "株式会社", "有限会社", "当社", "弊社", "サービス", "事業", "会社", "企業", "提供", "運営",
    "開発", "支援", "対応", "管理", "関連", "について", "および", "その他", "公式", "情報",
}


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
    parts = re.split(r"[\n,、/|]+", text)
    return [p.strip() for p in parts if p.strip()]


def _is_unclassified(row: sqlite3.Row) -> bool:
    source = str(row["industry_class_source"] or "").strip().lower()
    minor_code = str(row["industry_minor_code"] or "").strip()
    minor_name = str(row["industry_minor"] or "").strip()
    if not source or "unclassified" in source:
        return True
    if not minor_code:
        return True
    if minor_name in {"", "不明"}:
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract candidate alias terms from unclassified rows.")
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--out", default="industry_alias_candidates.csv")
    ap.add_argument("--min-freq", type=int, default=2)
    ap.add_argument("--limit", type=int, default=0, help="Limit unclassified rows to scan (0=all)")
    args = ap.parse_args()

    cls = IndustryClassifier(os.getenv("JSIC_CSV_PATH") or "docs/industry_select.csv")

    known_terms: set[str] = set()
    for alias in cls.alias_entries:
        if alias.alias_norm:
            known_terms.add(alias.alias_norm)
    known_terms.update(cls.taxonomy.normalized_name_index.keys())

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    sql = """
        SELECT id, description, business_tags, industry_class_source, industry_minor_code, industry_minor
        FROM companies
        ORDER BY id
    """
    if args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    rows = list(cur.execute(sql).fetchall())

    counter: Counter[str] = Counter()
    samples: dict[str, tuple[int, str]] = {}

    for row in rows:
        if not _is_unclassified(row):
            continue

        cid = int(row["id"])
        desc = str(row["description"] or "")
        tags = _iter_tag_values(str(row["business_tags"] or ""))
        combined_raw = "\n".join([desc] + tags)
        combined = norm_text(combined_raw)
        if not combined:
            continue

        for m in TOKEN_RE.finditer(combined):
            term = m.group(0)
            if term in STOPWORDS:
                continue
            if term.isdigit():
                continue
            if term in known_terms:
                continue
            if len(term) < 2:
                continue
            counter[term] += 1
            if term not in samples:
                samples[term] = (cid, combined[:200])

    ranked = sorted(counter.items(), key=lambda x: (-x[1], x[0]))

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["term", "freq", "sample_company_id", "sample_text"])
        for term, freq in ranked:
            if freq < max(1, int(args.min_freq)):
                continue
            sample_cid, sample_text = samples.get(term, (0, ""))
            writer.writerow([term, freq, sample_cid, sample_text])

    print(f"rows_scanned={len(rows)} candidates={len(ranked)} out={args.out}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
