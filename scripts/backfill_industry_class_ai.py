#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import unicodedata
from typing import Any

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.ai_verifier import AIVerifier, AI_ENABLED, GEN_IMPORT_ERROR
from src.industry_classifier import IndustryClassifier


def _normalize(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "")
    return "".join(s.split())


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


def _fallback_candidates(cls: IndustryClassifier, text: str, limit: int) -> list[dict[str, str]]:
    norm = _normalize(text)
    if not norm:
        return []
    taxonomy = cls.taxonomy
    matched_minors: set[str] = set()
    matched_middles: set[str] = set()
    matched_majors: set[str] = set()

    for code, name in taxonomy.minor_names.items():
        if _normalize(name) in norm or norm in _normalize(name):
            matched_minors.add(code)
    for code, name in taxonomy.middle_names.items():
        if _normalize(name) in norm or norm in _normalize(name):
            matched_middles.add(code)
    for code, name in taxonomy.major_names.items():
        if _normalize(name) in norm or norm in _normalize(name):
            matched_majors.add(code)

    # expand middle/major to minors
    for minor_code, middle_code in taxonomy.minor_to_middle.items():
        if middle_code in matched_middles:
            matched_minors.add(minor_code)
    for middle_code, major_code in taxonomy.middle_to_major.items():
        if major_code in matched_majors:
            for minor_code, mid in taxonomy.minor_to_middle.items():
                if mid == middle_code:
                    matched_minors.add(minor_code)

    out: list[dict[str, str]] = []
    for minor_code in list(matched_minors)[: max(1, limit)]:
        major_code, major_name, middle_code, middle_name, minor_code, minor_name = taxonomy.resolve_hierarchy(minor_code)
        if not (major_code and middle_code and minor_code):
            continue
        out.append(
            {
                "major_code": major_code,
                "major_name": major_name,
                "middle_code": middle_code,
                "middle_name": middle_name,
                "minor_code": minor_code,
                "minor_name": minor_name,
            }
        )
    return out


def _build_candidates_from_industry_name(
    cls: IndustryClassifier,
    industry_text: str,
    top_n: int,
) -> list[dict[str, str]]:
    norm = _normalize(industry_text)
    if not norm:
        return []
    # split tokens by common separators
    seps = "・,、/()（）-〜～~"
    tokens: list[str] = []
    buf = ""
    for ch in norm:
        if ch in seps:
            if buf:
                tokens.append(buf)
                buf = ""
        else:
            buf += ch
    if buf:
        tokens.append(buf)
    tokens = [t for t in tokens if len(t) >= 2]

    use_detail = bool(cls.taxonomy.detail_names)
    scores: list[tuple[int, str]] = []
    source = cls.taxonomy.detail_names if use_detail else cls.taxonomy.minor_names
    for code, name in source.items():
        name_norm = _normalize(name)
        score = 0
        if not name_norm:
            continue
        if norm in name_norm or name_norm in norm:
            score += 5
        for tok in tokens:
            if tok in name_norm:
                score += 1
        if score > 0:
            scores.append((score, minor_code))
    if not scores:
        return []

    scores.sort(key=lambda x: (-x[0], x[1]))
    out: list[dict[str, str]] = []
    for _, code in scores[: max(1, top_n)]:
        if use_detail:
            major_code, major_name, middle_code, middle_name, minor_code, minor_name, detail_code, detail_name = (
                cls.taxonomy.resolve_detail_hierarchy(code)
            )
            if not (major_code and middle_code and detail_code):
                continue
            out.append(
                {
                    "major_code": major_code,
                    "major_name": major_name,
                    "middle_code": middle_code,
                    "middle_name": middle_name,
                    "minor_code": detail_code,
                    "minor_name": detail_name,
                }
            )
        else:
            major_code, major_name, middle_code, middle_name, minor_code, minor_name = cls.taxonomy.resolve_hierarchy(code)
            if not (major_code and middle_code and minor_code):
                continue
            out.append(
                {
                    "major_code": major_code,
                    "major_name": major_name,
                    "middle_code": middle_code,
                    "middle_name": middle_name,
                    "minor_code": minor_code,
                    "minor_name": minor_name,
                }
            )
    return out


async def _run(args: argparse.Namespace) -> int:
    if not AI_ENABLED:
        print("AI not enabled. Check GEMINI_API_KEY and install google-generativeai.")
        if GEN_IMPORT_ERROR:
            print(f"AI import error: {GEN_IMPORT_ERROR}")
        return 2

    cls = IndustryClassifier(os.getenv("JSIC_CSV_PATH") or "docs/industry_select.csv")
    if not cls.loaded:
        print("JSIC taxonomy not loaded. Check JSIC_CSV_PATH.")
        return 2

    verifier = AIVerifier()
    if not verifier.model or not verifier.industry_prompt:
        print("AIVerifier not ready (model/prompt missing).")
        return 2

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    # ensure new columns exist
    cols = {row[1] for row in cur.execute("PRAGMA table_info(companies)")}
    if "industry_minor_item_code" not in cols:
        cur.execute("ALTER TABLE companies ADD COLUMN industry_minor_item_code TEXT;")
    if "industry_minor_item" not in cols:
        cur.execute("ALTER TABLE companies ADD COLUMN industry_minor_item TEXT;")
    con.commit()

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
        text = "\n".join(blocks)
        # Prefer matching by confirmed industry name (ignore codes)
        candidates = _build_candidates_from_industry_name(cls, industry or "", top_n=args.top_n)
        if not candidates:
            candidates = cls.build_ai_candidates(blocks, top_n=args.top_n)
        if not candidates:
            candidates = _fallback_candidates(cls, text, limit=max(args.top_n, 20))
        if not candidates:
            continue

        ai_res = await verifier.judge_industry(
            text=text,
            company_name=company_name or "",
            candidates_text=cls.format_candidates_text(candidates),
        )
        if not isinstance(ai_res, dict):
            continue
        maj = str(ai_res.get("major_code") or "").strip()
        mid = str(ai_res.get("middle_code") or "").strip()
        mino = str(ai_res.get("minor_code") or "").strip()
        conf = float(ai_res.get("confidence") or 0.0)
        if conf < args.min_confidence:
            continue

        # validate candidate exists
        key = {(c.get("major_code"), c.get("middle_code"), c.get("minor_code")) for c in candidates}
        if (maj, mid, mino) not in key:
            continue

        # resolve names (detail code -> minor representative)
        minor_item_code = ""
        minor_item_name = ""
        if mino in cls.taxonomy.detail_names:
            major_code, major_name, middle_code, middle_name, minor_code, minor_name, detail_code, detail_name = (
                cls.taxonomy.resolve_detail_hierarchy(mino)
            )
            minor_item_code = detail_code
            minor_item_name = detail_name
        else:
            major_code, major_name, middle_code, middle_name, minor_code, minor_name = cls.taxonomy.resolve_hierarchy(mino)
        if not (major_code and middle_code and minor_code):
            continue

        cur.execute(
            """
            UPDATE companies
            SET industry_major_code=?, industry_major=?,
                industry_middle_code=?, industry_middle=?,
                industry_minor_code=?, industry_minor=?,
                industry_minor_item_code=?, industry_minor_item=?,
                industry_class_source=?, industry_class_confidence=?
            WHERE id=?
            """,
            (
                major_code,
                major_name,
                middle_code,
                middle_name,
                minor_code,
                minor_name,
                minor_item_code,
                minor_item_name,
                "ai",
                conf,
                cid,
            ),
        )
        updated += 1
        if updated % 100 == 0:
            con.commit()
            print(f"updated {updated}/{total}")

        if args.sleep_ms > 0:
            await asyncio.sleep(args.sleep_ms / 1000.0)

    con.commit()
    con.close()
    print(f"done. updated={updated}/{total}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill industry major/middle/minor using AI.")
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true", help="overwrite existing industry classification")
    ap.add_argument("--top-n", type=int, default=int(os.getenv("INDUSTRY_AI_TOP_N", "12")))
    ap.add_argument("--min-confidence", type=float, default=float(os.getenv("AI_MIN_CONFIDENCE", "0.5")))
    ap.add_argument("--sleep-ms", type=int, default=0, help="sleep between calls (ms)")
    args = ap.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
