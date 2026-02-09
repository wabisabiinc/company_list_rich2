#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from typing import Any

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.ai_verifier import AIVerifier, AI_ENABLED, GEN_IMPORT_ERROR
from src.industry_classifier import IndustryClassifier


def _iter_blocks(
    company_name: str,
    industry: str,
    description: str,
    business_tags: str,
    license_text: str,
    description_evidence: str,
) -> list[str]:
    blocks: list[str] = []
    if company_name:
        blocks.append(company_name)
    if industry:
        blocks.append(industry)
    if description:
        blocks.append(description)
    if license_text:
        blocks.append(f"許認可: {license_text}"[:400])
    if description_evidence:
        blocks.append(f"根拠: {description_evidence}"[:400])
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
        SELECT id, company_name, industry, description, business_tags, license, description_evidence
        FROM companies
        {where}
        ORDER BY id
    """
    if args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    rows = cur.execute(sql).fetchall()
    total = len(rows)
    print(f"target rows: {total}")

    def _ai_accepts(ai_res: dict[str, Any], final_candidate: dict[str, str]) -> bool:
        try:
            conf = float(ai_res.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        if conf < args.min_confidence:
            return False
        if str(ai_res.get("human_review") or "").strip().lower() in {"true", "1", "yes"}:
            return False
        regulated_majors = {
            "医療，福祉",
            "建設業",
            "金融業，保険業",
        }
        major_name = (final_candidate.get("major_name") or "").strip()
        if major_name in regulated_majors:
            facts = ai_res.get("facts")
            license_info = ""
            if isinstance(facts, dict):
                license_info = str(
                    facts.get("licenses")
                    or facts.get("license")
                    or facts.get("license_or_registration")
                    or ""
                ).strip()
            if not license_info:
                return False
        return True

    async def _pick_ai_candidate(
        company_name: str,
        text: str,
        candidates: list[dict[str, str]],
        level: str,
        major_code: str = "",
        middle_code: str = "",
    ) -> tuple[dict[str, str] | None, dict[str, Any] | None]:
        if not candidates:
            return None, None
        if level == "major":
            cand_key_map = {c.get("major_code"): c for c in candidates}
        elif level == "middle":
            cand_key_map = {(c.get("major_code"), c.get("middle_code")): c for c in candidates}
        else:
            cand_key_map = {(c.get("major_code"), c.get("middle_code"), c.get("minor_code")): c for c in candidates}

        ai_res = await verifier.judge_industry(
            text=text,
            company_name=company_name or "",
            candidates_text=cls.format_candidates_text(candidates),
        )
        if not isinstance(ai_res, dict):
            return None, None
        maj = str(ai_res.get("major_code") or "").strip() or major_code
        mid = str(ai_res.get("middle_code") or "").strip() or middle_code
        mino = str(ai_res.get("minor_code") or "").strip()
        if level == "major":
            key = maj
        elif level == "middle":
            key = (maj, mid)
        else:
            key = (maj, mid, mino)
        return cand_key_map.get(key), ai_res

    updated = 0
    for cid, company_name, industry, description, business_tags, license_text, description_evidence in rows:
        blocks = _iter_blocks(
            company_name or "",
            industry or "",
            description or "",
            business_tags or "",
            license_text or "",
            description_evidence or "",
        )
        text = "\n".join(blocks)
        scores = cls.score_levels(blocks)
        major_candidates = cls.build_level_candidates("major", scores, top_n=args.top_n)
        if not major_candidates:
            continue
        picked_major, _ = await _pick_ai_candidate(
            company_name or "",
            text,
            major_candidates,
            "major",
        )
        if not picked_major:
            continue
        major_code_val = picked_major.get("major_code", "")

        middle_candidates = cls.build_level_candidates("middle", scores, top_n=args.top_n, major_code=major_code_val)
        if not middle_candidates:
            continue
        picked_middle, _ = await _pick_ai_candidate(
            company_name or "",
            text,
            middle_candidates,
            "middle",
            major_code=major_code_val,
        )
        if not picked_middle:
            continue
        middle_code_val = picked_middle.get("middle_code", "")

        minor_candidates = cls.build_level_candidates(
            "minor",
            scores,
            top_n=args.top_n,
            major_code=major_code_val,
            middle_code=middle_code_val,
        )
        if not minor_candidates:
            continue
        picked_minor, minor_ai_res = await _pick_ai_candidate(
            company_name or "",
            text,
            minor_candidates,
            "minor",
            major_code=major_code_val,
            middle_code=middle_code_val,
        )
        if not picked_minor or not isinstance(minor_ai_res, dict):
            continue

        final_candidate = picked_minor
        final_ai_res = minor_ai_res
        if scores.get("use_detail"):
            detail_candidates = cls.build_level_candidates(
                "detail",
                scores,
                top_n=args.top_n,
                major_code=major_code_val,
                middle_code=middle_code_val,
                minor_code=picked_minor.get("minor_code", ""),
            )
            if detail_candidates:
                picked_detail, detail_ai_res = await _pick_ai_candidate(
                    company_name or "",
                    text,
                    detail_candidates,
                    "detail",
                    major_code=major_code_val,
                    middle_code=middle_code_val,
                )
                if picked_detail and isinstance(detail_ai_res, dict):
                    final_candidate = picked_detail
                    final_ai_res = detail_ai_res

        if not _ai_accepts(final_ai_res, final_candidate):
            continue
        try:
            conf = float(final_ai_res.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        conf = max(0.0, min(1.0, conf))
        maj = str(final_ai_res.get("major_code") or "").strip()
        mid = str(final_ai_res.get("middle_code") or "").strip()
        mino = str(final_ai_res.get("minor_code") or "").strip()
        if not (maj and mid and mino):
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
        if not minor_item_code and minor_code and minor_name:
            # Fallback: populate minor_item with the resolved minor.
            minor_item_code = minor_code
            minor_item_name = minor_name

        cur.execute(
            """
            UPDATE companies
            SET industry=?,
                industry_major_code=?, industry_major=?,
                industry_middle_code=?, industry_middle=?,
                industry_minor_code=?, industry_minor=?,
                industry_minor_item_code=?, industry_minor_item=?,
                industry_class_source=?, industry_class_confidence=?
            WHERE id=?
            """,
            (
                (minor_item_name or minor_name or middle_name or major_name),
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
    ap.add_argument("--min-confidence", type=float, default=float(os.getenv("INDUSTRY_AI_MIN_CONFIDENCE", "0.55")))
    ap.add_argument("--sleep-ms", type=int, default=0, help="sleep between calls (ms)")
    args = ap.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
