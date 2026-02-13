#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.text_normalizer import norm_text_compact


def parse_allowed_major_codes(raw: str) -> tuple[str, ...]:
    text = str(raw or "").strip().upper()
    if not text:
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[\s,|/]+", text):
        code = token.strip().upper()
        if not code:
            continue
        if not re.fullmatch(r"[A-Z]", code):
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return tuple(sorted(out))


def load_taxonomy_minor_major(path: str) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    major_names: dict[str, str] = {}
    middle_to_major: dict[str, str] = {}
    minor_to_major: dict[str, str] = {}

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            major = str(row.get("大分類コード") or "").strip()
            middle = str(row.get("中分類コード") or "").strip()
            minor = str(row.get("小分類コード") or "").strip()
            detail = str(row.get("細分類コード") or "").strip()
            name = str(row.get("項目名") or "").strip()

            if middle == "00" and minor == "000" and detail == "0000" and major and name:
                major_names[major] = name
            elif minor == "000" and detail == "0000" and middle and major:
                middle_to_major[middle] = major
            elif minor != "000" and detail == "0000" and minor:
                major_from_middle = middle_to_major.get(middle, major)
                if major_from_middle:
                    minor_to_major[minor] = major_from_middle

    return major_names, middle_to_major, minor_to_major


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate industry_aliases.csv integrity")
    ap.add_argument("--aliases", default="industry_aliases.csv")
    ap.add_argument("--taxonomy", default="docs/industry_select.csv")
    ap.add_argument(
        "--fail-on-domain-outlier",
        action="store_true",
        help="Return non-zero when a domain_tag has cross-major outliers without requires_review=1",
    )
    args = ap.parse_args()

    major_names, _middle_to_major, minor_to_major = load_taxonomy_minor_major(args.taxonomy)

    required = {"alias", "target_minor_code", "priority", "requires_review"}
    errors: list[str] = []
    warnings: list[str] = []

    rows: list[dict[str, str]] = []
    with open(args.aliases, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        if not required.issubset(fieldnames):
            missing = sorted(list(required - fieldnames))
            errors.append(f"missing required columns: {', '.join(missing)}")
        for line_no, row in enumerate(reader, start=2):
            rec = {k: str(v or "").strip() for k, v in row.items()}
            rec["_line"] = str(line_no)
            rows.append(rec)

    alias_norm_to_codes: dict[str, set[str]] = defaultdict(set)
    domain_rows: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in rows:
        line_no = row["_line"]
        alias = row.get("alias", "")
        code = row.get("target_minor_code", "")
        requires_review_raw = (row.get("requires_review", "0") or "0").strip().lower()
        requires_review = requires_review_raw in {"1", "true", "yes"}
        domain_tag = row.get("domain_tag") or row.get("notes") or ""
        allowed_major_codes = parse_allowed_major_codes(row.get("allowed_major_codes", ""))

        if not alias:
            errors.append(f"line {line_no}: alias is empty")

        if not code:
            errors.append(f"line {line_no}: target_minor_code is empty")
            continue

        major_code = minor_to_major.get(code)
        if not major_code:
            errors.append(f"line {line_no}: unknown target_minor_code={code}")
            continue

        if not allowed_major_codes:
            warnings.append(f"line {line_no}: allowed_major_codes is empty (alias={alias}, code={code})")
        elif major_code not in allowed_major_codes:
            allowed_txt = "|".join(allowed_major_codes)
            errors.append(
                f"line {line_no}: target major mismatch alias={alias} code={code} major={major_code} allowed={allowed_txt}"
            )

        alias_norm = norm_text_compact(alias)
        if alias_norm:
            alias_norm_to_codes[alias_norm].add(code)

        row["_major_code"] = major_code
        row["_requires_review_bool"] = "1" if requires_review else "0"
        row["_domain_tag"] = domain_tag
        if domain_tag:
            domain_rows[domain_tag].append(row)

    for alias_norm, codes in sorted(alias_norm_to_codes.items()):
        if len(codes) <= 1:
            continue
        errors.append(f"normalized alias conflict: alias_norm={alias_norm} codes={','.join(sorted(codes))}")

    domain_outlier_msgs: list[str] = []
    for domain_tag, items in sorted(domain_rows.items()):
        majors = [str(it.get("_major_code") or "") for it in items if str(it.get("_major_code") or "")]
        unique = sorted({m for m in majors if m})
        if len(unique) <= 1:
            continue
        cnt = Counter(majors)
        dominant_major, _ = cnt.most_common(1)[0]
        for it in items:
            major = str(it.get("_major_code") or "")
            if not major or major == dominant_major:
                continue
            if str(it.get("_requires_review_bool") or "0") == "1":
                continue
            line_no = it.get("_line", "?")
            alias = it.get("alias", "")
            major_name = major_names.get(major, "")
            domain_outlier_msgs.append(
                f"line {line_no}: domain_tag outlier without review alias={alias} domain_tag={domain_tag} major={major}:{major_name}"
            )

    if domain_outlier_msgs:
        target = errors if args.fail_on_domain_outlier else warnings
        target.extend(domain_outlier_msgs)

    print("=== industry_aliases validation ===")
    print(f"aliases: {args.aliases}")
    print(f"taxonomy: {args.taxonomy}")
    print(f"rows: {len(rows)}")
    print(f"errors: {len(errors)}")
    print(f"warnings: {len(warnings)}")

    if errors:
        print("\n[ERRORS]")
        for msg in errors:
            print(f"- {msg}")

    if warnings:
        print("\n[WARNINGS]")
        for msg in warnings:
            print(f"- {msg}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
