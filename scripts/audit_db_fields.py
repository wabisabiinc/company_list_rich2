#!/usr/bin/env python3
"""
All-field audit for the companies SQLite DB (no network, no AI).

Goal:
- Detect obviously misplaced / malformed values across *all columns*.
- Output a long-format CSV: one row per (company, field, issue).

This is conservative: it flags suspicious patterns; it does not "fix" values.
Use alongside evidence-based fact_check_db.py and/or cleaning scripts.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Optional


PREFECTURES = (
    "北海道",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県",
)

PREF_RE = re.compile("|".join(re.escape(p) for p in PREFECTURES))
ZIP_RE = re.compile(r"(?:〒\s*)?\d{3}[-‐―－ー]?\d{4}")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:TEL|電話|☎|℡)?\s*(0\d{1,4})[-‐―－ー]?\d{1,4}[-‐―－ー]?\d{3,4}", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<\s*(?:script|style|div|span|p|a|br|li|ul|ol|table|tr|td)\b", re.IGNORECASE)
JS_NOISE_RE = re.compile(r"(window\.\w+|dataLayer\s*=|googletagmanager|function\s*\()", re.IGNORECASE)


def _norm(v: Any) -> str:
    if v is None:
        return ""
    s = unicodedata.normalize("NFKC", str(v))
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _extract_pref(s: str) -> str:
    m = PREF_RE.search(s or "")
    return m.group(0) if m else ""


def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None


@dataclass
class Issue:
    field: str
    code: str
    severity: str
    message: str


def _common_text_issues(field: str, s: str) -> list[Issue]:
    issues: list[Issue] = []
    if not s:
        return issues
    if len(s) >= 600:
        issues.append(Issue(field, "too_long", "medium", f"length={len(s)}"))
    if HTML_TAG_RE.search(s) or JS_NOISE_RE.search(s):
        issues.append(Issue(field, "html_or_js_noise", "high", "looks like html/js content"))
    if "\ufffd" in s:
        issues.append(Issue(field, "mojibake_replacement_char", "medium", "contains U+FFFD"))
    return issues


def _audit_url_field(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if not s.startswith(("http://", "https://")):
        issues.append(Issue(field, "url_not_http", "high", "not http(s) URL"))
    if " " in s:
        issues.append(Issue(field, "url_contains_space", "high", "space in URL"))
    return issues


def _audit_phone(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if URL_RE.search(s) or EMAIL_RE.search(s):
        issues.append(Issue(field, "phone_contains_url_or_email", "high", "url/email mixed"))
    digits = _digits_only(s)
    if len(digits) < 9:
        issues.append(Issue(field, "phone_too_short", "high", f"digits={len(digits)}"))
    if not PHONE_RE.search(s):
        issues.append(Issue(field, "phone_pattern_mismatch", "medium", "not matching common JP phone pattern"))
    return issues


def _audit_address(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if URL_RE.search(s) or EMAIL_RE.search(s):
        issues.append(Issue(field, "address_contains_url_or_email", "high", "url/email mixed"))
    # phone-in-address (avoid ZIP false positives by requiring >=10 digits)
    if PHONE_RE.search(s):
        digits = _digits_only(PHONE_RE.search(s).group(0))  # type: ignore[union-attr]
        if len(digits) >= 10:
            issues.append(Issue(field, "address_contains_phone", "high", "phone number mixed"))
    if not _extract_pref(s) and not ZIP_RE.search(s):
        # allow short/city-only addresses but flag
        issues.append(Issue(field, "address_no_pref_or_zip", "medium", "missing prefecture and ZIP"))
    # multiple zips implies multiple locations or garbage
    if len(ZIP_RE.findall(s)) >= 2:
        issues.append(Issue(field, "address_multiple_zips", "high", "multiple ZIP codes"))
    # common UI junk tokens
    for bad in ("コンテンツへスキップ", "プライバシ", "cookie", "All Rights Reserved", "Copyright", "メニュー", "資料ダウンロード"):
        if bad.lower() in s.lower():
            issues.append(Issue(field, "address_contains_ui_junk", "high", f"contains '{bad}'"))
            break
    return issues


def _audit_rep_name(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if URL_RE.search(s) or EMAIL_RE.search(s) or PHONE_RE.search(s) or ZIP_RE.search(s) or PREF_RE.search(s):
        issues.append(Issue(field, "rep_contains_contact_or_address", "high", "looks like contact/address"))
    # role labels / non-name tokens
    for bad in ("代表取締役", "取締役", "社長", "会長", "代表者名", "代表者", "口コミ件数", "受付時間", "免許", "資格"):
        if bad in s and len(s) <= 20:
            issues.append(Issue(field, "rep_label_or_non_name", "medium", f"contains '{bad}'"))
            break
    if len(s) >= 80:
        issues.append(Issue(field, "rep_too_long", "medium", f"length={len(s)}"))
    if re.search(r"\d", s):
        issues.append(Issue(field, "rep_contains_digits", "medium", "digits in rep_name"))
    return issues


def _audit_numericish(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if URL_RE.search(s) or EMAIL_RE.search(s):
        issues.append(Issue(field, "numeric_contains_url_or_email", "high", "url/email mixed"))
    # Allow kanji units but require some digits.
    if not re.search(r"[0-9０-９]", s):
        issues.append(Issue(field, "numeric_no_digits", "medium", "no digits found"))
    if len(s) >= 80:
        issues.append(Issue(field, "numeric_too_long", "medium", f"length={len(s)}"))
    return issues


def _audit_json_field(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    parsed = _safe_json_loads(s)
    if parsed is None:
        issues.append(Issue(field, "json_parse_failed", "high", "invalid JSON"))
    return issues


def _audit_enum(field: str, s: str, allowed: set[str]) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    if s not in allowed:
        issues.append(Issue(field, "enum_unknown", "medium", f"unexpected value '{s}'"))
    return issues


def _audit_corporate_number(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    digits = _digits_only(s)
    if len(digits) != 13:
        issues.append(Issue(field, "corp_no_not_13_digits", "high", f"digits={len(digits)}"))
    return issues


def _audit_fiscal_month(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    m = re.search(r"(\d{1,2})", s)
    if not m:
        issues.append(Issue(field, "fiscal_month_unparseable", "medium", "no month digits"))
        return issues
    month = int(m.group(1))
    if not (1 <= month <= 12):
        issues.append(Issue(field, "fiscal_month_out_of_range", "high", f"month={month}"))
    return issues


def _audit_year(field: str, s: str) -> list[Issue]:
    issues = _common_text_issues(field, s)
    if not s:
        return issues
    m = re.search(r"(19\d{2}|20\d{2})", s)
    if not m:
        issues.append(Issue(field, "year_unparseable", "medium", "no 4-digit year"))
        return issues
    y = int(m.group(1))
    if not (1800 <= y <= 2100):
        issues.append(Issue(field, "year_out_of_range", "high", f"year={y}"))
    return issues


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--table", default="companies")
    ap.add_argument("--status", default="", help="comma-separated statuses to include")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default="", help="default: /tmp/field_audit_<db>_<ts>.csv")
    ap.add_argument("--min-severity", default="low", choices=["low", "medium", "high"])
    ap.add_argument("--progress-every", type=int, default=0, help="print progress to stderr every N rows (0=off)")
    args = ap.parse_args(argv)

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 2

    ts = time.strftime("%Y%m%d_%H%M%S")
    if not args.out:
        base = os.path.basename(db_path).replace(".db", "")
        args.out = os.path.join("/tmp", f"field_audit_{base}_{ts}.csv")

    min_sev_rank = {"low": 0, "medium": 1, "high": 2}[args.min_severity]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({args.table})").fetchall()]
    colset = set(cols)

    where = []
    params: list[Any] = []
    if args.status.strip() and "status" in colset:
        statuses = [s.strip() for s in args.status.split(",") if s.strip()]
        where.append("status IN (%s)" % ",".join("?" for _ in statuses))
        params.extend(statuses)

    sql = f"SELECT * FROM {args.table}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id"
    if args.limit and args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    # Field groupings (apply if column exists)
    url_fields = {
        f
        for f in (
            "homepage",
            "official_homepage",
            "final_homepage",
            "alt_homepage",
            "reference_homepage",
            "provisional_homepage",
            "source_url_phone",
            "source_url_address",
            "source_url_rep",
        )
        if f in colset
    }
    phone_fields = {f for f in ("phone", "reference_phone") if f in colset}
    address_fields = {f for f in ("address", "found_address", "csv_address", "reference_address") if f in colset}
    rep_fields = {f for f in ("rep_name",) if f in colset}
    json_fields = {
        f
        for f in (
            "deep_urls_visited",
            "top3_urls",
            "exclude_reasons",
            "drop_reasons",
            "description_evidence",
            "business_tags",
            "page_type_per_url",
        )
        if f in colset
    }
    corp_fields = {f for f in ("corporate_number", "corporate_number_norm") if f in colset}

    numeric_fields = {f for f in ("employee_count", "revenue", "profit", "capital", "employees") if f in colset}
    enum_status_allowed = {"pending", "running", "done", "review", "no_homepage", "skipped", "error", "retry"}

    out_path = args.out
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["id", "company_name", "field", "value", "severity", "issue_code", "message"],
        )
        w.writeheader()

        rows_scanned = 0
        issues_written = 0

        for row in cur.execute(sql, params):
            rows_scanned += 1
            rowd = dict(row)
            cid = rowd.get("id")
            cname = _norm(rowd.get("company_name"))

            for field in cols:
                if field in ("id", "company_name"):
                    continue
                s = _norm(rowd.get(field))
                issues: list[Issue] = []

                if field in url_fields:
                    issues.extend(_audit_url_field(field, s))
                elif field in phone_fields:
                    issues.extend(_audit_phone(field, s))
                elif field in address_fields:
                    issues.extend(_audit_address(field, s))
                elif field in rep_fields:
                    issues.extend(_audit_rep_name(field, s))
                elif field in corp_fields:
                    issues.extend(_audit_corporate_number(field, s))
                elif field == "fiscal_month":
                    issues.extend(_audit_fiscal_month(field, s))
                elif field == "founded_year":
                    issues.extend(_audit_year(field, s))
                elif field == "status":
                    issues.extend(_audit_enum(field, s, enum_status_allowed))
                elif field in json_fields:
                    issues.extend(_audit_json_field(field, s))
                elif field in numeric_fields:
                    issues.extend(_audit_numericish(field, s))
                else:
                    issues.extend(_common_text_issues(field, s))
                    # Generic "misplaced URL/phone/email" detector for non-url fields.
                    if s and field not in url_fields and field not in json_fields and URL_RE.search(s):
                        issues.append(Issue(field, "unexpected_url_in_field", "high", "url appears in non-url field"))
                    if (
                        s
                        and field not in phone_fields
                        and field not in {"hubspot_id", "corporate_number", "corporate_number_norm"}
                        and field not in numeric_fields
                        and PHONE_RE.search(s)
                        and field not in address_fields
                    ):
                        issues.append(Issue(field, "unexpected_phone_in_field", "high", "phone appears in non-phone field"))
                    if s and field not in ("ai_model", "source_csv") and EMAIL_RE.search(s):
                        issues.append(Issue(field, "unexpected_email_in_field", "high", "email appears in field"))

                for iss in issues:
                    if {"low": 0, "medium": 1, "high": 2}[iss.severity] < min_sev_rank:
                        continue
                    w.writerow(
                        {
                            "id": cid,
                            "company_name": cname,
                            "field": iss.field,
                            "value": s,
                            "severity": iss.severity,
                            "issue_code": iss.code,
                            "message": iss.message,
                        }
                    )
                    issues_written += 1

            if args.progress_every and rows_scanned % int(args.progress_every) == 0:
                print(
                    f"[audit] rows_scanned={rows_scanned} issues_written={issues_written}",
                    file=sys.stderr,
                    flush=True,
                )

        print(
            json.dumps(
                {"db": db_path, "out": out_path, "rows_scanned": rows_scanned, "issues_written": issues_written},
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
