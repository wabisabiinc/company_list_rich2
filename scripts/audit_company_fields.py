#!/usr/bin/env python3
"""
Audit extracted company fields (representative name / address) in SQLite DB and
export suspicious rows to a CSV for review.

Design:
- Fast, cheap heuristic checks first (no network).
- Optional Gemini-based verification only for flagged rows (cost-controlled).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Optional

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore


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
PHONE_CAND_RE = re.compile(r"(?:TEL|電話|☎|℡)?\s*(0\d{1,4})[-‐―－ー]?\d{1,4}[-‐―－ー]?\d{3,4}", re.IGNORECASE)

REP_TITLES = (
    "代表取締役", "取締役社長", "代表者", "社長", "会長", "CEO", "COO", "CFO", "CTO",
    "代表", "取締役", "執行役員", "社長執行役員",
)

COMPANY_SUFFIX = (
    "株式会社", "有限会社", "合同会社", "(株)", "（株）", "㈱", "(有)", "（有）",
    "Inc", "Ltd", "LLC", "Co.", "Corporation",
)


def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _has_japanese_letters(s: str) -> bool:
    return bool(re.search(r"[ぁ-んァ-ン一-龥]", s))


def _extract_pref(s: str) -> str:
    m = PREF_RE.search(s or "")
    return m.group(0) if m else ""


def _contains_phone(text: str) -> bool:
    if not text:
        return False
    for m in PHONE_CAND_RE.finditer(text):
        cand = m.group(0) or ""
        digits = re.sub(r"\D+", "", cand)
        # Avoid false positives like "06-0035" from ZIP (short digit sequence).
        if 10 <= len(digits) <= 12 and digits.startswith("0"):
            return True
    return False


def _looks_like_label_only(rep: str) -> bool:
    if not rep:
        return False
    rep = rep.strip()
    if rep in REP_TITLES:
        return True
    return bool(re.fullmatch(r"(?:代表者名|代表者|代表取締役|取締役社長|社長|会長|CEO|COO|CFO|CTO)", rep))


def _rep_issues(rep: str) -> list[str]:
    issues: list[str] = []
    if not rep:
        return ["rep_missing"]
    if URL_RE.search(rep):
        issues.append("rep_contains_url")
    if EMAIL_RE.search(rep):
        issues.append("rep_contains_email")
    if _contains_phone(rep):
        issues.append("rep_contains_phone")
    if ZIP_RE.search(rep) or PREF_RE.search(rep):
        issues.append("rep_looks_like_address")
    if any(k in rep for k in COMPANY_SUFFIX):
        issues.append("rep_contains_company_suffix")
    if _looks_like_label_only(rep):
        issues.append("rep_label_only")
    if "お問い合わせ" in rep or "問合せ" in rep or "問い合わせ" in rep:
        issues.append("rep_contains_contact_words")
    if any(ch in rep for ch in ("。", "：", ":")) and len(rep) >= 24:
        issues.append("rep_sentence_like")
    digit_count = sum(1 for ch in rep if ch.isdigit())
    if digit_count >= 4:
        issues.append("rep_contains_many_digits")
    if len(rep) >= 60:
        issues.append("rep_too_long")
    if not _has_japanese_letters(rep) and len(rep) >= 10:
        issues.append("rep_non_japanese_long")
    return issues


def _addr_issues(addr: str) -> list[str]:
    issues: list[str] = []
    if not addr:
        return ["address_missing"]
    if URL_RE.search(addr):
        issues.append("address_contains_url")
    if EMAIL_RE.search(addr):
        issues.append("address_contains_email")
    if _contains_phone(addr):
        issues.append("address_contains_phone")
    if any(k in addr for k in REP_TITLES):
        issues.append("address_contains_rep_title")
    if not _extract_pref(addr) and not ZIP_RE.search(addr):
        issues.append("address_no_prefecture_or_zip")
    if len(addr) <= 6:
        issues.append("address_too_short")
    if len(addr) >= 140:
        issues.append("address_too_long")
    # Person-name-like fragment in address field
    if not any(ch.isdigit() for ch in addr) and not _extract_pref(addr) and _has_japanese_letters(addr) and len(addr) <= 12:
        issues.append("address_looks_like_name")
    return issues


def _issue_score(issues: Iterable[str]) -> int:
    weights = {
        "rep_missing": 1,
        "rep_label_only": 3,
        "rep_contains_url": 4,
        "rep_contains_email": 4,
        "rep_contains_phone": 4,
        "rep_looks_like_address": 4,
        "rep_contains_company_suffix": 3,
        "rep_contains_contact_words": 2,
        "rep_sentence_like": 2,
        "rep_contains_many_digits": 2,
        "rep_too_long": 2,
        "rep_non_japanese_long": 1,
        "address_missing": 1,
        "address_contains_url": 4,
        "address_contains_email": 4,
        "address_contains_phone": 4,
        "address_contains_rep_title": 3,
        "address_no_prefecture_or_zip": 3,
        "address_too_short": 2,
        "address_too_long": 2,
        "address_looks_like_name": 2,
    }
    return int(sum(weights.get(x, 1) for x in issues))


def _risk_label(score: int) -> str:
    if score >= 8:
        return "high"
    if score >= 4:
        return "medium"
    if score >= 1:
        return "low"
    return "ok"


def _extract_first_json(text: str) -> Any:
    if not text:
        return None
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    # Fast path: try parsing entire string.
    try:
        return json.loads(text)
    except Exception:
        pass

    stripped = text.lstrip()
    prefer_array = stripped.startswith("[")

    def scan_array(t: str) -> Any:
        bracket_stack = 0
        start_idx = -1
        for i, ch in enumerate(t):
            if ch == "[":
                if bracket_stack == 0:
                    start_idx = i
                bracket_stack += 1
            elif ch == "]":
                bracket_stack -= 1
                if bracket_stack == 0 and start_idx != -1:
                    candidate = t[start_idx : i + 1]
                    try:
                        return json.loads(candidate)
                    except Exception:
                        pass
        return None

    def scan_object(t: str) -> Any:
        brace_stack = 0
        start_idx = -1
        for i, ch in enumerate(t):
            if ch == "{":
                if brace_stack == 0:
                    start_idx = i
                brace_stack += 1
            elif ch == "}":
                brace_stack -= 1
                if brace_stack == 0 and start_idx != -1:
                    candidate = t[start_idx : i + 1]
                    try:
                        return json.loads(candidate)
                    except Exception:
                        pass
        return None

    if prefer_array:
        got = scan_array(text)
        if got is not None:
            return got
        return scan_object(text)

    got = scan_object(text)
    if got is not None:
        return got
    return scan_array(text)


@dataclass
class AiResult:
    rep_ok: Optional[bool] = None
    addr_ok: Optional[bool] = None
    rep_reason: str = ""
    addr_reason: str = ""
    confidence: float = 0.0


def _ai_available() -> tuple[bool, str, str]:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    model = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash-lite").strip()
    if not api_key:
        return False, "", model
    try:
        import google.generativeai as generativeai  # type: ignore
    except Exception:
        return False, "", model
    try:
        generativeai.configure(api_key=api_key)  # type: ignore
    except Exception:
        return False, "", model
    return True, api_key, model


def _resp_text(resp: Any) -> str:
    t = getattr(resp, "text", None)
    if isinstance(t, str) and t.strip():
        return t
    try:
        for cand in getattr(resp, "candidates", []) or []:
            parts = getattr(getattr(cand, "content", None), "parts", []) or []
            for p in parts:
                pt = getattr(p, "text", None)
                if isinstance(pt, str) and pt.strip():
                    return pt
    except Exception:
        pass
    return str(resp)


def _ai_judge_batch(
    rows: list[dict[str, Any]],
    *,
    model_name: str,
    timeout_sec: float,
    debug_out: str = "",
) -> list[AiResult]:
    import google.generativeai as generativeai  # type: ignore

    payload = []
    for r in rows:
        payload.append(
            {
                "id": r.get("id"),
                "company_name": r.get("company_name") or "",
                "rep_name": r.get("rep_name") or "",
                "address": r.get("address") or "",
            }
        )

    prompt = (
        "あなたは日本企業データの品質監査担当です。\n"
        "次の各レコードについて、rep_name（代表者名）と address（住所）がその欄にふさわしいかを判定してください。\n"
        "推測は禁止。判断材料が不足なら null。\n"
        "rep_name_ok: 人名として妥当なら true。住所/電話/URL/会社名/文/ラベルだけ等なら false。\n"
        "address_ok: 日本の住所として妥当なら true。人名/電話/URL/問い合わせ文等が混入なら false。\n"
        "出力は JSON 配列のみ（入力と同じ順序、同じ件数）。前後に説明文を付けない。\n"
        '各要素は {"id": number, "rep_name_ok": true|false|null, "address_ok": true|false|null, "rep_reason": string, "addr_reason": string, "confidence": number}。\n'
        "理由は短く。\n"
        "# INPUT\n"
        + json.dumps(payload, ensure_ascii=False)
    )

    m = generativeai.GenerativeModel(model_name)  # type: ignore
    if timeout_sec and timeout_sec > 0:
        resp = m.generate_content(prompt, request_options={"timeout": timeout_sec})  # type: ignore
    else:
        resp = m.generate_content(prompt)  # type: ignore
    text = _resp_text(resp)
    parsed = _extract_first_json(text)
    if not isinstance(parsed, list):
        if debug_out:
            try:
                os.makedirs(os.path.dirname(debug_out) or ".", exist_ok=True)
                with open(debug_out, "a", encoding="utf-8") as f:
                    f.write("\n=== AI RAW RESPONSE ===\n")
                    f.write(text[:20000])
                    f.write("\n")
            except Exception:
                pass
        return [AiResult() for _ in rows]

    results: list[AiResult] = []
    for item in parsed:
        if not isinstance(item, dict):
            results.append(AiResult())
            continue
        rep_ok = item.get("rep_name_ok")
        addr_ok = item.get("address_ok")
        if not isinstance(rep_ok, bool):
            rep_ok = None
        if not isinstance(addr_ok, bool):
            addr_ok = None
        rep_reason = _norm(item.get("rep_reason"))[:160]
        addr_reason = _norm(item.get("addr_reason"))[:160]
        try:
            conf = float(item.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        results.append(AiResult(rep_ok=rep_ok, addr_ok=addr_ok, rep_reason=rep_reason, addr_reason=addr_reason, confidence=conf))

    if len(results) < len(rows):
        results.extend([AiResult() for _ in range(len(rows) - len(results))])
    return results[: len(rows)]


def _default_out_path() -> str:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join("reports", f"field_audit_{ts}.csv")


def main(argv: list[str]) -> int:
    if load_dotenv is not None:
        try:
            load_dotenv()
        except Exception:
            pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--table", default="companies")
    ap.add_argument("--status", default="", help="comma-separated statuses to include (e.g. done,review)")
    ap.add_argument("--limit", type=int, default=0, help="max rows to scan (0=all)")
    ap.add_argument("--out", default=_default_out_path())
    ap.add_argument("--suspect-threshold", type=int, default=4, help="minimum score to output when --only-suspects=true")
    ap.add_argument("--only-suspects", action="store_true", default=True)
    ap.add_argument("--include-all", dest="only_suspects", action="store_false", help="output all rows (large)")
    ap.add_argument("--ai", action="store_true", default=False, help="use Gemini for flagged rows (costs)")
    ap.add_argument("--ai-max", type=int, default=200, help="max rows to send to AI")
    ap.add_argument("--ai-batch-size", type=int, default=20)
    ap.add_argument("--ai-timeout-sec", type=float, default=float(os.getenv("AI_CALL_TIMEOUT_SEC", "20") or 0))
    ap.add_argument("--ai-debug-out", default="", help="append raw AI responses to this file when parsing fails")
    args = ap.parse_args(argv)

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Discover columns (DB variants differ)
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({args.table})").fetchall()]
    colset = set(cols)

    def pick(*names: str) -> str:
        for n in names:
            if n in colset:
                return n
        return ""

    col_csv_addr = pick("csv_address")
    col_source_rep = pick("source_url_rep")
    col_source_addr = pick("source_url_address")

    where = []
    params: list[Any] = []
    if args.status.strip():
        statuses = [s.strip() for s in args.status.split(",") if s.strip()]
        where.append("status IN (%s)" % ",".join("?" for _ in statuses))
        params.extend(statuses)

    sql = f"SELECT * FROM {args.table}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id"
    if args.limit and args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    enable_ai = bool(args.ai)
    ai_ok, _, ai_model = _ai_available()
    if enable_ai and not ai_ok:
        print("AI enabled but GEMINI_API_KEY / google-generativeai not available; continuing without AI", file=sys.stderr)
        enable_ai = False

    out_fields = [
        "id",
        "company_name",
        "status",
        "rep_name",
        "address",
        "found_address",
        "csv_address",
        "homepage",
        "phone",
        "rep_issue_score",
        "addr_issue_score",
        "total_score",
        "risk",
        "rep_issues",
        "addr_issues",
        "source_url_rep",
        "source_url_address",
        "ai_rep_ok",
        "ai_addr_ok",
        "ai_confidence",
        "ai_rep_reason",
        "ai_addr_reason",
    ]

    scanned = 0
    written = 0
    ai_sent = 0
    ai_done = 0

    # We write rows as we go; AI annotations are applied only when enabled and we batch.
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()

        pending_ai_rows: list[dict[str, Any]] = []
        pending_ai_out: list[dict[str, Any]] = []

        def flush_ai() -> None:
            nonlocal ai_sent, ai_done, written
            if not enable_ai or not pending_ai_rows:
                for o in pending_ai_out:
                    w.writerow(o)
                    written += 1
                pending_ai_rows.clear()
                pending_ai_out.clear()
                return
            ai_sent += len(pending_ai_rows)
            results = _ai_judge_batch(
                pending_ai_rows,
                model_name=ai_model,
                timeout_sec=float(args.ai_timeout_sec or 0),
                debug_out=str(args.ai_debug_out or ""),
            )
            ai_done += len(results)
            for o, res in zip(pending_ai_out, results):
                o["ai_rep_ok"] = "" if res.rep_ok is None else int(res.rep_ok)
                o["ai_addr_ok"] = "" if res.addr_ok is None else int(res.addr_ok)
                o["ai_confidence"] = f"{res.confidence:.2f}" if res.confidence else ""
                o["ai_rep_reason"] = res.rep_reason
                o["ai_addr_reason"] = res.addr_reason
                w.writerow(o)
                written += 1
            pending_ai_rows.clear()
            pending_ai_out.clear()

        start = time.time()
        for row in cur.execute(sql, params):
            rowd = dict(row)
            scanned += 1
            rep = _norm(rowd.get("rep_name"))
            address = _norm(rowd.get("address"))
            found_addr = _norm(rowd.get("found_address"))
            csv_addr = _norm(rowd.get(col_csv_addr)) if col_csv_addr else ""

            rep_issues = _rep_issues(rep)
            addr_issues = _addr_issues(address)

            # Pref mismatch (cheap high-signal)
            pref_addr = _extract_pref(address)
            pref_found = _extract_pref(found_addr)
            if pref_addr and pref_found and pref_addr != pref_found:
                addr_issues.append("address_pref_mismatch_vs_found")

            rep_score = _issue_score(rep_issues)
            addr_score = _issue_score(addr_issues)
            total = rep_score + addr_score

            if args.only_suspects and total < int(args.suspect_threshold):
                continue

            out: dict[str, Any] = {
                "id": rowd.get("id"),
                "company_name": _norm(rowd.get("company_name")),
                "status": _norm(rowd.get("status")),
                "rep_name": rep,
                "address": address,
                "found_address": found_addr,
                "csv_address": csv_addr,
                "homepage": _norm(rowd.get("homepage")),
                "phone": _norm(rowd.get("phone")),
                "rep_issue_score": rep_score,
                "addr_issue_score": addr_score,
                "total_score": total,
                "risk": _risk_label(total),
                "rep_issues": ";".join(rep_issues),
                "addr_issues": ";".join(addr_issues),
                "source_url_rep": _norm(rowd.get(col_source_rep)) if col_source_rep else "",
                "source_url_address": _norm(rowd.get(col_source_addr)) if col_source_addr else "",
                "ai_rep_ok": "",
                "ai_addr_ok": "",
                "ai_confidence": "",
                "ai_rep_reason": "",
                "ai_addr_reason": "",
            }

            if enable_ai and ai_sent < int(args.ai_max) and total >= int(args.suspect_threshold):
                pending_ai_rows.append(rowd)
                pending_ai_out.append(out)
                if len(pending_ai_rows) >= int(args.ai_batch_size):
                    flush_ai()
            else:
                w.writerow(out)
                written += 1

        flush_ai()
        elapsed = time.time() - start

    print(
        json.dumps(
            {
                "db": db_path,
                "out": out_path,
                "scanned": scanned,
                "written": written,
                "ai_enabled": enable_ai,
                "ai_sent": ai_sent,
                "ai_done": ai_done,
                "elapsed_sec": round(elapsed, 2),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
