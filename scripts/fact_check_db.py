#!/usr/bin/env python3
"""
Evidence-based fact checking for all columns in the companies DB.

Important note on "fact check":
- This script does NOT prove ground truth.
- It verifies whether stored values are supported by *retrievable evidence pages*
  (source_url_* / homepage / official_homepage / description_evidence URLs).

Output is a LONG format CSV (one row per company-field):
- verdict: verified / not_found / blank / unknown_no_evidence / fetch_failed

Cost/accuracy knobs:
- Keep concurrency low (default sequential).
- Cache fetched pages to disk.
- Use strict matching to avoid false positives; expect more unknowns.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import hashlib
import html as html_mod
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from typing import Any, Optional

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore

try:
    import requests
except Exception as e:  # pragma: no cover
    requests = None  # type: ignore
    REQ_IMPORT_ERROR = e
else:
    REQ_IMPORT_ERROR = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception as e:  # pragma: no cover
    BeautifulSoup = None  # type: ignore
    BS_IMPORT_ERROR = e
else:
    BS_IMPORT_ERROR = None


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


def _norm(s: Any) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("\u3000", " ")
    s = html_mod.unescape(s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _addr_key(s: str) -> str:
    s = _norm(s)
    s = s.replace("〒", "")
    s = s.replace("-", "")
    s = s.replace("−", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[()（）\[\]【】「」『』,。．・:：/]", "", s)
    return s


def _extract_pref(s: str) -> str:
    m = PREF_RE.search(s or "")
    return m.group(0) if m else ""


def _extract_snippet(text: str, needle: str, *, window: int = 40) -> str:
    if not text or not needle:
        return ""
    idx = text.find(needle)
    if idx < 0:
        return ""
    start = max(0, idx - window)
    end = min(len(text), idx + len(needle) + window)
    return text[start:end].strip()


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclasses.dataclass
class FetchResult:
    url: str
    ok: bool
    status_code: int
    elapsed_ms: int
    text: str
    error: str = ""


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    if BeautifulSoup is None:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        try:
            tag.decompose()
        except Exception:
            pass
    text = soup.get_text(" ", strip=True)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_url(
    session: "requests.Session",
    url: str,
    *,
    cache_dir: str,
    timeout: tuple[float, float] = (7.0, 25.0),
    sleep_sec: float = 0.0,
) -> FetchResult:
    cache_path = ""
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        key = _sha256(url)
        cache_path = os.path.join(cache_dir, f"{key}.json")
        if os.path.exists(cache_path):
            try:
                data = json.load(open(cache_path, encoding="utf-8"))
                return FetchResult(
                    url=url,
                    ok=bool(data.get("ok")),
                    status_code=int(data.get("status_code") or 0),
                    elapsed_ms=int(data.get("elapsed_ms") or 0),
                    text=str(data.get("text") or ""),
                    error=str(data.get("error") or ""),
                )
            except Exception:
                pass

    if sleep_sec and sleep_sec > 0:
        time.sleep(float(sleep_sec))

    t0 = time.time()
    try:
        resp = session.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "ja,en-US;q=0.8",
            },
            allow_redirects=True,
        )
        elapsed_ms = int((time.time() - t0) * 1000)
        ct = (resp.headers.get("content-type") or "").lower()
        raw_text = ""
        if "text/html" in ct or "<html" in resp.text.lower():
            raw_text = _html_to_text(resp.text)
        else:
            # For non-HTML, keep a small prefix only (avoid huge downloads in reports).
            raw_text = unicodedata.normalize("NFKC", (resp.text or "")[:8000])
            raw_text = re.sub(r"\s+", " ", raw_text).strip()
        out = FetchResult(url=url, ok=resp.ok, status_code=int(resp.status_code), elapsed_ms=elapsed_ms, text=raw_text)
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        out = FetchResult(url=url, ok=False, status_code=0, elapsed_ms=elapsed_ms, text="", error=str(e)[:200])

    if cache_path:
        try:
            json.dump(dataclasses.asdict(out), open(cache_path, "w", encoding="utf-8"), ensure_ascii=False)
        except Exception:
            pass
    return out


def _as_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _parse_json_list(v: Any) -> list[Any]:
    if not v:
        return []
    if isinstance(v, list):
        return v
    if not isinstance(v, str):
        return []
    s = v.strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _gather_evidence_urls(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for k in ("source_url_phone", "source_url_address", "source_url_rep", "homepage", "official_homepage", "final_homepage"):
        u = _norm(row.get(k))
        if u and u.startswith(("http://", "https://")):
            urls.append(u)
    # Deep visited URLs (if present)
    for u in _parse_json_list(row.get("deep_urls_visited"))[:6]:
        uu = _norm(u)
        if uu and uu.startswith(("http://", "https://")):
            urls.append(uu)
    # Description evidence URLs (if present)
    for item in _parse_json_list(row.get("description_evidence"))[:6]:
        if isinstance(item, dict):
            uu = _norm(item.get("url"))
            if uu and uu.startswith(("http://", "https://")):
                urls.append(uu)
    # De-dupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


@dataclasses.dataclass
class Verdict:
    verdict: str
    reason: str
    evidence_url: str = ""
    evidence_snippet: str = ""


def _verified_on_any(url_texts: dict[str, FetchResult], predicate) -> Verdict:
    had_any = False
    for url, fr in url_texts.items():
        had_any = True
        if not fr.ok:
            continue
        v = predicate(url, fr.text or "")
        if v is not None:
            return v
    if not had_any:
        return Verdict("unknown_no_evidence", "no_evidence_urls")
    if any(not fr.ok for fr in url_texts.values()):
        return Verdict("fetch_failed", "all_failed_or_no_match_on_ok_pages")
    return Verdict("not_found", "not_found_in_evidence")


def verify_phone(value: str, url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    digits = _digits_only(v)
    if len(digits) < 9:
        return Verdict("not_found", "phone_value_too_short")

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        td = _digits_only(t)
        if digits and digits in td:
            return Verdict("verified", "digits_found", evidence_url=url, evidence_snippet=_extract_snippet(td, digits))
        return None

    return _verified_on_any(url_texts, pred)


def verify_address(value: str, url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    key = _addr_key(v)
    if not key:
        return Verdict("not_found", "addr_key_empty")

    def pred(url: str, text: str) -> Optional[Verdict]:
        if not text:
            return None
        tk = _addr_key(text)
        if key and key in tk:
            return Verdict("verified", "addr_key_found", evidence_url=url, evidence_snippet=_extract_snippet(tk, key[: min(len(key), 24)]))
        return None

    return _verified_on_any(url_texts, pred)


def verify_rep_name(value: str, url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    # For names, use token-based matching (spaces separate names).
    tokens = [t for t in re.split(r"\s+", v) if t]
    if not tokens:
        return Verdict("not_found", "no_tokens")

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        # Prefer matches near representative keywords.
        for tok in tokens:
            if tok and tok in t:
                snippet = _extract_snippet(t, tok, window=60)
                if any(k in snippet for k in ("代表", "取締役", "社長", "会長", "CEO")):
                    return Verdict("verified", "token_found_near_rep_keyword", evidence_url=url, evidence_snippet=snippet)
        # Fallback: all tokens found anywhere (for multi-name OK).
        if all(tok in t for tok in tokens):
            return Verdict("verified", "all_tokens_found", evidence_url=url, evidence_snippet=_extract_snippet(t, tokens[0], window=60))
        return None

    return _verified_on_any(url_texts, pred)


def verify_text_contains(value: str, url_texts: dict[str, FetchResult], *, label: str) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    needle = v
    # For long text, search shorter fragments to avoid false negatives due to whitespace.
    if len(needle) >= 40:
        needle = re.sub(r"\s+", " ", needle)
        needle = needle[:60]

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        if needle in t:
            return Verdict("verified", f"{label}_substring_found", evidence_url=url, evidence_snippet=_extract_snippet(t, needle))
        return None

    return _verified_on_any(url_texts, pred)


def verify_amount(value: str, url_texts: dict[str, FetchResult], *, label: str) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    digits = _digits_only(v)
    if len(digits) < 4:
        # too small/ambiguous, don't claim verified
        return Verdict("unknown_no_evidence", f"{label}_digits_too_short")

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        td = _digits_only(t)
        if digits in td:
            return Verdict("verified", f"{label}_digits_found", evidence_url=url, evidence_snippet=_extract_snippet(td, digits))
        return None

    return _verified_on_any(url_texts, pred)


def verify_founded_year(value: str, url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    m = re.search(r"(19\d{2}|20\d{2})", v)
    if not m:
        return Verdict("unknown_no_evidence", "founded_year_not_parseable")
    year = m.group(1)

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        if year in t:
            snippet = _extract_snippet(t, year, window=80)
            if any(k in snippet for k in ("設立", "創業", "創立")):
                return Verdict("verified", "year_found_near_founded_keyword", evidence_url=url, evidence_snippet=snippet)
        return None

    return _verified_on_any(url_texts, pred)


def verify_fiscal_month(value: str, url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    # Accept "3" / "3月" / "03" etc.
    m = re.search(r"(\d{1,2})", v)
    if not m:
        return Verdict("unknown_no_evidence", "fiscal_month_not_parseable")
    month = int(m.group(1))
    if not (1 <= month <= 12):
        return Verdict("unknown_no_evidence", "fiscal_month_out_of_range")
    needle = f"{month}月"

    def pred(url: str, text: str) -> Optional[Verdict]:
        t = _norm(text)
        if not t:
            return None
        if needle in t:
            snippet = _extract_snippet(t, needle, window=80)
            if any(k in snippet for k in ("決算", "期", "事業年度", "会計年度", "年度")):
                return Verdict("verified", "month_found_near_fiscal_keyword", evidence_url=url, evidence_snippet=snippet)
        return None

    return _verified_on_any(url_texts, pred)


def verify_description(value: str, row: dict[str, Any], url_texts: dict[str, FetchResult]) -> Verdict:
    v = _norm(value)
    if not v:
        return Verdict("blank", "empty")
    # First: quick format sanity (doesn't verify truth).
    if any(k in v for k in ("http://", "https://", "@", "TEL", "電話", "FAX", "お問い合わせ", "採用", "アクセス")):
        return Verdict("not_found", "description_contains_forbidden_terms")
    if len(v) < 20:
        return Verdict("unknown_no_evidence", "description_too_short")

    # If description_evidence exists, verify snippets appear in those pages.
    items = _parse_json_list(row.get("description_evidence"))
    if not items:
        return Verdict("unknown_no_evidence", "no_description_evidence")

    checked = 0
    for item in items[:4]:
        if not isinstance(item, dict):
            continue
        url = _norm(item.get("url"))
        snip = _norm(item.get("snippet"))
        if not url or not snip:
            continue
        checked += 1
        fr = url_texts.get(url)
        if not fr:
            continue
        if not fr.ok:
            continue
        t = _norm(fr.text)
        if snip and snip in t:
            return Verdict("verified", "description_evidence_snippet_found", evidence_url=url, evidence_snippet=_extract_snippet(t, snip[: min(len(snip), 40)]))
    if checked == 0:
        return Verdict("unknown_no_evidence", "description_evidence_missing_or_empty")
    return Verdict("not_found", "description_evidence_snippet_not_found")


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
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default="", help="CSV output path (long format)")
    ap.add_argument("--cache-dir", default="cache/fact_check")
    ap.add_argument("--no-cache", action="store_true", default=False, help="disable on-disk cache")
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    ap.add_argument("--max-fetch", type=int, default=6000, help="global cap for URL fetches (safety)")
    args = ap.parse_args(argv)

    if REQ_IMPORT_ERROR is not None or requests is None:
        print(f"missing dependency: requests ({REQ_IMPORT_ERROR})", file=sys.stderr)
        return 2
    if BS_IMPORT_ERROR is not None or BeautifulSoup is None:
        print(f"missing dependency: beautifulsoup4 ({BS_IMPORT_ERROR})", file=sys.stderr)
        return 2

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 2

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = args.out.strip() or os.path.join("reports", f"fact_check_{os.path.basename(db_path).replace('.db','')}_{ts}.csv")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    cache_dir = "" if args.no_cache else (args.cache_dir or "")
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

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

    # One shared session + minimal retry via adapter if available.
    sess = requests.Session()
    try:
        from requests.adapters import HTTPAdapter  # type: ignore
        from urllib3.util.retry import Retry  # type: ignore

        retry = Retry(total=2, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
    except Exception:
        pass

    fetch_count = 0

    def get_texts_for_row(rowd: dict[str, Any]) -> dict[str, FetchResult]:
        nonlocal fetch_count
        urls = _gather_evidence_urls(rowd)
        out: dict[str, FetchResult] = {}
        for u in urls:
            if fetch_count >= int(args.max_fetch):
                break
            fr = fetch_url(sess, u, cache_dir=cache_dir, sleep_sec=float(args.sleep_sec or 0.0))
            fetch_count += 1
            out[u] = fr
        return out

    rows_scanned = 0
    written = 0

    # Only these are "fact-checkable" by evidence matching in this script.
    check_fields: list[str] = []
    for f in (
        "homepage",
        "official_homepage",
        "final_homepage",
        "phone",
        "address",
        "found_address",
        "rep_name",
        "listing",
        "revenue",
        "profit",
        "capital",
        "employees",
        "license",
        "industry",
        "business_tags",
        "fiscal_month",
        "founded_year",
        "description",
        "corporate_number",
    ):
        if f in colset:
            check_fields.append(f)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "company_name",
                "field",
                "value",
                "verdict",
                "reason",
                "evidence_url",
                "evidence_snippet",
            ],
        )
        w.writeheader()

        for row in cur.execute(sql, params):
            rows_scanned += 1
            rowd = dict(row)
            cid = rowd.get("id")
            cname = _norm(rowd.get("company_name"))
            url_texts = get_texts_for_row(rowd)

            for field in check_fields:
                val = _norm(rowd.get(field))
                if field in ("homepage", "official_homepage", "final_homepage"):
                    # Verify homepage is present as a URL; we don't claim truth.
                    if not val:
                        ver = Verdict("blank", "empty")
                    elif val.startswith(("http://", "https://")):
                        ver = Verdict("verified", "url_format_ok")
                    else:
                        ver = Verdict("not_found", "not_a_url")
                elif field in ("phone",):
                    ver = verify_phone(val, url_texts)
                elif field in ("address", "found_address"):
                    ver = verify_address(val, url_texts)
                elif field in ("rep_name",):
                    ver = verify_rep_name(val, url_texts)
                elif field in ("listing", "industry", "license"):
                    ver = verify_text_contains(val, url_texts, label=field)
                elif field in ("revenue", "profit", "capital", "employees"):
                    ver = verify_amount(val, url_texts, label=field)
                elif field in ("founded_year",):
                    ver = verify_founded_year(val, url_texts)
                elif field in ("fiscal_month",):
                    ver = verify_fiscal_month(val, url_texts)
                elif field in ("description",):
                    ver = verify_description(val, rowd, url_texts)
                elif field in ("business_tags",):
                    # JSON list expected; verify at least one tag appears.
                    tags = _parse_json_list(rowd.get(field))
                    if not tags:
                        ver = Verdict("blank" if not val else "unknown_no_evidence", "no_tags")
                    else:
                        tag_strs = [_norm(t) for t in tags if _norm(t)]
                        if not tag_strs:
                            ver = Verdict("unknown_no_evidence", "no_valid_tags")
                        else:
                            ver = verify_text_contains(tag_strs[0], url_texts, label=field)
                elif field in ("corporate_number",):
                    # Format check only (13 digits); authoritative verification is out of scope here.
                    digits = _digits_only(val)
                    if not val:
                        ver = Verdict("blank", "empty")
                    elif len(digits) == 13:
                        ver = Verdict("verified", "13_digits_format_ok")
                    else:
                        ver = Verdict("not_found", "corporate_number_format_not_13_digits")
                else:
                    ver = Verdict("unknown_no_evidence", "no_verifier_for_field")

                w.writerow(
                    {
                        "id": cid,
                        "company_name": cname,
                        "field": field,
                        "value": val,
                        "verdict": ver.verdict,
                        "reason": ver.reason,
                        "evidence_url": ver.evidence_url,
                        "evidence_snippet": ver.evidence_snippet,
                    }
                )
                written += 1

    print(
        json.dumps(
            {
                "db": db_path,
                "out": out_path,
                "rows_scanned": rows_scanned,
                "records_written": written,
                    "fetch_count": fetch_count,
                    "cache_dir": cache_dir,
                    "checked_fields": check_fields,
                },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
