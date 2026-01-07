#!/usr/bin/env python3
"""
Clean company fields in SQLite DB:
- address: keep "HQ-like" address only, remove mixed-in noise (phone/menu/copyright/etc).
- rep_name: keep only name-like content; otherwise blank. Multiple names allowed.

This script is designed to be conservative and auditable:
- Default is dry-run (no DB changes), producing a CSV report of proposed edits.
- With --apply, it makes a timestamped DB backup and applies changes in a transaction.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html as html_mod
import os
import re
import shutil
import sqlite3
import sys
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
ZIP_RE = re.compile(r"(?:〒\s*)?(\d{3})[-‐―－ー]?(\d{4})")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_CAND_RE = re.compile(r"(?:TEL|電話|☎|℡)?\s*(0\d{1,4})[-‐―－ー]?\d{1,4}[-‐―－ー]?\d{3,4}", re.IGNORECASE)

COORD_RE = re.compile(r"(北緯\s*\d+[^\s]{0,20}\s*東経\s*\d+[^\s]{0,20})", re.IGNORECASE)
COORD_DEC_RE = re.compile(r"\b-?\d{1,3}\.\d{3,}\b")

ADDR_JUNK_TERMS = (
    "メニュー",
    "メニュ",
    "コンテンツへスキップ",
    "プライバシ",
    "privacy",
    "cookie",
    "利用規約",
    "サイトマップ",
    "資料ダウンロード",
    "ダウンロード",
    "All Rights Reserved",
    "All Right Reserved",
    "Copyright",
    "©",
    "copy right",
    "place",
    "Japan",
)

ADDR_LABEL_PREFIX_RE = re.compile(
    r"^(?:本社|本店|本部|所在地|住所|本社所在地|本社住所|所在地住所)\s*[:：]?\s*",
    re.IGNORECASE,
)

HQ_HINTS = (
    "本社",
    "本店",
    "本部",
    "本社所在地",
    "本社住所",
)

BRANCH_HINTS = (
    "支店",
    "営業所",
    "事務所",
    "センター",
    "倉庫",
    "工場",
    "出張所",
    "オフィス",
    "分室",
    "事業所",
)

REP_TITLES = (
    "代表取締役",
    "代表者",
    "代表",
    "取締役社長",
    "社長",
    "会長",
    "取締役",
    "執行役員",
    "CEO",
    "COO",
    "CFO",
    "CTO",
    "初代社長",
    "副支社長",
    "副支",
    "頭取",
    "部長",
    "課長",
    "係長",
    "主任",
    "担当",
)

REP_STOPWORDS = (
    "受付時間",
    "口コミ件数",
    "企業情報",
    "企業· 情報",
    "免許",
    "資格",
    "免許・資格保有者",
    "おかげをもちまして",
    "わたしたち",
    "ホームページをリニューアル",
    "船舶貸渡業",
    "業種",
    "所属会社",
    "当社",
    "お問い合わせ",
    "問合せ",
    "問い合わせ",
    "随時更新中",
    "随時更新中!!",
    "物流",
    "運送",
    "運輸",
    "輸送",
    "陸運",
    "海運",
    "交通",
    "観光",
    "汽船",
    "バス",
    "タクシー",
    "自動車",
    "自動車取扱業",
    "取扱業",
    "事務所",
    "センター",
    "センタ",
    "支社",
    "支店",
    "営業所",
    "初代",
    "新人",
    "他",
    "保有車両",
    "軽トラック",
    "トラック",
    "設",
)

# Unicode ranges for Japanese/Chinese ideographs including extensions (covers 𠮷 etc).
HAN_CHARS = (
    "\u3400-\u4DBF"  # CJK Ext A
    "\u4E00-\u9FFF"  # CJK Unified Ideographs
    "\uF900-\uFAFF"  # CJK Compatibility Ideographs
    "\U00020000-\U0002EBEF"  # CJK Ext B..I (broad)
)
HAN_RE = re.compile(rf"[{HAN_CHARS}々〆ヶヵ]")


def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("\\u3000", " ")
    s = html_mod.unescape(s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_pref(s: str) -> str:
    m = PREF_RE.search(s or "")
    return m.group(0) if m else ""


def _contains_phone(text: str) -> bool:
    if not text:
        return False
    for m in PHONE_CAND_RE.finditer(text):
        cand = m.group(0) or ""
        digits = re.sub(r"\D+", "", cand)
        # Avoid false positives like "06-0035" from ZIP.
        if 10 <= len(digits) <= 12 and digits.startswith("0"):
            return True
    return False


def _strip_phone(text: str) -> tuple[str, bool]:
    if not text:
        return "", False
    changed = False

    def repl(m: re.Match) -> str:
        nonlocal changed
        cand = m.group(0) or ""
        digits = re.sub(r"\D+", "", cand)
        if 10 <= len(digits) <= 12 and digits.startswith("0"):
            changed = True
            return " "
        return cand

    out = PHONE_CAND_RE.sub(repl, text)
    out = re.sub(r"\s+", " ", out).strip()
    return out, changed


def _strip_urls_emails(text: str) -> tuple[str, bool]:
    if not text:
        return "", False
    out = text
    out2 = URL_RE.sub(" ", out)
    out3 = EMAIL_RE.sub(" ", out2)
    changed = out3 != text
    out3 = re.sub(r"\s+", " ", out3).strip()
    return out3, changed


def _strip_address_junk(text: str, *, company_name: str = "") -> tuple[str, list[str]]:
    actions: list[str] = []
    if not text:
        return "", actions
    out = text

    out2, url_changed = _strip_urls_emails(out)
    if url_changed:
        actions.append("addr_drop_url_or_email")
    out = out2

    out2, phone_changed = _strip_phone(out)
    if phone_changed:
        actions.append("addr_drop_phone")
    out = out2

    out2 = COORD_RE.sub(" ", out)
    if out2 != out:
        actions.append("addr_drop_coords")
    out = out2
    out2 = COORD_DEC_RE.sub(" ", out)
    if out2 != out:
        actions.append("addr_drop_coord_numbers")
    out = out2

    for term in ADDR_JUNK_TERMS:
        if term and term.lower() in out.lower():
            out = re.sub(re.escape(term), " ", out, flags=re.IGNORECASE)
            actions.append(f"addr_drop_junk:{term}")

    # Drop company name if it is embedded (often from footer/copyright).
    cn = _norm(company_name)
    if cn and cn in out and len(cn) >= 4:
        out2 = out.replace(cn, " ")
        if out2 != out:
            out = out2
            actions.append("addr_drop_company_name")

    out = re.sub(r"\s+", " ", out).strip()
    out2 = ADDR_LABEL_PREFIX_RE.sub("", out)
    if out2 != out:
        actions.append("addr_drop_label_prefix")
    out = out2

    # Normalize ZIP formatting to "〒NNN-NNNN"
    def _zip_repl(m: re.Match) -> str:
        return f"〒{m.group(1)}-{m.group(2)}"

    out2 = ZIP_RE.sub(_zip_repl, out)
    if out2 != out:
        actions.append("addr_normalize_zip")
    out = out2
    out = re.sub(r"\s+", " ", out).strip()
    # Remove duplicate prefecture tokens (e.g. '福岡県福岡県')
    for pref in PREFECTURES:
        out2 = re.sub(rf"({re.escape(pref)})\s*\1", r"\1", out)
        if out2 != out:
            out = out2
            actions.append("addr_drop_duplicate_pref")
    out = re.sub(r"\s+", " ", out).strip()
    out = re.sub(r"[.．。·・]+\s*$", "", out).strip()
    return out, actions


def _split_address_candidates(text: str) -> list[tuple[int, str]]:
    """
    Return list of (start_index, segment_text) in original order.
    Prefer splitting by ZIP markers; if none but multiple prefectures appear, split at prefecture starts.
    """
    if not text:
        return []
    # ZIP-based segmentation.
    zip_pat = r"(?:〒\s*)?\d{3}[-‐―－ー]?\d{4}"
    zips = list(re.finditer(zip_pat, text))
    if len(zips) >= 2:
        segments: list[tuple[int, str]] = []
        for i, m in enumerate(zips):
            start = m.start()
            end = zips[i + 1].start() if i + 1 < len(zips) else len(text)
            seg = text[start:end].strip()
            # Include a small lookback context so labels like "神戸営業所" are associated with the segment.
            if i > 0:
                prev_end = zips[i - 1].end()
                gap = text[prev_end:start]
                gap_tail = gap[-24:].strip()
                if gap_tail and any(k in gap_tail for k in BRANCH_HINTS + HQ_HINTS):
                    seg = f"{gap_tail} {seg}".strip()
            if seg:
                segments.append((start, seg))
        return segments

    # If any ZIP exists (single), do not split by prefectures; keep as one address.
    if zips:
        return [(0, text.strip())]

    # Prefecture-based segmentation when multiple prefectures exist.
    prefs = list(PREF_RE.finditer(text))
    if len(prefs) >= 2:
        segments = []
        for i, m in enumerate(prefs):
            start = m.start()
            end = prefs[i + 1].start() if i + 1 < len(prefs) else len(text)
            seg = text[start:end].strip()
            if seg:
                segments.append((start, seg))
        return segments

    return [(0, text.strip())]


def _score_address_segment(seg: str) -> int:
    s = seg or ""
    score = 0
    if any(h in s for h in HQ_HINTS):
        score += 10
    if any(b in s for b in BRANCH_HINTS):
        score -= 6
    if "〒" in s or ZIP_RE.search(s):
        score += 3
    if _extract_pref(s):
        score += 2
    if re.search(r"(?:都|道|府|県).{0,20}(?:市|区|町|村|郡)", s):
        score += 2
    if any(ch.isdigit() for ch in s):
        score += 1
    # Penalize obvious UI noise if still present.
    if any(t.lower() in s.lower() for t in ("メニュー", "スキップ", "privacy", "cookie", "copyright")):
        score -= 4
    return score


def clean_address_keep_hq(raw: str, *, company_name: str = "") -> tuple[str, list[str]]:
    raw_n = _norm(raw)
    cleaned, actions = _strip_address_junk(raw_n, company_name=company_name)
    if not cleaned:
        return "", actions + (["addr_became_empty"] if raw_n else [])

    candidates = _split_address_candidates(cleaned)
    if len(candidates) <= 1:
        return cleaned, actions

    scored = []
    for start, seg in candidates:
        scored.append((_score_address_segment(seg), start, seg))
    scored.sort(key=lambda x: (-x[0], x[1]))
    best = scored[0][2]
    actions.append("addr_select_hq_like_segment")

    # Drop leading labels in the chosen segment too.
    best2 = ADDR_LABEL_PREFIX_RE.sub("", best).strip()
    if best2 != best:
        actions.append("addr_drop_label_prefix")
        best = best2
    best = re.sub(r"\s+", " ", best).strip()
    # If a branch label is dangling at the end (e.g. "... 神戸営業所"), drop it.
    tail_re = re.compile(r"\s*[^\s]{1,16}(?:%s)\s*$" % "|".join(map(re.escape, BRANCH_HINTS)))
    best2 = tail_re.sub("", best).strip()
    if best2 != best:
        actions.append("addr_drop_trailing_branch_label")
        best = best2
    return best, actions


def _name_candidates(text: str) -> list[str]:
    """
    Extract potential person-name substrings.
    - allows: '山田 太郎', '山田太郎', 'ジョン・ドウ', 'Taro Yamada', and single-token kanji names.
    """
    if not text:
        return []
    t = text
    # Normalize separators to spaces.
    t = re.sub(r"[・、,／/|→⇒＞>]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    candidates: list[str] = []
    han = rf"[{HAN_CHARS}々〆ヶヵ]"
    # Han full name (with optional space)
    for m in re.finditer(rf"{han}{{1,6}}\s*{han}{{1,6}}", t):
        candidates.append(m.group(0).strip())
    # Kana/Katakana name-like
    for m in re.finditer(r"[ァ-ヶー]{2,}(?:\s+[ァ-ヶー]{2,})?", t):
        candidates.append(m.group(0).strip())
    # Latin name-like
    for m in re.finditer(r"[A-Za-z][A-Za-z .'-]{2,}", t):
        candidates.append(m.group(0).strip())
    # Single-token Han (allow given-name only) ONLY when no full-name candidates exist.
    if not any(HAN_RE.search(c) and " " in c for c in candidates):
        for m in re.finditer(rf"{han}{{1,6}}", t):
            candidates.append(m.group(0).strip())

    # Deduplicate preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        c = re.sub(r"\s+", " ", c).strip()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _is_stopword_like(token: str) -> bool:
    if not token:
        return True
    if token in REP_TITLES:
        return True
    if token in REP_STOPWORDS:
        return True
    if token in ("月", "火", "水", "木", "金", "土", "日"):
        return True
    if token in ("代", "表"):
        return True
    return False


def _is_name_like_simple(text: str) -> bool:
    """
    Heuristic: whether `text` looks like a person name.
    - Allows single given-name-only (e.g. '順二').
    - Rejects when address/contact/junk indicators exist.
    """
    s = _norm(text)
    if not s:
        return False
    if URL_RE.search(s) or EMAIL_RE.search(s) or _contains_phone(s) or ZIP_RE.search(s) or _extract_pref(s):
        return False
    if any(w and w.lower() in s.lower() for w in REP_STOPWORDS):
        return False
    if re.search(r"\d", s):
        return False
    # Reject pure titles/labels.
    if s in REP_TITLES or s in ("代 表", "代表", "社長", "会長", "取締役"):
        return False
    # Must contain Han/Kana/Latin letters.
    if HAN_RE.search(s) or re.search(r"[ぁ-んァ-ン]", s) or re.search(r"[A-Za-z]", s):
        return True
    return False


def clean_rep_name(raw: str) -> tuple[str, list[str]]:
    """
    Keep only name-like content; otherwise blank.
    Multiple names allowed. Single given-name-only is allowed.
    """
    raw_orig = "" if raw is None else str(raw)
    raw_n = _norm(raw_orig)
    actions: list[str] = []
    if not raw_n:
        return "", actions

    # Hard reject: clearly not a name field.
    if URL_RE.search(raw_n) or EMAIL_RE.search(raw_n) or _contains_phone(raw_n) or ZIP_RE.search(raw_n) or _extract_pref(raw_n):
        return "", ["rep_blank_contains_contact_or_address"]

    lowered = raw_n.lower()
    has_stopword = False
    for w in REP_STOPWORDS:
        if w and w.lower() in lowered:
            actions.append(f"rep_contains_stopword:{w}")
            has_stopword = True
    # Remove titles (keep names).
    t = raw_n
    for title in REP_TITLES:
        if title in t:
            t = t.replace(title, " ")
            actions.append(f"rep_drop_title:{title}")
    # Remove trivial label-ish fragments.
    t = re.sub(r"\b(?:rep_name|代表者名|代表者)\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"[：:]", " ", t)
    t = re.sub(r"[・、,／/|]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    cands = _name_candidates(t)
    # Filter stopword-like candidates.
    kept: list[str] = []
    for c in cands:
        if _is_stopword_like(c):
            continue
        if any(sw in c for sw in REP_STOPWORDS):
            continue
        # Short katakana fragments are usually not person names (e.g. 'コミ' from 口コミ).
        if re.fullmatch(r"[ァ-ヶー]{1,3}", c):
            continue
        # Business-category-ish patterns
        if c.endswith("業") or "取扱" in c:
            continue
        if any(k in c for k in ("物流", "運送", "運輸", "輸送", "交通", "観光", "汽船", "バス", "タクシー", "センター", "事務所")):
            # If it contains obvious transport/company words, treat as non-person.
            # Exception: allow if it also contains a clear Han full-name pattern with a space.
            if not re.search(r"\s", c):
                continue
        if any(k in c for k in ("支社", "支店", "営業所")):
            continue
        kept.append(c)

    if not kept:
        # Fallback: if a "full name" candidate was filtered out by a stopword,
        # try recovering single-token Han names (e.g. "武富孝一 設" -> "武富孝一").
        han = rf"[{HAN_CHARS}々〆ヶヵ]"
        for c in re.findall(rf"{han}{{1,6}}", t):
            c = c.strip()
            if not c or _is_stopword_like(c):
                continue
            if any(sw in c for sw in REP_STOPWORDS):
                continue
            if c.endswith("業") or "取扱" in c:
                continue
            if any(k in c for k in ("物流", "運送", "運輸", "輸送", "交通", "観光", "汽船", "バス", "タクシー", "センター", "事務所", "支社", "支店", "営業所")):
                continue
            kept.append(c)
        # Deduplicate preserving order
        seen: set[str] = set()
        kept2: list[str] = []
        for c in kept:
            if c in seen:
                continue
            seen.add(c)
            kept2.append(c)
        kept = kept2

    if not kept:
        return "", ["rep_blank_not_name_like"] + actions

    def is_strong_name(c: str) -> bool:
        han = rf"[{HAN_CHARS}々〆ヶヵ]"
        if re.search(rf"{han}{{1,6}}\s+{han}{{1,6}}", c):
            return True
        if re.search(r"[A-Za-z]+\s+[A-Za-z]+", c):
            return True
        # Single token Han name (allow), but not 1-char.
        if HAN_RE.search(c) and " " not in c and len(c) >= 2:
            return True
        # Katakana full token (foreign name) only when relatively long.
        if re.fullmatch(r"[ァ-ヶー]{4,}", c):
            return True
        return False

    if has_stopword and not any(is_strong_name(c) for c in kept):
        return "", ["rep_blank_stopword_no_strong_name"] + actions

    # If there is severe junk besides names, blank; otherwise keep extracted names only.
    remainder = t
    for k in kept:
        remainder = remainder.replace(k, " ")
    for title in REP_TITLES:
        remainder = remainder.replace(title, " ")
    for w in REP_STOPWORDS:
        remainder = remainder.replace(w, " ")
    remainder = re.sub(r"[\s\-‐―－ー()（）\[\]【】「」『』.,]+", " ", remainder)
    remainder = re.sub(r"\s+", " ", remainder).strip()
    # Allow tiny leftover Han fragments (often a stray surname token) when we already extracted a strong name.
    han = rf"[{HAN_CHARS}々〆ヶヵ]"
    if remainder and re.fullmatch(rf"{han}{{1,6}}", remainder) and len(remainder) <= 3:
        actions.append("rep_ignore_small_han_remainder")
        remainder = ""
    if remainder and re.search(r"[0-9A-Za-zぁ-んァ-ン一-龥々]", remainder):
        # Still contains meaningful text that is not a title/known stopword.
        return "", ["rep_blank_contains_extra_text"] + actions

    out = " ".join(kept)
    out = re.sub(r"\s+", " ", out).strip()
    if out != raw_n:
        actions.append("rep_keep_extracted_names_only")
    return out, actions


@dataclass
class Change:
    id: Any
    company_name: str
    old_address: str
    new_address: str
    address_actions: str
    old_rep: str
    new_rep: str
    rep_actions: str


def _default_report_path(db_path: str) -> str:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(db_path).replace(".db", "")
    return os.path.join("reports", f"field_clean_{base}_{ts}.csv")


def _load_ai_audit_map(path: str) -> dict[int, dict[str, str]]:
    """
    Load AI audit results exported by scripts/audit_company_fields.py.
    Returns: {id: {"ai_rep_ok": "1|0|", "ai_addr_ok": "1|0|"}}
    """
    m: dict[int, dict[str, str]] = {}
    if not path or not os.path.exists(path):
        return m
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                cid = int(str(row.get("id") or "").strip())
            except Exception:
                continue
            m[cid] = {
                "ai_rep_ok": str(row.get("ai_rep_ok") or "").strip(),
                "ai_addr_ok": str(row.get("ai_addr_ok") or "").strip(),
            }
    return m


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("COMPANIES_DB_PATH") or "data/companies.db")
    ap.add_argument("--table", default="companies")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--report", default="")
    ap.add_argument("--audit-csv", default="", help="AI audit CSV (reports/field_audit_*_ai.csv). If provided, rep_name cleaning follows ai_rep_ok.")
    ap.add_argument("--apply", action="store_true", default=False)
    ap.add_argument("--backup-dir", default="backups")
    args = ap.parse_args(argv)

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 2

    report_path = args.report.strip() or _default_report_path(db_path)
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    audit_map = _load_ai_audit_map(args.audit_csv.strip())

    sql = f"SELECT id, company_name, address, rep_name FROM {args.table} ORDER BY id"
    if args.limit and args.limit > 0:
        sql += f" LIMIT {int(args.limit)}"

    changes: list[Change] = []
    scanned = 0
    addr_changed = 0
    rep_changed = 0

    for row in cur.execute(sql):
        scanned += 1
        rowd = dict(row)
        cid = rowd.get("id")
        name = _norm(rowd.get("company_name"))
        old_addr = _norm(rowd.get("address"))
        old_rep = _norm(rowd.get("rep_name"))

        new_addr, addr_actions = clean_address_keep_hq(old_addr, company_name=name)

        # rep_name policy:
        # - If AI says rep is OK, keep as-is (trim only), unless it contains contact/address indicators.
        # - Else (false/null/no-audit), keep only extracted name-like tokens; otherwise blank.
        ai = audit_map.get(int(cid)) if cid is not None and str(cid).isdigit() else None
        ai_rep_ok = (ai or {}).get("ai_rep_ok", "")
        if ai_rep_ok == "1" and _is_name_like_simple(old_rep):
            new_rep, rep_actions = old_rep, []
        else:
            new_rep, rep_actions = clean_rep_name(old_rep)

        if new_addr != old_addr or new_rep != old_rep:
            if new_addr != old_addr:
                addr_changed += 1
            if new_rep != old_rep:
                rep_changed += 1
            changes.append(
                Change(
                    id=cid,
                    company_name=name,
                    old_address=old_addr,
                    new_address=new_addr,
                    address_actions=";".join(addr_actions),
                    old_rep=old_rep,
                    new_rep=new_rep,
                    rep_actions=";".join(rep_actions),
                )
            )

    with open(report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "id",
                "company_name",
                "old_address",
                "new_address",
                "address_actions",
                "old_rep_name",
                "new_rep_name",
                "rep_actions",
            ]
        )
        for c in changes:
            w.writerow(
                [
                    c.id,
                    c.company_name,
                    c.old_address,
                    c.new_address,
                    c.address_actions,
                    c.old_rep,
                    c.new_rep,
                    c.rep_actions,
                ]
            )

    print(
        f"scanned={scanned} changes={len(changes)} addr_changed={addr_changed} rep_changed={rep_changed} report={report_path} apply={bool(args.apply)}"
    )

    if not args.apply:
        return 0

    # Backup then apply in a transaction.
    os.makedirs(args.backup_dir, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(args.backup_dir, f"{os.path.basename(db_path)}.{ts}.bak")
    shutil.copy2(db_path, backup_path)
    print(f"backup={backup_path}")

    conn.execute("BEGIN")
    try:
        for c in changes:
            conn.execute(
                f"UPDATE {args.table} SET address=?, rep_name=? WHERE id=?",
                (c.new_address, c.new_rep, c.id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
