#!/usr/bin/env python3
import argparse
import asyncio
import datetime as dt
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urljoin, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from src.ai_verifier import AIVerifier
from src.company_scraper import CompanyScraper

CONTACT_KEYWORDS = (
    "お問い合わせ",
    "お問合せ",
    "問い合わせ",
    "contact",
    "inquiry",
    "toiawase",
    "otoiawase",
    "support",
    "相談",
    "フォーム",
    "form",
)
CONTACT_PATH_HINTS = (
    "/contact",
    "/contactus",
    "/contact-us",
    "/inquiry",
    "/toiawase",
    "/otoiawase",
    "/form",
    "/support",
)
REJECT_PATH_HINTS = (
    "/recruit",
    "/career",
    "/careers",
    "/job",
    "/jobs",
    "/news",
    "/blog",
    "/press",
    "/privacy",
    "/policy",
    "/terms",
)
EXTERNAL_FORM_HOSTS = (
    "forms.gle",
    "form.run",
    "ssl.form-mailer.jp",
    "form-mailer.jp",
    "formzu.net",
    "ssl.formzu.net",
)
JP_2LD_SUFFIXES = (
    "co.jp",
    "or.jp",
    "ac.jp",
    "ed.jp",
    "go.jp",
    "lg.jp",
    "gr.jp",
    "ne.jp",
)

MIN_SCORE = 25
MAX_FETCH_PER_COMPANY = 6
AI_MIN_CONFIDENCE_DEFAULT = 0.6
COMPANY_TIMEOUT_SEC_DEFAULT = 30


def _normalize_fieldnames(fieldnames: Iterable[str]) -> List[str]:
    return [(name or "").lstrip("\ufeff").strip() for name in fieldnames]


def _iter_csv_rows(csv_path: str) -> Iterable[Dict[str, str]]:
    import csv

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return
        normalized = _normalize_fieldnames(reader.fieldnames)
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                if key is None:
                    continue
                norm_key = (key or "").lstrip("\ufeff").strip()
                cleaned[norm_key] = (value or "").strip()
            for idx, key in enumerate(normalized):
                if key and key not in cleaned:
                    cleaned[key] = ""
            yield cleaned


def _normalize_corporate_number(raw: str) -> str:
    return "".join(ch for ch in (raw or "") if ch.isdigit())


def _registrable_domain(host: str) -> str:
    host = (host or "").lower().strip(".")
    if not host:
        return ""
    labels = host.split(".")
    if len(labels) < 2:
        return host
    for suffix in JP_2LD_SUFFIXES:
        if host.endswith(suffix):
            if len(labels) >= 3:
                return ".".join(labels[-3:])
            return host
    return ".".join(labels[-2:])


def _same_reg_domain(host_a: str, host_b: str) -> bool:
    return _registrable_domain(host_a) == _registrable_domain(host_b)


def _parse_candidates(base_url: str, html: str, scraper: CompanyScraper) -> List[Dict[str, Any]]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    candidates: Dict[str, Dict[str, Any]] = {}

    def add_candidate(url: str, token: str, source: str) -> None:
        if not url:
            return
        if url in candidates:
            return
        candidates[url] = {"url": url, "token": token, "source": source}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue
        url = urljoin(base_url, href)
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        token = " ".join(
            [
                anchor.get_text(separator=" ", strip=True) or "",
                anchor.get("title") or "",
                href,
            ]
        ).lower()
        if any(k in token for k in CONTACT_KEYWORDS) or any(k in (parsed.path or "").lower() for k in CONTACT_PATH_HINTS):
            add_candidate(url, token, "anchor")

    for url in scraper._find_priority_links(base_url, html, max_links=6, target_types=["contact"]):
        add_candidate(url, "priority", "priority_links")

    for url in scraper._fallback_priority_links(base_url, target_types=["contact"]):
        add_candidate(url, "fallback", "fallback_links")

    return list(candidates.values())


def _score_candidate(
    candidate: Dict[str, Any],
    base_url: str,
    base_host: str,
) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []
    url = candidate["url"]
    token = candidate.get("token", "")
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = (parsed.path or "").lower()

    if host == base_host:
        score += 20
        reasons.append("same_host")
    elif _same_reg_domain(base_host, host):
        score += 15
        reasons.append("same_reg_domain")

    if any(seg in path for seg in CONTACT_PATH_HINTS):
        score += 10
        reasons.append("path_contact_hint")
    if any(k in token for k in CONTACT_KEYWORDS):
        score += 8
        reasons.append("token_contact_kw")
    if "お問い合わせ" in token:
        score += 8
        reasons.append("token_ja_contact")

    if any(seg in path for seg in REJECT_PATH_HINTS):
        score -= 40
        reasons.append("reject_path")
    if any(k in token for k in ("採用", "recruit", "career", "求人")):
        score -= 30
        reasons.append("recruit_hint")

    if host in EXTERNAL_FORM_HOSTS:
        score += 5
        reasons.append("external_form_host")

    if url == base_url:
        score += 2
        reasons.append("same_as_homepage")

    return score, reasons


def _score_with_content(scraper: CompanyScraper, url: str, html: str, text: str) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []
    if html and "<form" in html.lower():
        score += 10
        reasons.append("has_form")
    if text and any(k in text for k in ("お問い合わせ", "お問合せ", "問い合わせ")):
        score += 6
        reasons.append("text_contact_kw")
    page_type = scraper.classify_page_type(url, text=text or "", html=html or "")
    if page_type.get("page_type") == "ACCESS_CONTACT":
        score += 15
        reasons.append("access_contact")
    return score, reasons


def _homepage_is_contact(scraper: CompanyScraper, url: str, html: str, text: str) -> bool:
    if not html and not text:
        return False
    page_type = scraper.classify_page_type(url, text=text or "", html=html or "")
    if page_type.get("page_type") != "ACCESS_CONTACT":
        return False
    return "<form" in (html or "").lower() or any(k in (text or "") for k in ("お問い合わせ", "お問合せ", "問い合わせ"))


def _build_ai_signals(homepage: str, url: str, html: str) -> str:
    base_host = urlparse(homepage).netloc.lower()
    cand_host = urlparse(url).netloc.lower()
    same_host = base_host == cand_host
    same_reg = _same_reg_domain(base_host, cand_host)
    has_form = False
    form_count = 0
    input_count = 0
    action_hosts: List[str] = []
    title = ""
    if html:
        soup = BeautifulSoup(html, "html.parser")
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        forms = soup.find_all("form")
        form_count = len(forms)
        has_form = form_count > 0
        for form in forms[:5]:
            action = (form.get("action") or "").strip()
            if action:
                action_url = urljoin(url, action)
                host = urlparse(action_url).netloc.lower()
                if host and host not in action_hosts:
                    action_hosts.append(host)
            input_count += len(form.find_all(["input", "textarea", "select"]))
    signals = [
        f"same_host={str(same_host).lower()}",
        f"same_reg_domain={str(same_reg).lower()}",
        f"has_form={str(has_form).lower()}",
        f"form_count={form_count}",
        f"input_count={input_count}",
        f"action_hosts={','.join(action_hosts[:3])}",
        f"title={title}",
    ]
    return "; ".join(s for s in signals if s)


def _update_homepages_from_csv(conn: sqlite3.Connection, csv_path: str, force_homepage: bool) -> None:
    name_counts: Dict[str, int] = {}
    for row in conn.execute("SELECT company_name, COUNT(*) FROM companies GROUP BY company_name"):
        name = (row[0] or "").strip()
        if name:
            name_counts[name] = int(row[1] or 0)

    total_rows = 0
    updated_by_corp = 0
    updated_by_name = 0
    skipped_no_match = 0

    for row in _iter_csv_rows(csv_path):
        total_rows += 1
        homepage = (row.get("final_homepage") or "").strip()
        if not homepage:
            continue
        corp = _normalize_corporate_number(row.get("corporate_number") or "")
        updated = False
        if corp:
            if force_homepage:
                cur = conn.execute(
                    "UPDATE companies SET final_homepage=? WHERE corporate_number=?",
                    (homepage, corp),
                )
            else:
                cur = conn.execute(
                    "UPDATE companies SET final_homepage=? WHERE corporate_number=? AND TRIM(IFNULL(final_homepage,''))=''",
                    (homepage, corp),
                )
            if cur.rowcount:
                updated_by_corp += cur.rowcount
                updated = True
        if not updated:
            name = (row.get("company_name") or "").strip()
            if name and name_counts.get(name) == 1:
                if force_homepage:
                    cur = conn.execute(
                        "UPDATE companies SET final_homepage=? WHERE company_name=?",
                        (homepage, name),
                    )
                else:
                    cur = conn.execute(
                        "UPDATE companies SET final_homepage=? WHERE company_name=? AND TRIM(IFNULL(final_homepage,''))=''",
                        (homepage, name),
                    )
                if cur.rowcount:
                    updated_by_name += cur.rowcount
                    updated = True
        if not updated:
            skipped_no_match += 1

    conn.commit()
    print(
        "CSVホームページ更新: rows=%d updated_by_corp=%d updated_by_name=%d no_match=%d"
        % (total_rows, updated_by_corp, updated_by_name, skipped_no_match)
    )


async def _pick_contact_url(scraper: CompanyScraper, base_url: str) -> Tuple[str, float, str, str]:
    if not base_url:
        return "", 0.0, "none", "no_homepage"
    try:
        info = await scraper.get_page_info(base_url, allow_slow=False)
    except Exception:
        return "", 0.0, "error", "homepage_fetch_failed"

    base_html = info.get("html", "") or ""
    base_text = info.get("text", "") or ""
    base_host = urlparse(base_url).netloc.lower()

    if _homepage_is_contact(scraper, base_url, base_html, base_text):
        return base_url, 40.0, "homepage_form", "homepage_is_contact"

    candidates = _parse_candidates(base_url, base_html, scraper)
    if not candidates:
        return "", 0.0, "none", "no_candidates"

    scored: List[Dict[str, Any]] = []
    for cand in candidates:
        base_score, reasons = _score_candidate(cand, base_url, base_host)
        scored.append(
            {
                "url": cand["url"],
                "source": cand.get("source", ""),
                "base_score": base_score,
                "reasons": reasons,
            }
        )

    scored.sort(key=lambda x: (-x["base_score"], x["url"]))
    top = scored[:MAX_FETCH_PER_COMPANY]

    best_url = ""
    best_score = -999
    best_source = ""
    best_reason = ""

    for cand in top:
        url = cand["url"]
        try:
            page = await scraper.get_page_info(url, allow_slow=False)
        except Exception:
            continue
        html = page.get("html", "") or ""
        text = page.get("text", "") or ""
        extra_score, extra_reasons = _score_with_content(scraper, url, html, text)
        score = cand["base_score"] + extra_score
        reasons = cand["reasons"] + extra_reasons
        if score > best_score:
            best_score = score
            best_url = url
            best_source = cand.get("source", "")
            best_reason = ",".join(reasons)

    if best_score < MIN_SCORE:
        return "", float(best_score), "low_score", best_reason or "below_threshold"
    return best_url, float(best_score), best_source or "unknown", best_reason or "scored"


def _ensure_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(companies)")
    cols = {row[1] for row in cur.fetchall()}
    to_add = [
        ("contact_url", "TEXT"),
        ("contact_url_source", "TEXT"),
        ("contact_url_score", "REAL"),
        ("contact_url_reason", "TEXT"),
        ("contact_url_checked_at", "TEXT"),
        ("contact_url_ai_verdict", "TEXT"),
        ("contact_url_ai_confidence", "REAL"),
        ("contact_url_ai_reason", "TEXT"),
    ]
    for name, typ in to_add:
        if name not in cols:
            conn.execute(f"ALTER TABLE companies ADD COLUMN {name} {typ}")
    conn.commit()


async def _run(
    db_path: str,
    limit: int,
    force: bool,
    dry_run: bool,
    ai_enabled: bool,
    ai_min_confidence: float,
    csv_path: str,
    force_homepage: bool,
    progress: bool,
    progress_detail: bool,
    company_timeout_sec: int,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_columns(conn)
    if csv_path:
        _update_homepages_from_csv(conn, csv_path, force_homepage)

    where = "TRIM(IFNULL(final_homepage,''))<>''"
    if not force:
        where += " AND (contact_url IS NULL OR TRIM(contact_url)='')"
    limit_sql = f" LIMIT {limit}" if limit > 0 else ""
    rows = conn.execute(
        f"SELECT id, company_name, final_homepage FROM companies WHERE {where} ORDER BY id{limit_sql}"
    ).fetchall()

    total_rows = len(rows)
    scraper = CompanyScraper(headless=True)
    verifier = AIVerifier(db_path=db_path) if ai_enabled else None
    ai_ready = bool(verifier and verifier.model and verifier.contact_form_prompt)
    if ai_enabled and not ai_ready:
        print("AI判定が有効化されていません（モデル/プロンプト未設定）。ルールのみで進めます。")
    try:
        for idx, row in enumerate(rows, 1):
            cid = row["id"]
            company_name = (row["company_name"] or "").strip()
            homepage = (row["final_homepage"] or "").strip()
            if not homepage:
                continue
            if progress:
                print(f"[{idx}/{total_rows}] 開始 id={cid} homepage={homepage}")
                sys.stdout.flush()

            async def _process_company() -> Tuple[str, float, str, str, str, float, str]:
                url, score, source, reason = await _pick_contact_url(scraper, homepage)
                ai_verdict = ""
                ai_confidence = 0.0
                ai_reason = ""
                if url and ai_ready:
                    try:
                        page = await scraper.get_page_info(url, allow_slow=False)
                        text = page.get("text", "") or ""
                        html = page.get("html", "") or ""
                        signals = _build_ai_signals(homepage, url, html)
                        ai_result = await verifier.judge_contact_form(
                            text=text,
                            company_name=company_name,
                            homepage=homepage,
                            url=url,
                            signals=signals,
                        )
                    except Exception:
                        ai_result = None
                    if ai_result:
                        verdict_val = ai_result.get("is_official_contact_form")
                        ai_confidence = float(ai_result.get("confidence") or 0.0)
                        ai_reason = str(ai_result.get("reason") or "")
                        if verdict_val is True and ai_confidence >= ai_min_confidence:
                            ai_verdict = "official"
                        elif verdict_val is False:
                            ai_verdict = "not_official"
                        else:
                            ai_verdict = "unsure"
                    else:
                        ai_verdict = "unsure"
                        ai_reason = "ai_no_result"

                    if ai_verdict != "official":
                        reason = f"{reason};ai:{ai_verdict}"
                        url = ""
                return url, score, source, reason, ai_verdict, ai_confidence, ai_reason

            try:
                url, score, source, reason, ai_verdict, ai_confidence, ai_reason = await asyncio.wait_for(
                    _process_company(),
                    timeout=max(1, int(company_timeout_sec)),
                )
            except asyncio.TimeoutError:
                url = ""
                score = 0.0
                source = "timeout"
                reason = "company_timeout"
                ai_verdict = ""
                ai_confidence = 0.0
                ai_reason = ""

            checked_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            if progress_detail:
                print(
                    "結果: contact_url=%s score=%.1f source=%s reason=%s ai_verdict=%s ai_conf=%.2f ai_reason=%s"
                    % (url or "", float(score or 0.0), source or "", reason or "", ai_verdict or "", float(ai_confidence or 0.0), ai_reason or "")
                )
                sys.stdout.flush()

            if dry_run:
                print(
                    f"{cid}\t{homepage}\t{url}\t{score:.1f}\t{source}\t{reason}\t{ai_verdict}\t{ai_confidence:.2f}\t{ai_reason}"
                )
                continue
            conn.execute(
                """
                UPDATE companies
                   SET contact_url=?,
                       contact_url_source=?,
                       contact_url_score=?,
                       contact_url_reason=?,
                       contact_url_checked_at=?,
                       contact_url_ai_verdict=?,
                       contact_url_ai_confidence=?,
                       contact_url_ai_reason=?
                 WHERE id=?
                """,
                (
                    url or "",
                    source or "",
                    float(score or 0.0),
                    reason or "",
                    checked_at,
                    ai_verdict or "",
                    float(ai_confidence or 0.0),
                    ai_reason or "",
                    cid,
                ),
            )
            conn.commit()
    finally:
        try:
            await scraper.close()
        except Exception:
            pass
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract contact form URLs and store in DB.")
    ap.add_argument("--db", required=True, help="SQLite DB path")
    ap.add_argument("--csv", default="", help="Source CSV path to update final_homepage before extraction")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = no limit)")
    ap.add_argument("--force", action="store_true", help="Overwrite existing contact_url")
    ap.add_argument("--dry-run", action="store_true", help="Print results without updating DB")
    ap.add_argument("--force-homepage", action="store_true", help="Overwrite existing final_homepage from CSV")
    ap.add_argument("--ai", action="store_true", help="Enable AI-based contact form gate")
    ap.add_argument("--ai-min-confidence", type=float, default=AI_MIN_CONFIDENCE_DEFAULT, help="AI confidence threshold")
    ap.add_argument("--progress", action="store_true", help="Print progress per company")
    ap.add_argument("--progress-detail", action="store_true", help="Print result details per company in Japanese")
    ap.add_argument("--company-timeout-sec", type=int, default=COMPANY_TIMEOUT_SEC_DEFAULT, help="Per-company timeout in seconds")
    args = ap.parse_args()

    asyncio.run(
        _run(
            args.db,
            args.limit,
            args.force,
            args.dry_run,
            args.ai,
            args.ai_min_confidence,
            args.csv,
            args.force_homepage,
            args.progress,
            args.progress_detail,
            args.company_timeout_sec,
        )
    )


if __name__ == "__main__":
    main()
