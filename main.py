# main.py
import asyncio
import os
import csv
import logging
import re
import random
import time
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse
from dotenv import load_dotenv

from src.database_manager import DatabaseManager
from src.company_scraper import CompanyScraper
from src.ai_verifier import AIVerifier, DEFAULT_MODEL as AI_MODEL_NAME
from src.reference_checker import ReferenceChecker

# --------------------------------------------------
# ロギング設定
# --------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# .env 読み込み
load_dotenv()

# --------------------------------------------------
# 実行オプション（.env）
# --------------------------------------------------
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
USE_AI = os.getenv("USE_AI", "true").lower() == "true"
WORKER_ID = os.getenv("WORKER_ID", "w1")  # 並列識別子

MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
ID_MIN = int(os.getenv("ID_MIN", "0"))
ID_MAX = int(os.getenv("ID_MAX", "0"))
AI_COOLDOWN_SEC = float(os.getenv("AI_COOLDOWN_SEC", "0"))
SLEEP_BETWEEN_SEC = float(os.getenv("SLEEP_BETWEEN_SEC", "0"))
JITTER_RATIO = float(os.getenv("JITTER_RATIO", "0.30"))
REFERENCE_CSVS = [p.strip() for p in os.getenv("REFERENCE_CSVS", "").split(",") if p.strip()]
FETCH_CONCURRENCY = max(1, int(os.getenv("FETCH_CONCURRENCY", "3")))
PROFILE_FETCH_CONCURRENCY = max(1, int(os.getenv("PROFILE_FETCH_CONCURRENCY", "3")))
SEARCH_CANDIDATE_LIMIT = max(1, int(os.getenv("SEARCH_CANDIDATE_LIMIT", "3")))
# 全体のタイムアウトは使わず、フェーズ別で管理する
TIME_LIMIT_SEC = float(os.getenv("TIME_LIMIT_SEC", "0"))
TIME_LIMIT_FETCH_ONLY = float(os.getenv("TIME_LIMIT_FETCH_ONLY", "60"))  # 公式未確定で候補取得フェーズ（0で無効）
TIME_LIMIT_WITH_OFFICIAL = float(os.getenv("TIME_LIMIT_WITH_OFFICIAL", "90"))  # 公式確定後、主要項目未充足（0で無効）

MIRROR_TO_CSV = os.getenv("MIRROR_TO_CSV", "false").lower() == "true"
OUTPUT_CSV_PATH = os.getenv("OUTPUT_CSV_PATH", "data/output.csv")
CSV_FIELDNAMES = [
    "id", "company_name", "address", "employee_count",
    "homepage", "phone", "found_address", "rep_name", "description",
    "listing", "revenue", "profit", "capital", "fiscal_month", "founded_year"
]
PHASE_METRICS_PATH = os.getenv("PHASE_METRICS_PATH", "logs/phase_metrics.csv")

REFERENCE_CHECKER: ReferenceChecker | None = None
if REFERENCE_CSVS:
    try:
        REFERENCE_CHECKER = ReferenceChecker.from_csvs(REFERENCE_CSVS)
        log.info("Reference data loaded: %s rows", len(REFERENCE_CHECKER))
    except Exception:
        log.exception("Reference data loading failed")

ZIP_CODE_RE = re.compile(r"(\d{3}-\d{4})")
KANJI_TOKEN_RE = re.compile(r"[一-龥]{2,}")
LISTING_ALLOWED_KEYWORDS = [
    "上場", "未上場", "非上場", "東証", "名証", "札証", "福証", "JASDAQ",
    "TOKYO PRO", "マザーズ", "グロース", "スタンダード", "プライム",
    "Nasdaq", "NYSE"
]
AMOUNT_ALLOWED_UNITS = ("億円", "万円", "千円", "円")
DESCRIPTION_HINTS = (
    "会社概要", "法人概要", "団体概要", "組合概要", "企業情報", "基本情報",
    "事業内容", "事業紹介", "沿革", "理念", "ごあいさつ", "ご挨拶",
    "私たちについて", "about", "会社紹介", "法人紹介", "概要"
)
GENERIC_DESCRIPTION_TERMS = {
    "会社概要", "企業情報", "事業概要", "法人概要", "団体概要",
    "トップメッセージ", "ご挨拶", "メッセージ", "沿革", "理念",
}

# --------------------------------------------------
# 正規化 & 一致判定
# --------------------------------------------------
def normalize_phone(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"[‐―－ー]+", "-", s)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def normalize_address(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip().replace("　", " ")
    s = re.sub(r"[‐―－ー]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    m = re.search(r"(\d{3}-\d{4})\s*(.*)", s)
    if m:
        body = m.group(2).strip()
        return f"〒{m.group(1)} {body}"
    return s if s else None

def addr_compatible(input_addr: str, found_addr: str) -> bool:
    input_addr = normalize_address(input_addr)
    found_addr = normalize_address(found_addr)
    if not input_addr or not found_addr:
        return False
    return input_addr[:8] in found_addr or found_addr[:8] in input_addr

def pick_best_address(expected_addr: str | None, candidates: list[str]) -> str | None:
    normalized_candidates = []
    for cand in candidates:
        norm = normalize_address(cand)
        if norm:
            normalized_candidates.append(norm)
    if not normalized_candidates:
        return None
    if not expected_addr:
        return normalized_candidates[0]

    expected_norm = normalize_address(expected_addr)
    if not expected_norm:
        return normalized_candidates[0]

    expected_key = CompanyScraper._addr_key(expected_norm)
    expected_zip_match = ZIP_CODE_RE.search(expected_norm)
    expected_zip = expected_zip_match.group(1) if expected_zip_match else ""
    expected_tokens = KANJI_TOKEN_RE.findall(expected_norm)

    best = normalized_candidates[0]
    best_score = float("-inf")
    for cand in normalized_candidates:
        key = CompanyScraper._addr_key(cand)
        score = 0.0
        cand_zip_match = ZIP_CODE_RE.search(cand)
        if expected_zip and cand_zip_match and cand_zip_match.group(1) == expected_zip:
            score += 8
        elif not expected_zip and cand_zip_match:
            score += 1
        if expected_key and key:
            score += SequenceMatcher(None, expected_key, key).ratio() * 6
        for token in expected_tokens:
            if token and token in cand:
                score += min(len(token), 4)
                break
        if score > best_score:
            best_score = score
            best = cand
    return best

def clean_listing_value(val: str) -> str:
    text = (val or "").strip().replace("　", " ")
    if not text:
        return ""
    if re.search(r"[。！？!?\n]", text):
        return ""
    text = re.sub(r"\s+", "", text)
    if len(text) > 15:
        return ""
    lowered = text.lower()
    if any(keyword.lower() in lowered for keyword in LISTING_ALLOWED_KEYWORDS):
        return text
    if re.fullmatch(r"(?:上場|未上場|非上場)", text):
        return text
    if re.fullmatch(r"[0-9]{4}", text):  # 証券コードのみ
        return text
    return ""

def clean_amount_value(val: str) -> str:
    text = (val or "").strip().replace("　", " ")
    if not text:
        return ""
    if not re.search(r"[0-9０-９]", text):
        return ""
    if not any(unit in text for unit in AMOUNT_ALLOWED_UNITS):
        return ""
    text = re.sub(r"\s+", "", text)
    if len(text) > 40:
        text = text[:40]
    return text

def clean_description_value(val: str) -> str:
    text = (val or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    stripped = text.strip("・-—‐－ー")
    if stripped in GENERIC_DESCRIPTION_TERMS:
        return ""
    if len(stripped) < 8:
        return ""
    if re.fullmatch(r"(会社概要|事業概要|法人概要|沿革|会社案内|企業情報)", stripped):
        return ""
    # 事業内容を示す動詞/名詞が無い見出しは除外
    biz_keywords = ("事業", "製造", "開発", "販売", "提供", "サービス", "運営", "支援", "施工", "設計", "製作")
    if not any(k in stripped for k in biz_keywords):
        return ""
    return stripped[:80]

def clean_fiscal_month(val: str) -> str:
    text = (val or "").strip().replace("　", " ")
    if not text:
        return ""
    m = re.search(r"(1[0-2]|0?[1-9])\s*月", text)
    if m:
        return f"{int(m.group(1))}月"
    m = re.search(r"(1[0-2]|0?[1-9])", text)
    if m:
        return f"{int(m.group(1))}月"
    return ""


def extract_description_snippet(text: str | None) -> str | None:
    if not text:
        return None
    paragraphs = [p.strip() for p in re.split(r"[\r\n]+", text) if p.strip()]
    if not paragraphs:
        return None
    # ノイズになる段落を除外（ニュース・採用・日付行など）
    noise_patterns = (
        r"採用", r"求人", r"募集", r"ニュース", r"お知らせ", r"新着", r"イベント",
        r"\d{4}\s*年\s*\d{1,2}\s*月", r"\d{4}/\d{1,2}/\d{1,2}", r"\d{4}-\d{1,2}-\d{1,2}",
        r"会社概要", r"事業概要", r"法人概要", r"沿革"
    )
    cleaned_paragraphs: list[str] = []
    for para in paragraphs:
        lowered = para.lower()
        if any(re.search(pat, para) for pat in noise_patterns):
            continue
        if "news" in lowered or "recruit" in lowered or "採用" in para:
            continue
        cleaned_paragraphs.append(para)
    if cleaned_paragraphs:
        paragraphs = cleaned_paragraphs

    lowered = [p.lower() for p in paragraphs]
    for idx, para in enumerate(paragraphs):
        if any(hint.lower() in lowered[idx] for hint in DESCRIPTION_HINTS):
            cleaned = clean_description_value(para)
            if cleaned:
                return cleaned
    for para in paragraphs:
        cleaned = clean_description_value(para)
        if cleaned:
            return cleaned
    return None

def clean_founded_year(val: str) -> str:
    text = (val or "").strip()
    if not text:
        return ""
    m = re.search(r"(18|19|20)\d{2}", text)
    if m:
        return m.group(0)
    if text.isdigit() and len(text) == 4:
        return text
    return ""


def record_needs_official_ai(record: dict[str, Any]) -> bool:
    rule_details = record.get("rule") or {}
    flag_info = record.get("flag_info")
    if flag_info and flag_info.get("is_official"):
        return False
    if rule_details.get("is_official"):
        return False
    score = float(rule_details.get("score") or 0.0)
    if rule_details.get("strong_domain") and score >= 4:
        return False
    return True


async def ensure_info_has_screenshot(
    scraper: CompanyScraper,
    url: str,
    info: dict[str, Any] | None,
) -> dict[str, Any]:
    info = info or {}
    if info.get("screenshot"):
        return info
    try:
        refreshed = await scraper.get_page_info(url, need_screenshot=True)
    except Exception:
        return info
    if not refreshed:
        return info
    merged = dict(info)
    for key in ("text", "html", "url", "screenshot"):
        if key in refreshed and refreshed[key]:
            merged[key] = refreshed[key]
    return merged

async def ensure_info_text(
    scraper: CompanyScraper,
    url: str,
    info: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    テキスト/HTMLのみ不足している場合に軽量に再取得する（スクショは撮らない）。
    """
    info = info or {}
    if info.get("text") and info.get("html"):
        return info
    try:
        refreshed = await scraper.get_page_info(url, need_screenshot=False)
        if refreshed:
            if refreshed.get("text"):
                info["text"] = refreshed.get("text", "")
            if refreshed.get("html"):
                info["html"] = refreshed.get("html", "")
    except Exception:
        pass
    return info

def log_phase_metric(
    company_id: int,
    phase: str,
    elapsed_sec: float,
    status: str,
    homepage: str,
    error_code: str,
) -> None:
    if not PHASE_METRICS_PATH:
        return
    try:
        os.makedirs(os.path.dirname(PHASE_METRICS_PATH) or ".", exist_ok=True)
        file_exists = os.path.exists(PHASE_METRICS_PATH)
        with open(PHASE_METRICS_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["id", "phase", "elapsed_sec", "status", "homepage", "worker", "error_code"])
            writer.writerow([company_id, phase, f"{elapsed_sec:.3f}", status, homepage or "", WORKER_ID, error_code or ""])
    except Exception:
        log.debug("phase metrics write skipped", exc_info=True)

def is_ambiguous_company_name(name: str) -> bool:
    base = CompanyScraper._normalize_company_name(name)
    if not base:
        return True
    # ほぼ固有名詞がそのまま入っているとみなし、極端に短い場合のみ曖昧扱い
    if len(base) <= 2:
        return True
    tokens = CompanyScraper._company_tokens(name)
    return len(tokens) == 0

def should_skip_company(name: str) -> bool:
    """
    明らかに法人ではない/店舗・支店のみの名称をスキップする。
    - 都道府県名そのもの、庁・役所・役場を含む自治体名
    - コンビニ店舗（セブン/ファミマ/ローソン等）で末尾が「店」
    - 法人格を含まない支店/営業所/出張所のみの名称
    """
    base = (name or "").strip()
    if not base:
        return False
    norm = CompanyScraper._normalize_company_name(base)
    if norm in CompanyScraper.PREFECTURE_NAMES:
        return True
    if re.search(r"(県庁|市役所|区役所|町役場|村役場)$", base):
        return True
    konbini_keywords = ("セブン-イレブン", "セブンイレブン", "7-11", "7－11", "7–11", "ファミリーマート", "ファミマ", "ローソン", "ミニストップ", "セイコーマート", "デイリーヤマザキ")
    if any(kw in base for kw in konbini_keywords) and base.endswith("店"):
        return True
    has_corp = any(tag in base for tag in ("株式会社", "有限会社", "合同会社", "Inc", "Co.", "Corporation", "Company", "Ltd"))
    if not has_corp and re.search(r"(支店|営業所|出張所)$", base):
        return True
    return False

# --------------------------------------------------
# 内部: 次ジョブ取得
# --------------------------------------------------
def claim_next(manager: DatabaseManager) -> dict | None:
    if hasattr(manager, "claim_next_company"):
        return manager.claim_next_company(WORKER_ID)
    return manager.get_next_company()

# --------------------------------------------------
# ユーティリティ：ジッター付きスリープ秒
# --------------------------------------------------
def jittered_seconds(base: float, ratio: float) -> float:
    if base <= 0 or ratio <= 0:
        return max(0.0, base)
    low = max(0.0, base * (1.0 - ratio))
    high = base * (1.0 + ratio)
    return random.uniform(low, high)

# --------------------------------------------------
# メイン処理（ワーカー）
# --------------------------------------------------
async def process():
    log.info(
        "=== Runner started (worker=%s) === HEADLESS=%s USE_AI=%s MAX_ROWS=%s "
        "ID_MIN=%s ID_MAX=%s AI_COOLDOWN_SEC=%s SLEEP_BETWEEN_SEC=%s JITTER_RATIO=%.2f "
        "MIRROR_TO_CSV=%s",
        WORKER_ID, HEADLESS, USE_AI, MAX_ROWS, ID_MIN, ID_MAX,
        AI_COOLDOWN_SEC, SLEEP_BETWEEN_SEC, JITTER_RATIO, MIRROR_TO_CSV
    )

    scraper = CompanyScraper(headless=HEADLESS)
    # CompanyScraper に start()/close() が無い実装でも動くように安全に呼ぶ
    if hasattr(scraper, "start") and callable(getattr(scraper, "start")):
        try:
            await scraper.start()
        except Exception:
            log.warning("scraper.start() はスキップ（未実装または失敗）", exc_info=True)

    verifier = AIVerifier() if USE_AI else None
    manager = DatabaseManager()

    csv_file = None
    csv_writer = None
    try:
        if MIRROR_TO_CSV:
            os.makedirs(os.path.dirname(OUTPUT_CSV_PATH) or ".", exist_ok=True)
            file_exists = os.path.exists(OUTPUT_CSV_PATH) and os.path.getsize(OUTPUT_CSV_PATH) > 0
            csv_file = open(OUTPUT_CSV_PATH, mode="a", newline="", encoding="utf-8")
            csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
            if not file_exists:
                csv_writer.writeheader()
                csv_file.flush()
            log.info("CSV mirror enabled -> %s", OUTPUT_CSV_PATH)

        processed = 0

        while True:
            if MAX_ROWS and processed >= MAX_ROWS:
                log.info("MAX_ROWS=%s に到達。", MAX_ROWS)
                break

            company = claim_next(manager)
            if not company:
                log.info("キューが空です。終了。")
                break

            cid = company.get("id")
            name = (company.get("company_name") or "").strip()
            addr = (company.get("address") or "").strip()

            if (ID_MIN and cid < ID_MIN) or (ID_MAX and cid > ID_MAX):
                log.info("[skip] id=%s はレンジ外 -> skipped (worker=%s)", cid, WORKER_ID)
                manager.update_status(cid, "skipped")
                continue
            if should_skip_company(name):
                log.info("[skip] 法人でない名称のためスキップ: id=%s name=%s", cid, name)
                manager.update_status(cid, "skipped")
                continue

            log.info("[%s] %s の処理開始 (worker=%s)", cid, name, WORKER_ID)

            started_at = time.monotonic()
            timed_out = False

            def elapsed() -> float:
                return time.monotonic() - started_at

            def over_time_limit() -> bool:
                return TIME_LIMIT_SEC > 0 and elapsed() > TIME_LIMIT_SEC
            def over_fetch_limit() -> bool:
                return TIME_LIMIT_FETCH_ONLY > 0 and not homepage and elapsed() > TIME_LIMIT_FETCH_ONLY
            def over_after_official() -> bool:
                return TIME_LIMIT_WITH_OFFICIAL > 0 and homepage and elapsed() > TIME_LIMIT_WITH_OFFICIAL

            try:
                candidate_limit = SEARCH_CANDIDATE_LIMIT
                if is_ambiguous_company_name(name):
                    candidate_limit = SEARCH_CANDIDATE_LIMIT + 1
                    log.info("[%s] 曖昧名のため候補数を拡張: %s -> %s", cid, SEARCH_CANDIDATE_LIMIT, candidate_limit)
                urls = await scraper.search_company(name, addr, num_results=candidate_limit)
                homepage = ""
                info = None
                primary_cands: dict[str, list[str]] = {}
                fallback_cands: list[tuple[str, dict[str, list[str]]]] = []
                homepage_official_flag = 0
                homepage_official_source = ""
                homepage_official_score = 0.0
                force_review = False
                ai_time_spent = 0.0
                chosen_domain_score = 0
                search_phase_end = 0.0
                official_phase_end = 0.0
                deep_phase_end = 0.0

                fetch_sem = asyncio.Semaphore(FETCH_CONCURRENCY)

                async def prepare_candidate(idx: int, candidate: str):
                    normalized_candidate = scraper.normalize_homepage_url(candidate)
                    url_for_flag = normalized_candidate or candidate
                    try:
                        flag_info = manager.get_url_flag(url_for_flag)
                    except Exception:
                        flag_info = None
                    if flag_info and not flag_info.get("is_official"):
                        log.info("[%s] 既知の非公式URLを除外: %s", cid, candidate)
                        return None
                    async with fetch_sem:
                        candidate_info = await scraper.get_page_info(candidate)
                    candidate_text = candidate_info.get("text", "") or ""
                    candidate_html = candidate_info.get("html") or ""
                    extracted = scraper.extract_candidates(candidate_text, candidate_html)
                    rule_details = scraper.is_likely_official_site(
                        name, candidate, candidate_info, addr, extracted, return_details=True
                    )
                    if not isinstance(rule_details, dict):
                        rule_details = {"is_official": bool(rule_details), "score": 0.0}
                    return (
                        idx,
                        {
                            "url": candidate,
                            "normalized_url": url_for_flag,
                            "info": candidate_info,
                            "extracted": extracted,
                            "rule": rule_details,
                            "flag_info": flag_info,
                        },
                    )

                prepare_tasks = [
                    asyncio.create_task(prepare_candidate(idx, candidate))
                    for idx, candidate in enumerate(urls)
                ]
                candidate_records: list[dict[str, Any]] = []
                if prepare_tasks:
                    prepared = await asyncio.gather(*prepare_tasks, return_exceptions=True)
                    ordered: list[tuple[int, dict[str, Any]]] = []
                    for result in prepared:
                        if isinstance(result, Exception) or not result:
                            continue
                        ordered.append(result)
                    ordered.sort(key=lambda x: x[0])
                    candidate_records = [record for _, record in ordered]
                search_phase_end = elapsed()

                if over_fetch_limit() or over_time_limit():
                    timed_out = True
                if homepage and over_after_official():
                    timed_out = True

                if not candidate_records:
                    company.update({
                        "homepage": "",
                        "phone": "",
                        "found_address": "",
                        "rep_name": company.get("rep_name", "") or "",
                        "description": company.get("description", "") or "",
                        "listing": company.get("listing", "") or "",
                        "revenue": company.get("revenue", "") or "",
                        "profit": company.get("profit", "") or "",
                        "capital": company.get("capital", "") or "",
                        "fiscal_month": company.get("fiscal_month", "") or "",
                        "founded_year": company.get("founded_year", "") or "",
                        "homepage_official_flag": 0,
                        "homepage_official_source": "",
                        "homepage_official_score": 0.0,
                    })
                    manager.save_company_data(company, status="no_homepage")
                    log.info("[%s] 候補ゼロ -> no_homepage で保存", cid)
                    if csv_writer:
                        csv_writer.writerow({k: company.get(k, "") for k in CSV_FIELDNAMES})
                        csv_file.flush()
                    processed += 1
                    if SLEEP_BETWEEN_SEC > 0:
                        await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))
                    continue

                ai_official_attempted = False
                if USE_AI and verifier is not None and hasattr(verifier, "judge_official_homepage"):
                    ai_tasks: list[asyncio.Task] = []
                    ai_sem = asyncio.Semaphore(2)

                    async def run_official_ai(record: dict[str, Any]):
                        nonlocal ai_official_attempted, ai_time_spent
                        if not record_needs_official_ai(record):
                            return
                        async with ai_sem:
                            domain_score = scraper._domain_score(  # type: ignore
                                scraper._company_tokens(name),  # type: ignore
                                record.get("normalized_url") or record.get("url") or "",
                            )
                            info_payload = record.get("info") or {}
                            info_payload = await ensure_info_has_screenshot(
                                scraper,
                                record.get("url"),
                                info_payload,
                            )
                            record["info"] = info_payload
                            ai_started = time.monotonic()
                            try:
                                ai_verdict = await verifier.judge_official_homepage(
                                    info_payload.get("text", "") or "",
                                    info_payload.get("screenshot"),
                                    name,
                                    addr,
                                    record.get("normalized_url") or record.get("url"),
                                )
                            except Exception:
                                log.warning("[%s] AI公式判定失敗: %s", cid, record.get("url"), exc_info=True)
                                ai_verdict = None
                            ai_time_spent += time.monotonic() - ai_started
                            if ai_verdict:
                                ai_ok = ai_verdict.get("is_official")
                                addr_hit = bool(record.get("rule", {}).get("address_match"))
                                if ai_ok and domain_score < 4 and not addr_hit:
                                    ai_verdict["is_official"] = False
                                record["ai_judge"] = ai_verdict
                                ai_official_attempted = True
                            if ai_official_attempted and AI_COOLDOWN_SEC > 0:
                                await asyncio.sleep(jittered_seconds(AI_COOLDOWN_SEC, JITTER_RATIO))

                    for record in candidate_records[:3]:
                        ai_tasks.append(asyncio.create_task(run_official_ai(record)))
                    if ai_tasks:
                        await asyncio.gather(*ai_tasks, return_exceptions=True)

                for record in candidate_records:
                    normalized_url = record.get("normalized_url") or record.get("url")
                    extracted = record.get("extracted") or {}
                    rule_details = record.get("rule") or {}
                    domain_score = scraper._domain_score(  # type: ignore
                        scraper._company_tokens(name),  # type: ignore
                        normalized_url or "",
                    )
                    addr_hit = bool(rule_details.get("address_match"))
                    pref_hit = bool(rule_details.get("prefecture_match"))
                    zip_hit = bool(rule_details.get("postal_code_match"))
                    address_ok = (not addr) or addr_hit or pref_hit or zip_hit
                    ai_judge = record.get("ai_judge")
                    flag_info = record.get("flag_info")
                    if rule_details.get("blocked_host"):
                        manager.upsert_url_flag(
                            normalized_url,
                            is_official=False,
                            source="rule",
                            reason=f"blocked_host:{rule_details.get('host', '')}",
                            scope="host",
                        )
                        fallback_cands.append((record.get("url"), extracted))
                        log.info("[%s] 除外ホスト(%s)をスキップ: %s", cid, rule_details.get("host"), record.get("url"))
                        continue
                    if ai_judge:
                        if ai_judge.get("is_official") is False:
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="ai",
                                reason=ai_judge.get("reason", ""),
                                confidence=ai_judge.get("confidence"),
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] AIが非公式判定: %s", cid, record.get("url"))
                            continue
                        if ai_judge.get("is_official") is True:
                            if domain_score < 3 and not address_ok:
                                fallback_cands.append((record.get("url"), extracted))
                                force_review = True
                                log.info("[%s] AI公式を見送り: low_domain_and_no_address url=%s", cid, record.get("url"))
                                continue
                            if domain_score < 4:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="ai",
                                    reason="domain_score_low",
                                    confidence=ai_judge.get("confidence"),
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式判定を却下: low_domain_score=%s url=%s", cid, domain_score, record.get("url"))
                                continue
                            if domain_score < 6 and not address_ok:
                                force_review = True
                                log.info("[%s] AI公式判定: 住所突合弱いためreview扱い url=%s", cid, record.get("url"))
                            homepage = normalized_url
                            info = record.get("info")
                            primary_cands = extracted
                            homepage_official_flag = 1
                            homepage_official_source = "ai"
                            homepage_official_score = float(rule_details.get("score") or 0.0)
                            chosen_domain_score = domain_score
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=True,
                                source="ai",
                                reason=ai_judge.get("reason", ""),
                                confidence=ai_judge.get("confidence"),
                            )
                            break
                    if flag_info and flag_info.get("is_official"):
                        if domain_score < 3 and not address_ok:
                            fallback_cands.append((record.get("url"), extracted))
                            force_review = True
                            log.info("[%s] キャッシュ公式見送り: low_domain_and_no_address url=%s", cid, record.get("url"))
                            continue
                        if not address_ok:
                            force_review = True
                            log.info("[%s] キャッシュ公式: 住所突合弱いためreview扱い url=%s", cid, record.get("url"))
                        homepage = normalized_url
                        info = record.get("info")
                        primary_cands = extracted
                        homepage_official_flag = 1
                        homepage_official_source = flag_info.get("judge_source") or "cache"
                        homepage_official_score = float(rule_details.get("score") or 0.0)
                        chosen_domain_score = domain_score
                        break
                    if rule_details.get("is_official"):
                        if domain_score < 3 and not address_ok:
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason=f"weak_domain_score={domain_score}",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            force_review = True
                            log.info("[%s] 低ドメイン一致+住所弱で公式判定を見送り: %s", cid, record.get("url"))
                            continue
                        if not address_ok:
                            force_review = True
                            log.info("[%s] 住所突合弱いが公式扱い→review: %s", cid, record.get("url"))
                        homepage = normalized_url
                        info = record.get("info")
                        primary_cands = extracted
                        homepage_official_flag = 1
                        homepage_official_source = "rule"
                        homepage_official_score = float(rule_details.get("score") or 0.0)
                        chosen_domain_score = domain_score
                        manager.upsert_url_flag(
                            normalized_url,
                            is_official=True,
                            source="rule",
                            reason=f"score={rule_details.get('score', 0.0):.1f}",
                        )
                        break
                    score_val = float(rule_details.get("score") or 0.0)
                    if score_val <= 1 and not rule_details.get("strong_domain"):
                        manager.upsert_url_flag(
                            normalized_url,
                            is_official=False,
                            source="rule",
                            reason=f"score={score_val:.1f}",
                        )
                        fallback_cands.append((record.get("url"), extracted))
                    log.info("[%s] 非公式と判断: %s", cid, record.get("url"))

                provisional_homepage = ""
                provisional_info = None
                provisional_cands: dict[str, list[str]] = {}
                provisional_domain_score = 0
                if not homepage and candidate_records:
                    best_score = float("-inf")
                    best_record: dict[str, Any] | None = None
                    for record in candidate_records:
                        normalized_url = record.get("normalized_url") or record.get("url")
                        rule_details = record.get("rule") or {}
                        domain_score = scraper._domain_score(  # type: ignore
                            scraper._company_tokens(name),  # type: ignore
                            normalized_url or "",
                        )
                        addr_hit = bool(rule_details.get("address_match"))
                        pref_hit = bool(rule_details.get("prefecture_match"))
                        zip_hit = bool(rule_details.get("postal_code_match"))
                        address_ok = addr_hit or pref_hit or zip_hit
                        # 採用条件を緩和: domain_score>=2 または 住所突合あり
                        if domain_score >= 2 or address_ok:
                            score = domain_score * 2 + (3 if address_ok else 0) + float(rule_details.get("score") or 0.0)
                            if score > best_score:
                                best_score = score
                                best_record = record
                    if best_record:
                        provisional_homepage = best_record.get("normalized_url") or best_record.get("url") or ""
                        provisional_info = best_record.get("info")
                        provisional_cands = best_record.get("extracted") or {}
                        provisional_domain_score = scraper._domain_score(  # type: ignore
                            scraper._company_tokens(name),  # type: ignore
                            provisional_homepage,
                        )
                        log.info("[%s] 公式未確定のため暫定ホームページで深掘り: %s", cid, provisional_homepage)

                if not homepage and provisional_homepage:
                    homepage = provisional_homepage
                    info = provisional_info
                    primary_cands = provisional_cands
                    homepage_official_flag = 0
                    homepage_official_source = "provisional"
                    homepage_official_score = 0.0
                    chosen_domain_score = provisional_domain_score
                    force_review = True

                official_phase_end = elapsed()
                priority_docs: dict[str, dict[str, Any]] = {}

                phone = ""
                found_address = ""
                rep_name_val = scraper.clean_rep_name(company.get("rep_name")) or ""
                description_val = clean_description_value(company.get("description") or "")
                listing_val = clean_listing_value(company.get("listing") or "")
                revenue_val = clean_amount_value(company.get("revenue") or "")
                profit_val = clean_amount_value(company.get("profit") or "")
                capital_val = clean_amount_value(company.get("capital") or "")
                fiscal_val = clean_fiscal_month(company.get("fiscal_month") or "")
                founded_val = clean_founded_year(company.get("founded_year") or "")
                phone_source = "none"
                address_source = "none"
                ai_used = 0
                ai_model = ""
                company.setdefault("error_code", "")
                company.setdefault("listing", listing_val)
                company.setdefault("revenue", revenue_val)
                company.setdefault("profit", profit_val)
                company.setdefault("capital", capital_val)
                company.setdefault("fiscal_month", fiscal_val)
                company.setdefault("founded_year", founded_val)
                src_phone = ""
                src_addr = ""
                src_rep = ""
                verify_result = {"phone_ok": False, "address_ok": False}
                confidence = 0.0
                need_listing = not bool(listing_val)
                need_capital = not bool(capital_val)
                need_revenue = not bool(revenue_val)
                need_profit = not bool(profit_val)
                need_fiscal = not bool(fiscal_val)
                need_founded = not bool(founded_val)
                need_description = not bool(description_val)
                rule_phone = None
                rule_address = None
                rule_rep = None

                info_dict = info or {}
                info_url = homepage

                def absorb_doc_data(url: str, pdata: dict[str, Any]) -> None:
                    nonlocal rule_phone, rule_address, rule_rep
                    nonlocal src_phone, src_addr, src_rep
                    nonlocal listing_val, need_listing
                    nonlocal capital_val, need_capital
                    nonlocal revenue_val, need_revenue
                    nonlocal profit_val, need_profit
                    nonlocal fiscal_val, need_fiscal
                    nonlocal founded_val, need_founded
                    nonlocal description_val, need_description

                    cc = scraper.extract_candidates(pdata.get("text", ""), pdata.get("html", ""))
                    if not rule_phone and cc.get("phone_numbers"):
                        cand = normalize_phone(cc["phone_numbers"][0])
                        if cand:
                            rule_phone = cand
                            src_phone = url
                    if not rule_address and cc.get("addresses"):
                        cand_addr = pick_best_address(addr, cc["addresses"])
                        if cand_addr:
                            rule_address = cand_addr
                            src_addr = url
                    if not rule_rep and cc.get("rep_names"):
                        cand_rep = scraper.clean_rep_name(cc["rep_names"][0])
                        if cand_rep:
                            rule_rep = cand_rep
                            src_rep = url
                    if not listing_val and cc.get("listings"):
                        listing_val = (cc["listings"][0] or "").strip()
                        need_listing = not bool(listing_val)
                    if not capital_val and cc.get("capitals"):
                        capital_val = (cc["capitals"][0] or "").strip()
                        need_capital = not bool(capital_val)
                    if not revenue_val and cc.get("revenues"):
                        revenue_val = (cc["revenues"][0] or "").strip()
                        need_revenue = not bool(revenue_val)
                    if not profit_val and cc.get("profits"):
                        profit_val = (cc["profits"][0] or "").strip()
                        need_profit = not bool(profit_val)
                    if not fiscal_val and cc.get("fiscal_months"):
                        fiscal_val = clean_fiscal_month(cc["fiscal_months"][0])
                        need_fiscal = not bool(fiscal_val)
                    if not founded_val and cc.get("founded_years"):
                        founded_val = (cc["founded_years"][0] or "").strip()
                        need_founded = not bool(founded_val)
                    if need_description and not description_val:
                        snippet = extract_description_snippet(pdata.get("text", ""))
                        if snippet:
                            description_val = snippet
                            need_description = False

                if homepage:
                    info_dict = info or {}
                    info_url = info_dict.get("url") or homepage
                    cands = primary_cands or {}
                    phones = cands.get("phone_numbers") or []
                    addrs = cands.get("addresses") or []
                    reps = cands.get("rep_names") or []
                    listings = cands.get("listings") or []
                    capitals = cands.get("capitals") or []
                    revenues = cands.get("revenues") or []
                    profits = cands.get("profits") or []
                    fiscals = cands.get("fiscal_months") or []
                    founded_years = cands.get("founded_years") or []

                    rule_phone = normalize_phone(phones[0]) if phones else None
                    rule_address = pick_best_address(addr, addrs) if addrs else None
                    rule_rep = reps[0] if reps else None
                    rule_rep = scraper.clean_rep_name(rule_rep) if rule_rep else None
                    if rule_phone and not src_phone:
                        src_phone = info_url
                    if rule_address and not src_addr:
                        src_addr = info_url
                    if rule_rep and not src_rep:
                        src_rep = info_url
                    if listings and not listing_val:
                        listing_val = listings[0].strip()
                    if capitals and not capital_val:
                        capital_val = capitals[0].strip()
                    if revenues and not revenue_val:
                        revenue_val = revenues[0].strip()
                    if profits and not profit_val:
                        profit_val = profits[0].strip()
                    if fiscals and not fiscal_val:
                        fiscal_val = clean_fiscal_month(fiscals[0])
                    if founded_years and not founded_val:
                        founded_val = founded_years[0].strip()

                    need_listing = not bool(listing_val)
                    need_capital = not bool(capital_val)
                    need_revenue = not bool(revenue_val)
                    need_profit = not bool(profit_val)
                    need_fiscal = not bool(fiscal_val)
                    need_founded = not bool(founded_val)
                    need_description = not bool(description_val)

                    if over_time_limit():
                        timed_out = True

                    fully_filled = (
                        homepage
                        and (rule_phone or phone)
                        and (rule_address or found_address)
                        and (rule_rep or rep_name_val)
                        and not any([
                            need_listing, need_capital, need_revenue,
                            need_profit, need_fiscal, need_founded, need_description,
                        ])
                    )

                    try:
                        extra_need = any([
                            not rule_phone,
                            not rule_address,
                            not rule_rep,
                            need_description,
                        ])
                        priority_limit = 10 if extra_need else 6
                        site_docs = (
                            {}
                            if timed_out or fully_filled
                            else await scraper.fetch_priority_documents(
                                homepage, info_dict.get("html", ""), max_links=priority_limit
                            )
                        )
                    except Exception:
                        site_docs = {}
                    for url, pdata in site_docs.items():
                        priority_docs[url] = pdata
                        absorb_doc_data(url, pdata)

                need_external_profiles = (
                    not homepage
                    or not rule_phone
                    or not rule_address
                    or not rule_rep
                    or need_listing
                    or need_capital
                    or need_revenue
                    or need_profit
                    or need_fiscal
                    or need_founded
                    or need_description
                )

                if need_external_profiles and not timed_out and not fully_filled:
                    try:
                        profile_urls = await scraper.search_company_info_pages(name, addr, max_results=3)
                    except Exception:
                        profile_urls = []
                    filtered_profile_urls: list[str] = []
                    for profile_url in profile_urls:
                        if homepage:
                            try:
                                if urlparse(profile_url).netloc.lower() != urlparse(homepage).netloc.lower():
                                    continue
                            except Exception:
                                continue
                        else:
                            try:
                                if not scraper.is_relevant_profile_url(name, profile_url):
                                    continue
                            except Exception:
                                continue
                        filtered_profile_urls.append(profile_url)

                    if filtered_profile_urls and not timed_out:
                        profile_sem = asyncio.Semaphore(PROFILE_FETCH_CONCURRENCY)

                        async def fetch_profile(url: str):
                            async with profile_sem:
                                info_payload = await scraper.get_page_info(url)
                            return url, info_payload

                        fetch_tasks = [asyncio.create_task(fetch_profile(url)) for url in filtered_profile_urls]
                        fetched_profiles = await asyncio.gather(*fetch_tasks, return_exceptions=True)
                        for profile in fetched_profiles:
                            if isinstance(profile, Exception) or not profile:
                                continue
                            profile_url, profile_info = profile
                            pdata = {
                                "text": profile_info.get("text", "") or "",
                                "html": profile_info.get("html", "") or "",
                            }
                            priority_docs[profile_url] = pdata
                            absorb_doc_data(profile_url, pdata)

                ai_result = None
                ai_attempted = False
                ai_task: asyncio.Task | None = None
                pre_ai_phone_ok = bool(rule_phone or phone)
                pre_ai_addr_ok = bool(rule_address or found_address)
                pre_ai_rep_ok = bool(rule_rep or rep_name_val)
                ai_needed = (
                    homepage
                    and USE_AI
                    and verifier is not None
                    and any([
                        not pre_ai_phone_ok,
                        not pre_ai_addr_ok,
                        not pre_ai_rep_ok,
                        need_listing,
                        need_capital,
                        need_revenue,
                        need_profit,
                        need_fiscal,
                        need_founded,
                        need_description,
                    ])
                )
                if ai_needed and not timed_out:
                    ai_attempted = True
                    info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict)
                    info = info_dict
                    async def run_ai_verify():
                        nonlocal ai_time_spent
                        ai_started = time.monotonic()
                        try:
                            res = await verifier.verify_info(
                                info_dict.get("text", "") or "",
                                info_dict.get("screenshot"),
                                name,
                                addr,
                            )
                        except Exception:
                            log.warning("[%s] AI検証失敗 -> ルールベースにフォールバック", cid, exc_info=True)
                            return None
                        finally:
                            ai_time_spent += time.monotonic() - ai_started
                        return res
                    ai_task = asyncio.create_task(run_ai_verify())

                ai_phone: str | None = None
                ai_addr: str | None = None
                ai_rep: str | None = None
                if ai_task:
                    ai_result = await ai_task
                if ai_result:
                    ai_used = 1
                    ai_model = AI_MODEL_NAME
                    ai_phone = normalize_phone(ai_result.get("phone_number"))
                    ai_addr = normalize_address(ai_result.get("address"))
                    ai_rep = ai_result.get("rep_name") or ai_result.get("representative")
                    ai_rep = scraper.clean_rep_name(ai_rep) if ai_rep else None
                    if not listing_val:
                        listing_ai = ai_result.get("listing")
                        if isinstance(listing_ai, str) and listing_ai.strip():
                            listing_val = listing_ai.strip()
                    if not capital_val:
                        capital_ai = ai_result.get("capital")
                        if isinstance(capital_ai, str) and capital_ai.strip():
                            capital_val = capital_ai.strip()
                    if not revenue_val:
                        revenue_ai = ai_result.get("revenue")
                        if isinstance(revenue_ai, str) and revenue_ai.strip():
                            revenue_val = revenue_ai.strip()
                    if not profit_val:
                        profit_ai = ai_result.get("profit")
                        if isinstance(profit_ai, str) and profit_ai.strip():
                            profit_val = profit_ai.strip()
                    if not fiscal_val:
                        fiscal_ai = ai_result.get("fiscal_month")
                        if isinstance(fiscal_ai, str) and fiscal_ai.strip():
                            fiscal_val = clean_fiscal_month(fiscal_ai)
                    if not founded_val:
                        founded_ai = ai_result.get("founded_year")
                        if isinstance(founded_ai, str) and founded_ai.strip():
                            founded_val = founded_ai.strip()
                    description = ai_result.get("description")
                    if isinstance(description, str) and description.strip():
                        description_val = description.strip()[:50]
                else:
                    if ai_attempted and AI_COOLDOWN_SEC > 0:
                        await asyncio.sleep(jittered_seconds(AI_COOLDOWN_SEC, JITTER_RATIO))
                need_listing = not bool(listing_val)
                need_capital = not bool(capital_val)
                need_revenue = not bool(revenue_val)
                need_profit = not bool(profit_val)
                need_fiscal = not bool(fiscal_val)
                need_founded = not bool(founded_val)
                need_description = not bool(description_val)

                # AI 2回目: まだ欠損がある場合に、優先リンクから集めたテキストで再度問い合わせ
                if USE_AI and verifier is not None and priority_docs and not timed_out:
                    missing_fields = any([
                        not phone,
                        not found_address,
                        not rep_name_val,
                        need_description,
                        need_listing,
                        need_capital,
                        need_revenue,
                        need_profit,
                        need_fiscal,
                        need_founded,
                    ])
                    if missing_fields:
                        combined_text = "\n\n".join(v.get("text", "") or "" for v in priority_docs.values())
                        if combined_text.strip():
                            screenshot_payload = None
                            if info_url:
                                info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict)
                            screenshot_payload = (info_dict or {}).get("screenshot")
                            async def run_ai_verify2():
                                nonlocal ai_time_spent
                                ai_started = time.monotonic()
                                try:
                                    return await verifier.verify_info(combined_text, screenshot_payload, name, addr)
                                except Exception:
                                    return None
                                finally:
                                    ai_time_spent += time.monotonic() - ai_started
                            ai_attempted = True
                            ai_result2 = await run_ai_verify2()
                            if ai_result2:
                                ai_used = 1
                                ai_model = AI_MODEL_NAME
                                ai_phone2 = normalize_phone(ai_result2.get("phone_number"))
                                ai_addr2 = normalize_address(ai_result2.get("address"))
                                ai_rep2 = ai_result2.get("rep_name") or ai_result2.get("representative")
                                ai_rep2 = scraper.clean_rep_name(ai_rep2) if ai_rep2 else None
                                desc2 = ai_result2.get("description")
                                if not listing_val:
                                    listing_ai2 = ai_result2.get("listing")
                                    if isinstance(listing_ai2, str) and listing_ai2.strip():
                                        listing_val = listing_ai2.strip()
                                if not capital_val:
                                    capital_ai2 = ai_result2.get("capital")
                                    if isinstance(capital_ai2, str) and capital_ai2.strip():
                                        capital_val = capital_ai2.strip()
                                if not revenue_val:
                                    revenue_ai2 = ai_result2.get("revenue")
                                    if isinstance(revenue_ai2, str) and revenue_ai2.strip():
                                        revenue_val = revenue_ai2.strip()
                                if not profit_val:
                                    profit_ai2 = ai_result2.get("profit")
                                    if isinstance(profit_ai2, str) and profit_ai2.strip():
                                        profit_val = profit_ai2.strip()
                                if not fiscal_val:
                                    fiscal_ai2 = ai_result2.get("fiscal_month")
                                    if isinstance(fiscal_ai2, str) and fiscal_ai2.strip():
                                        fiscal_val = clean_fiscal_month(fiscal_ai2)
                                if not founded_val:
                                    founded_ai2 = ai_result2.get("founded_year")
                                    if isinstance(founded_ai2, str) and founded_ai2.strip():
                                        founded_val = founded_ai2.strip()
                                if ai_phone2 and not phone:
                                    phone = ai_phone2
                                    phone_source = "ai"
                                    src_phone = info_url
                                if ai_addr2 and not found_address:
                                    found_address = ai_addr2
                                    address_source = "ai"
                                    src_addr = info_url
                                if ai_rep2 and not rep_name_val:
                                    rep_name_val = ai_rep2
                                    src_rep = info_url
                                if isinstance(desc2, str) and desc2.strip() and not description_val:
                                    description_val = desc2.strip()[:50]
                                need_listing = not bool(listing_val)
                                need_capital = not bool(capital_val)
                                need_revenue = not bool(revenue_val)
                                need_profit = not bool(profit_val)
                                need_fiscal = not bool(fiscal_val)
                                need_founded = not bool(founded_val)
                                need_description = not bool(description_val)

                    if ai_phone:
                        phone = ai_phone
                        phone_source = "ai"
                        src_phone = info_url
                    elif rule_phone:
                        phone = rule_phone
                        phone_source = "rule"
                        if not src_phone:
                            src_phone = info_url
                    else:
                        phone = ""
                        phone_source = "none"

                    if ai_addr:
                        found_address = ai_addr
                        address_source = "ai"
                        src_addr = info_url
                    elif rule_address:
                        found_address = rule_address or ""
                        address_source = "rule" if rule_address else "none"
                        if rule_address and not src_addr:
                            src_addr = info_url
                    else:
                        found_address = ""
                        address_source = "none"

                    if ai_rep:
                        rep_name_val = ai_rep
                        src_rep = info_url
                    elif rule_rep:
                        rep_name_val = rule_rep
                        if not src_rep:
                            src_rep = info_url

                    # 欠落情報があれば浅く探索して補完
                    need_phone = not bool(phone)
                    need_addr = not bool(found_address)
                    need_rep = not bool(rep_name_val)
                    need_extra_fields = any([
                        need_listing, need_capital, need_revenue,
                        need_profit, need_fiscal, need_founded, need_description,
                    ])
                    related_page_limit = 5 if need_extra_fields else 3
                    if need_phone or need_addr or need_rep or need_extra_fields:
                        if over_time_limit():
                            timed_out = True
                            related = {}
                        else:
                            try:
                                more_pages = 3 if (need_phone or need_addr or need_rep or need_description) else 1
                                related = await scraper.crawl_related(
                                    homepage,
                                    need_phone,
                                    need_addr,
                                    need_rep,
                                    max_pages=related_page_limit + more_pages,
                                    max_hops=3 if (need_phone or need_addr or need_rep or need_description) else 2,
                                    need_listing=need_listing,
                                    need_capital=need_capital,
                                    need_revenue=need_revenue,
                                    need_profit=need_profit,
                                    need_fiscal=need_fiscal,
                                    need_founded=need_founded,
                                    need_description=need_description,
                                )
                            except Exception:
                                related = {}
                        for url, data in related.items():
                            text = data.get("text", "") or ""
                            html_content = data.get("html", "") or ""
                            cc = scraper.extract_candidates(text, html_content)
                            if need_phone and cc.get("phone_numbers"):
                                cand = normalize_phone(cc["phone_numbers"][0])
                                if cand:
                                    phone = cand
                                    phone_source = "rule"
                                    src_phone = url
                                    need_phone = False
                            if need_addr and cc.get("addresses"):
                                cand_addr = pick_best_address(addr, cc["addresses"])
                                if cand_addr:
                                    found_address = cand_addr
                                    address_source = "rule"
                                    src_addr = url
                                    need_addr = False
                            if need_rep and cc.get("rep_names"):
                                cand_rep = cc["rep_names"][0]
                                cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                                if cand_rep:
                                    rep_name_val = cand_rep
                                    src_rep = url
                                    need_rep = False
                            if need_listing and cc.get("listings"):
                                listing_val = (cc["listings"][0] or "").strip()
                                need_listing = not bool(listing_val)
                            if need_capital and cc.get("capitals"):
                                capital_val = (cc["capitals"][0] or "").strip()
                                need_capital = not bool(capital_val)
                            if need_revenue and cc.get("revenues"):
                                revenue_val = (cc["revenues"][0] or "").strip()
                                need_revenue = not bool(revenue_val)
                            if need_profit and cc.get("profits"):
                                profit_val = (cc["profits"][0] or "").strip()
                                need_profit = not bool(profit_val)
                            if need_fiscal and cc.get("fiscal_months"):
                                fiscal_val = clean_fiscal_month(cc["fiscal_months"][0])
                                need_fiscal = not bool(fiscal_val)
                            if need_founded and cc.get("founded_years"):
                                founded_val = (cc["founded_years"][0] or "").strip()
                                need_founded = not bool(founded_val)
                            if need_description and not description_val:
                                snippet = extract_description_snippet(text)
                                if snippet:
                                    description_val = snippet
                                    need_description = False
                            if not (
                                need_phone or need_addr or need_rep or need_listing or need_capital
                                or need_revenue or need_profit or need_fiscal or need_founded or need_description
                            ):
                                break

                    if homepage and (need_addr or not found_address) and not timed_out:
                        try:
                            extra_docs = await scraper.fetch_priority_documents(
                                homepage,
                                info_dict.get("html", ""),
                                max_links=12,
                            )
                        except Exception:
                            extra_docs = {}
                        for url, pdata in extra_docs.items():
                            priority_docs[url] = pdata
                            absorb_doc_data(url, pdata)

                    deep_phase_end = elapsed()
                    if timed_out:
                        verify_result = {"phone_ok": False, "address_ok": False}
                    else:
                        try:
                            verify_result = await scraper.verify_on_site(homepage, phone or None, found_address or None)
                        except Exception:
                            log.warning("[%s] verify_on_site 失敗", cid, exc_info=True)
                            verify_result = {"phone_ok": False, "address_ok": False}

                    matches = int(bool(verify_result.get("phone_ok"))) + int(bool(verify_result.get("address_ok")))
                    if matches == 2:
                        confidence = 1.0
                    elif matches == 1:
                        confidence = 0.8
                    else:
                        confidence = 0.4
                else:
                    if urls:
                        log.info("[%s] 公式サイト候補を判別できず -> 未保存", cid)
                    else:
                        log.info("[%s] 有効なホームページ候補なし。", cid)
                    company["rep_name"] = company.get("rep_name", "") or ""
                    company["description"] = company.get("description", "") or ""
                    confidence = 0.4

                # 公式サイトから取得できなかった指標は検索結果の非公式ページから補完
                if not phone or not found_address or (not rep_name_val and not homepage):
                    for url, data in fallback_cands:
                        if not phone and data.get("phone_numbers"):
                            cand = normalize_phone(data["phone_numbers"][0])
                            if cand:
                                phone = cand
                                phone_source = "rule"
                                src_phone = url
                        if not found_address and data.get("addresses"):
                            cand_addr = pick_best_address(addr, data["addresses"])
                            if cand_addr:
                                found_address = cand_addr
                                address_source = "rule"
                                src_addr = url
                        if not rep_name_val and data.get("rep_names"):
                            cand_rep = scraper.clean_rep_name(data["rep_names"][0])
                            if cand_rep:
                                rep_name_val = cand_rep
                                src_rep = url
                        if phone and found_address and rep_name_val:
                            break

                if not listing_val:
                    for url, data in fallback_cands:
                        values = data.get("listings") or []
                        if values:
                            listing_val = (values[0] or "").strip()
                            if listing_val:
                                break
                if not capital_val:
                    for url, data in fallback_cands:
                        values = data.get("capitals") or []
                        if values:
                            capital_val = (values[0] or "").strip()
                            if capital_val:
                                break
                if not revenue_val:
                    for url, data in fallback_cands:
                        values = data.get("revenues") or []
                        if values:
                            revenue_val = (values[0] or "").strip()
                            if revenue_val:
                                break
                if not profit_val:
                    for url, data in fallback_cands:
                        values = data.get("profits") or []
                        if values:
                            profit_val = (values[0] or "").strip()
                            if profit_val:
                                break
                if not fiscal_val:
                    for url, data in fallback_cands:
                        values = data.get("fiscal_months") or []
                        if values:
                            fiscal_val = clean_fiscal_month(values[0] or "")
                            if fiscal_val:
                                break
                if not founded_val:
                    for url, data in fallback_cands:
                        values = data.get("founded_years") or []
                        if values:
                            founded_val = (values[0] or "").strip()
                            if founded_val:
                                break

                normalized_found_address = normalize_address(found_address) if found_address else ""
                rep_name_val = scraper.clean_rep_name(rep_name_val) or ""
                description_val = clean_description_value(description_val)
                listing_val = clean_listing_value(listing_val)
                capital_val = clean_amount_value(capital_val)
                revenue_val = clean_amount_value(revenue_val)
                profit_val = clean_amount_value(profit_val)
                fiscal_val = clean_fiscal_month(fiscal_val)
                founded_val = clean_founded_year(founded_val)
                company.update({
                    "homepage": homepage,
                    "phone": phone or "",
                    "found_address": normalized_found_address,
                    "rep_name": rep_name_val,
                    "description": description_val,
                    "listing": listing_val,
                    "revenue": revenue_val,
                    "profit": profit_val,
                    "capital": capital_val,
                    "fiscal_month": fiscal_val,
                    "founded_year": founded_val,
                    "phone_source": phone_source,
                    "address_source": address_source,
                    "ai_used": ai_used,
                    "ai_model": ai_model,
                    "extract_confidence": confidence,
                    "source_url_phone": src_phone,
                    "source_url_address": src_addr,
                    "source_url_rep": src_rep,
                    "homepage_official_flag": homepage_official_flag,
                    "homepage_official_source": homepage_official_source,
                    "homepage_official_score": homepage_official_score,
                })

                if REFERENCE_CHECKER:
                    accuracy_payload = REFERENCE_CHECKER.evaluate(company)
                    if accuracy_payload:
                        company.update(accuracy_payload)

                status = "done" if homepage else "no_homepage"
                if timed_out:
                    company["error_code"] = "timeout"
                    status = "review"
                if not homepage and candidate_records:
                    status = "review"
                if status == "done" and found_address and not addr_compatible(addr, found_address):
                    status = "review"
                if status == "done" and not verify_result.get("phone_ok") and not verify_result.get("address_ok"):
                    status = "review"
                if force_review and status != "error":
                    status = "review"
                if status == "done" and chosen_domain_score and chosen_domain_score < 4 and homepage_official_source in ("ai", "cache", "rule"):
                    status = "review"
                if status == "review" and not verify_result.get("phone_ok") and not verify_result.get("address_ok"):
                    homepage = ""
                    homepage_official_flag = 0
                    homepage_official_source = ""
                    homepage_official_score = 0.0

                company.setdefault("error_code", "")

                total_elapsed = elapsed()
                if not search_phase_end:
                    search_phase_end = total_elapsed
                if not official_phase_end:
                    official_phase_end = search_phase_end
                if not deep_phase_end:
                    deep_phase_end = total_elapsed
                search_time = search_phase_end
                official_time = max(0.0, official_phase_end - search_phase_end)
                deep_time = max(0.0, deep_phase_end - official_phase_end)
                log.info(
                    "[%s] timings: search=%.1fs official=%.1fs deep=%.1fs ai=%.1fs total=%.1fs",
                    cid,
                    search_time,
                    official_time,
                    deep_time,
                    ai_time_spent,
                    total_elapsed,
                )
                try:
                    log_phase_metric(cid, "search", search_time, status, homepage, company.get("error_code", ""))
                    log_phase_metric(cid, "official", official_time, status, homepage, company.get("error_code", ""))
                    log_phase_metric(cid, "deep", deep_time, status, homepage, company.get("error_code", ""))
                    log_phase_metric(cid, "ai", ai_time_spent, status, homepage, company.get("error_code", ""))
                    log_phase_metric(cid, "total", total_elapsed, status, homepage, company.get("error_code", ""))
                except Exception:
                    log.debug("phase metrics skipped", exc_info=True)

                manager.save_company_data(company, status=status)
                log.info("[%s] 保存完了: status=%s elapsed=%.1fs (worker=%s)", cid, status, elapsed(), WORKER_ID)

                if csv_writer:
                    csv_writer.writerow({k: company.get(k, "") for k in CSV_FIELDNAMES})
                    csv_file.flush()

                processed += 1

            except Exception as e:
                log.error("[%s] エラー: %s (worker=%s)", cid, e, WORKER_ID, exc_info=True)
                manager.update_status(cid, "error")

            # 1社ごとのスリープ（±JITTERでレート制限/ドメイン集中回避）
            if SLEEP_BETWEEN_SEC > 0:
                await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))

    finally:
        if csv_file:
            csv_file.close()
        if hasattr(scraper, "close") and callable(getattr(scraper, "close")):
            try:
                await scraper.close()
            except Exception:
                log.warning("scraper.close() はスキップ（未実装または失敗）", exc_info=True)
        manager.close()
        log.info("全処理終了 (worker=%s)", WORKER_ID)

if __name__ == "__main__":
    asyncio.run(process())
