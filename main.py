# main.py
import asyncio
import os
import csv
import logging
import re
import random
import time
import html as html_mod
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from src.database_manager import DatabaseManager
from src.company_scraper import CompanyScraper, CITY_RE
from src.ai_verifier import AIVerifier, DEFAULT_MODEL as AI_MODEL_NAME, _normalize_amount as ai_normalize_amount
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
TIME_LIMIT_FETCH_ONLY = float(os.getenv("TIME_LIMIT_FETCH_ONLY", "30"))  # 公式未確定で候補取得フェーズ（0で無効）
TIME_LIMIT_WITH_OFFICIAL = float(os.getenv("TIME_LIMIT_WITH_OFFICIAL", "45"))  # 公式確定後、主要項目未充足（0で無効）
TIME_LIMIT_DEEP = float(os.getenv("TIME_LIMIT_DEEP", "60"))  # 深掘り専用の上限（公式確定後）（0で無効）
OFFICIAL_AI_USE_SCREENSHOT = os.getenv("OFFICIAL_AI_USE_SCREENSHOT", "true").lower() == "true"
SECOND_PASS_ENABLED = os.getenv("SECOND_PASS_ENABLED", "false").lower() == "true"
SECOND_PASS_RETRY_STATUSES = [s.strip() for s in os.getenv("SECOND_PASS_RETRY_STATUSES", "review,no_homepage,error").split(",") if s.strip()]
LONG_PAGE_TIMEOUT_MS = int(os.getenv("LONG_PAGE_TIMEOUT_MS", os.getenv("PAGE_TIMEOUT_MS", "9000")))
LONG_SLOW_PAGE_THRESHOLD_MS = int(os.getenv("LONG_SLOW_PAGE_THRESHOLD_MS", os.getenv("SLOW_PAGE_THRESHOLD_MS", "9000")))
LONG_TIME_LIMIT_FETCH_ONLY = float(os.getenv("LONG_TIME_LIMIT_FETCH_ONLY", os.getenv("TIME_LIMIT_FETCH_ONLY", "30")))
LONG_TIME_LIMIT_WITH_OFFICIAL = float(os.getenv("LONG_TIME_LIMIT_WITH_OFFICIAL", os.getenv("TIME_LIMIT_WITH_OFFICIAL", "45")))
LONG_TIME_LIMIT_DEEP = float(os.getenv("LONG_TIME_LIMIT_DEEP", os.getenv("TIME_LIMIT_DEEP", "60")))

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
    s = re.sub(r"(内線|ext|extension)\s*[:：]?\s*\d+$", "", s, flags=re.I)
    # ハイフン類を統一
    s = re.sub(r"[‐―－ー–—]+", "-", s)
    digits = re.sub(r"\D", "", s)
    if digits.startswith("81") and len(digits) >= 10:
        digits = "0" + digits[2:]
    # 国内番号は0始まりで10〜11桁のみ許容
    if not digits.startswith("0") or len(digits) not in (10, 11):
        return None
    m = re.search(r"^(0\d{1,4})(\d{2,4})(\d{3,4})$", digits)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.search(r"^(0\d{1,4})-?(\d{2,4})-?(\d{3,4})$", s)
    return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}" if m2 else None

def normalize_address(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip().replace("　", " ")
    # 全角英数字・記号を半角に寄せる
    s = s.translate(str.maketrans("０１２３４５６７８９－ー―‐／", "0123456789----/"))
    # 漢数字を簡易的に算用数字へ
    def convert_kanji_numbers(text: str) -> str:
        digit_map = {"〇": 0, "零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
        def repl(match: re.Match) -> str:
            chars = match.group(0)
            total = 0
            current = 0
            for ch in chars:
                if ch == "十":
                    current = max(current, 1) * 10
                else:
                    current = current * 10 + digit_map.get(ch, 0)
            return str(total + current)
        return re.sub(r"[〇零一二三四五六七八九十]+", repl, text)
    s = convert_kanji_numbers(s)
    s = re.sub(r"[‐―－ー]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    m = re.search(r"(\d{3}-\d{4})\s*(.*)", s)
    if m:
        body = m.group(2).strip()
        return f"〒{m.group(1)} {body}"
    return s if s else None


def looks_like_address(text: str | None) -> bool:
    """
    明らかに住所ではない文字列（例: 「企業理念」「企業紹介映像」など）を弾くための軽い判定。
    - 郵便番号 or 都道府県名が含まれていれば住所らしいとみなす
    """
    if not text:
        return False
    s = (text or "").strip()
    if not s:
        return False
    if ZIP_CODE_RE.search(s):
        return True
    has_pref = False
    try:
        has_pref = any(pref in s for pref in CompanyScraper.PREFECTURE_NAMES)
    except Exception:
        has_pref = False
    has_city = bool(CITY_RE.search(s))
    if has_pref and has_city:
        return True

    if (has_pref or has_city) and re.search(r"(丁目|番地|号)", s):
        return True
    if (has_pref or has_city) and re.search(r"(ビル|マンション)", s):
        return True
    return False

def addr_compatible(input_addr: str, found_addr: str) -> bool:
    input_addr = normalize_address(input_addr)
    found_addr = normalize_address(found_addr)
    if not input_addr or not found_addr:
        return True
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
    raw = (val or "").strip()
    if not raw:
        return ""
    # 従業員数など人員系の表記を除外
    if re.search(r"(従業員|社員|職員|スタッフ)\s*[0-9０-９]+", raw):
        return ""
    if re.search(r"[0-9０-９]+\s*(名|人)\b", raw):
        return ""
    # まず AI 側と同等の金額正規化ロジックを流用して数値＋「円」に統一を試みる
    try:
        normalized = ai_normalize_amount(raw)
    except Exception:
        normalized = None
    if isinstance(normalized, str) and normalized.strip():
        return normalized.strip()[:40]

    # フォールバック: 単位付きの金額部分だけを抽出して軽くクレンジング
    text = raw.replace("　", " ")
    m = re.search(r"([0-9０-９,\.]+(?:兆|億|百|十)?万円?|[0-9０-９,\.]+円)", text)
    if m:
        text = m.group(1)
    if not re.search(r"[0-9０-９]", text):
        return ""
    if not any(unit in text for unit in AMOUNT_ALLOWED_UNITS):
        return ""
    text = re.sub(r"[()（）]", "", text)
    text = re.sub(r"\s+", "", text)
    if len(text) > 40:
        text = text[:40]
    return text

def clean_description_value(val: str) -> str:
    text = html_mod.unescape((val or "").strip())
    text = re.sub(r"<[^>]+>", " ", text)
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    stripped = text.strip("・-—‐－ー")
    if "<" in stripped or "class=" in stripped or "svg" in stripped:
        return ""
    policy_blocks = (
        "方針",
        "ポリシー",
        "理念",
        "ビジョン",
        "挨拶",
        "ご挨拶",
        "メッセージ",
        "品質",
        "環境",
        "安全",
        "コンプライアンス",
        "情報セキュリティ",
    )
    if any(word in stripped for word in policy_blocks):
        return ""
    if stripped in GENERIC_DESCRIPTION_TERMS:
        return ""
    if len(stripped) < 6:
        return ""
    if re.fullmatch(r"(会社概要|事業概要|法人概要|沿革|会社案内|企業情報)", stripped):
        return ""
    # 事業内容を示すキーワードが全く無い場合だけ除外
    biz_keywords = (
        "事業", "製造", "開発", "販売", "提供", "サービス", "運営", "支援", "施工", "設計", "製作",
        "物流", "建設", "工事", "コンサル", "consulting", "solution", "ソリューション",
        "product", "製品", "プロダクト", "システム", "プラント", "加工", "レンタル", "運送",
    )
    if not any(k in stripped for k in biz_keywords):
        return ""
    return stripped[:80]

def clean_fiscal_month(val: str) -> str:
    text = (val or "").strip().replace("　", " ")
    if not text:
        return ""
    text = text.replace("期", "月").replace("末", "月")
    if re.fullmatch(r"[Qq][1-4]", text):
        qmap = {"Q1": "3月", "Q2": "6月", "Q3": "9月", "Q4": "12月"}
        return qmap.get(text.upper(), "")
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

def extract_meta_description(html: str | None) -> str | None:
    if not html:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None
    for attr in ("description", "og:description"):
        node = soup.find("meta", attrs={"name": attr}) or soup.find("meta", attrs={"property": attr})
        if node:
            content = node.get("content") or ""
            cleaned = clean_description_value(content)
            if cleaned:
                return cleaned
    return None

def extract_lead_description(html: str | None) -> str | None:
    if not html:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None
    candidates: list[str] = []
    for tag in soup.find_all(["h1", "h2", "p"], limit=8):
        text = tag.get_text(separator=" ", strip=True)
        cleaned = clean_description_value(text)
        if cleaned:
            candidates.append(cleaned)
    return candidates[0] if candidates else None

def extract_description_from_payload(payload: dict[str, Any]) -> str:
    text = payload.get("text", "") or ""
    html = payload.get("html", "") or ""
    snippet = extract_description_snippet(text)
    if snippet:
        return snippet
    meta = extract_meta_description(html)
    if meta:
        return meta
    lead = extract_lead_description(html)
    if lead:
        return lead
    return ""

def _sanitize_ai_text_block(text: str | None) -> str:
    if not text:
        return ""
    cleaned_lines: list[str] = []
    nav_keywords = (
        "copyright", "all rights reserved", "privacy policy", "サイトマップ", "sitemap",
        "recruit", "求人", "採用", "お問い合わせ", "アクセスマップ"
    )
    DIGIT_RE = re.compile(r"[0-9０-９]")
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in nav_keywords):
            if DIGIT_RE.search(line) or "〒" in line:
                pass
            else:
                continue
        cleaned_lines.append(line)
    result = " ".join(cleaned_lines)
    result = re.sub(r"\s+", " ", result).strip()
    if len(result) > 3500:
        result = result[:3500]
    return result

def build_ai_text_payload(*blocks: str) -> str:
    payloads = []
    for block in blocks:
        cleaned = _sanitize_ai_text_block(block)
        if cleaned:
            payloads.append(cleaned)
    joined = "\n\n".join(payloads)
    return joined[:4000]

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
    if record.get("force_ai_official"):
        return True
    rule_details = record.get("rule") or {}
    if rule_details.get("is_official"):
        return False
    domain_score = int(record.get("domain_score") or 0)
    host_token_hit = bool(record.get("host_token_hit"))
    if domain_score >= 4 and host_token_hit:
        return False
    if record.get("strong_domain_host"):
        return False
    score = float(rule_details.get("score") or 0.0)
    if rule_details.get("strong_domain") and score >= 4:
        return False
    return True


async def ensure_info_has_screenshot(
    scraper: CompanyScraper,
    url: str,
    info: dict[str, Any] | None,
    need_screenshot: bool = True,
) -> dict[str, Any]:
    info = info or {}
    if info.get("screenshot") or not need_screenshot:
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
        second_pass = False
        original_retry_statuses = list(manager.retry_statuses)
        if SECOND_PASS_ENABLED:
            manager.retry_statuses = []

        while True:
            if MAX_ROWS and processed >= MAX_ROWS:
                log.info("MAX_ROWS=%s に到達。", MAX_ROWS)
                break

            company = claim_next(manager)
            if not company:
                if SECOND_PASS_ENABLED and not second_pass:
                    # セカンドパス: 長めのタイムアウトで retry_statuses を再処理
                    log.info("キューが空です。セカンドパス開始（遅延許容モード）。")
                    second_pass = True
                    manager.retry_statuses = SECOND_PASS_RETRY_STATUSES
                    # タイムアウトを緩和
                    global TIME_LIMIT_FETCH_ONLY, TIME_LIMIT_WITH_OFFICIAL, TIME_LIMIT_DEEP
                    TIME_LIMIT_FETCH_ONLY = LONG_TIME_LIMIT_FETCH_ONLY
                    TIME_LIMIT_WITH_OFFICIAL = LONG_TIME_LIMIT_WITH_OFFICIAL
                    TIME_LIMIT_DEEP = LONG_TIME_LIMIT_DEEP
                    try:
                        scraper.page_timeout_ms = LONG_PAGE_TIMEOUT_MS
                        scraper.slow_page_threshold_ms = LONG_SLOW_PAGE_THRESHOLD_MS
                    except Exception:
                        pass
                    continue
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
            company_has_corp = any(suffix in name for suffix in CompanyScraper.CORP_SUFFIXES)

            def elapsed() -> float:
                return time.monotonic() - started_at

            def over_time_limit() -> bool:
                return TIME_LIMIT_SEC > 0 and elapsed() > TIME_LIMIT_SEC

            def over_fetch_limit() -> bool:
                return TIME_LIMIT_FETCH_ONLY > 0 and not homepage and elapsed() > TIME_LIMIT_FETCH_ONLY

            def over_after_official() -> bool:
                return TIME_LIMIT_WITH_OFFICIAL > 0 and homepage and elapsed() > TIME_LIMIT_WITH_OFFICIAL

            def over_deep_limit() -> bool:
                # 深掘りフェーズの専用上限（公式確定後）
                return TIME_LIMIT_DEEP > 0 and homepage and elapsed() > TIME_LIMIT_DEEP

            try:
                candidate_limit = SEARCH_CANDIDATE_LIMIT
                company_tokens = scraper._company_tokens(name)  # type: ignore
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
                    domain_score_for_flag = scraper._domain_score(company_tokens, url_for_flag)  # type: ignore
                    if flag_info and flag_info.get("is_official") is False:
                        log.info("[%s] 既知の非公式URLを除外: %s (domain_score=%s)", cid, candidate, domain_score_for_flag)
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
                            "order_idx": idx,
                            "search_rank": idx,
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

                if candidate_records:
                    for record in candidate_records:
                        normalized_url = record.get("normalized_url") or record.get("url") or ""
                        domain_score_val = scraper._domain_score(company_tokens, normalized_url)  # type: ignore
                        host_token_hit = scraper._host_token_hit(company_tokens, normalized_url)  # type: ignore
                        record["domain_score"] = domain_score_val
                        record["host_token_hit"] = host_token_hit
                        record["strong_domain_host"] = company_has_corp and domain_score_val >= 4 and host_token_hit
                        record.setdefault("order_idx", 0)
                        record.setdefault("search_rank", 0)
                    candidate_records.sort(
                        key=lambda rec: (
                            not rec.get("strong_domain_host", False),
                            -int(rec.get("domain_score") or 0),
                            rec.get("order_idx", 0),
                        )
                    )
                    top3_ranked = sorted(candidate_records, key=lambda r: r.get("search_rank", 1e9))[:3]
                    for r in top3_ranked:
                        r["force_ai_official"] = True

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

                    async def run_official_ai(record: dict[str, Any]) -> dict[str, Any] | None:
                        nonlocal ai_official_attempted, ai_time_spent
                        async with ai_sem:
                            normalized_for_ai = record.get("normalized_url") or record.get("url") or ""
                            domain_score = int(record.get("domain_score") or 0)
                            if normalized_for_ai and domain_score == 0:
                                domain_score = scraper._domain_score(company_tokens, normalized_for_ai)  # type: ignore
                                record["domain_score"] = domain_score
                            info_payload = record.get("info") or {}
                            info_payload = await ensure_info_has_screenshot(
                                scraper,
                                record.get("url"),
                                info_payload,
                                need_screenshot=OFFICIAL_AI_USE_SCREENSHOT,
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
                                return None
                            ai_time_spent += time.monotonic() - ai_started
                            ai_official_attempted = True
                            if ai_verdict and ai_verdict.get("is_official") and domain_score < 4 and not record.get("rule", {}).get("address_match"):
                                ai_verdict["is_official"] = False
                            return ai_verdict

                    def _attach_ai_task(record: dict[str, Any]) -> None:
                        if not record_needs_official_ai(record):
                            return
                        task = asyncio.create_task(run_official_ai(record))
                        ai_tasks.append(task)

                        def _on_done(t: asyncio.Task, rec: dict[str, Any] = record) -> None:
                            try:
                                verdict = t.result()
                            except Exception:
                                return
                            if verdict:
                                rec["ai_judge"] = verdict

                        task.add_done_callback(_on_done)
                        task.add_done_callback(lambda t: t.exception() if t.done() else None)

                    for record in candidate_records[:3]:
                        _attach_ai_task(record)

                for record in candidate_records:
                    normalized_url = record.get("normalized_url") or record.get("url")
                    extracted = record.get("extracted") or {}
                    rule_details = record.get("rule") or {}
                    domain_score = int(record.get("domain_score") or 0)
                    if normalized_url and domain_score == 0:
                        domain_score = scraper._domain_score(company_tokens, normalized_url)  # type: ignore
                        record["domain_score"] = domain_score
                    host_token_hit = bool(record.get("host_token_hit"))
                    strong_domain_host = bool(record.get("strong_domain_host"))
                    addr_hit = bool(rule_details.get("address_match"))
                    pref_hit = bool(rule_details.get("prefecture_match"))
                    zip_hit = bool(rule_details.get("postal_code_match"))
                    address_ok = (not addr) or addr_hit or pref_hit or zip_hit
                    name_hit = bool(rule_details.get("name_present")) or domain_score >= 5
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
                            ai_override = (
                                strong_domain_host
                                or rule_details.get("name_present")
                                or address_ok
                            )
                            if ai_override:
                                log.info("[%s] AI非公式だが強ドメイン/名称/住所一致のためAI否定を無視: %s", cid, record.get("url"))
                            else:
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
                            name_or_domain_ok = (
                                name_hit
                                or strong_domain_host
                                or rule_details.get("strong_domain")
                                or domain_score >= 5
                            )
                            if not (name_or_domain_ok or address_ok):
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="name_domain_mismatch",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式でも名称/ドメイン一致弱のためスキップ: %s", cid, record.get("url"))
                                continue
                            if domain_score < 3 and not strong_domain_host and not rule_details.get("strong_domain") and not address_ok:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="domain_score_low",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式でもドメインスコア不足のためスキップ: %s", cid, record.get("url"))
                                continue
                            homepage = normalized_url
                            info = record.get("info")
                            primary_cands = extracted
                            homepage_official_flag = 1
                            homepage_official_source = "ai"
                            homepage_official_score = float(rule_details.get("score") or 0.0)
                            chosen_domain_score = domain_score
                            if not name_hit and not address_ok:
                                force_review = True
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=True,
                                source="ai",
                                reason=ai_judge.get("reason", ""),
                                confidence=ai_judge.get("confidence"),
                            )
                            break
                    # キャッシュ公式は採用しない（参考のみ）
                    if rule_details.get("is_official"):
                        name_or_domain_ok = (
                            name_hit
                            or strong_domain_host
                            or rule_details.get("strong_domain")
                            or domain_score >= 5
                        )
                        if not (name_or_domain_ok or address_ok):
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason="name_domain_mismatch",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] 名称/ドメイン一致弱のため公式判定を除外: %s", cid, record.get("url"))
                            continue
                        if domain_score < 3 and not strong_domain_host and not rule_details.get("strong_domain") and not address_ok:
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason=f"weak_domain_score={domain_score}",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] 低ドメイン一致のため公式判定を見送り: %s", cid, record.get("url"))
                            continue
                        if not address_ok and not name_hit:
                            force_review = True
                            log.info("[%s] 名称/住所一致弱いが公式扱い→review: %s", cid, record.get("url"))
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
                best_record: dict[str, Any] | None = None
                if not homepage and candidate_records:
                    best_score = float("-inf")
                    for record in candidate_records:
                        normalized_url = record.get("normalized_url") or record.get("url")
                        rule_details = record.get("rule") or {}
                        domain_score = int(record.get("domain_score") or 0)
                        if normalized_url and domain_score == 0:
                            domain_score = scraper._domain_score(company_tokens, normalized_url)  # type: ignore
                            record["domain_score"] = domain_score
                        strong_domain_host = bool(record.get("strong_domain_host"))
                        addr_hit = bool(rule_details.get("address_match"))
                        pref_hit = bool(rule_details.get("prefecture_match"))
                        zip_hit = bool(rule_details.get("postal_code_match"))
                        address_ok = (not addr) or addr_hit or pref_hit or zip_hit
                        score = (
                            domain_score * 2
                            + (3 if address_ok else 0)
                            + float(rule_details.get("score") or 0.0)
                            + (4 if strong_domain_host else 0)
                        )
                        if score > best_score:
                            best_score = score
                            best_record = record
                    if best_record:
                        normalized_url = best_record.get("normalized_url") or best_record.get("url") or ""
                        rule_details = best_record.get("rule") or {}
                        domain_score = int(best_record.get("domain_score") or 0)
                        if normalized_url and domain_score == 0:
                            domain_score = scraper._domain_score(company_tokens, normalized_url)  # type: ignore
                            best_record["domain_score"] = domain_score
                        strong_domain_host = bool(best_record.get("strong_domain_host"))
                        name_present = bool(rule_details.get("name_present"))
                        strong_domain = bool(rule_details.get("strong_domain"))
                        addr_hit = bool(rule_details.get("address_match"))
                        pref_hit = bool(rule_details.get("prefecture_match"))
                        zip_hit = bool(rule_details.get("postal_code_match"))
                        address_ok = addr_hit or pref_hit or zip_hit
                        safe_pick = strong_domain_host or domain_score >= 2 or name_present or strong_domain or address_ok
                        provisional_homepage = normalized_url
                        provisional_info = best_record.get("info")
                        provisional_cands = best_record.get("extracted") or {}
                        provisional_domain_score = domain_score
                        if safe_pick:
                            log.info("[%s] 公式未確定のため暫定ホームページで深掘り: %s", cid, provisional_homepage)
                        else:
                            force_review = True
                            homepage_official_source = "provisional_unsafe"
                            log.info("[%s] 公式未確定だが深掘りターゲットとして暫定採用(要レビュー): %s", cid, provisional_homepage)

                if not homepage and provisional_homepage:
                    homepage = provisional_homepage
                    info = provisional_info
                    primary_cands = provisional_cands
                    homepage_official_flag = 0
                    if not homepage_official_source:
                        homepage_official_source = "provisional"
                    homepage_official_score = 0.0
                    chosen_domain_score = provisional_domain_score
                    force_review = True

                if not homepage and timed_out and best_record:
                    normalized_url = best_record.get("normalized_url") or best_record.get("url") or ""
                    if normalized_url:
                        rule_details = best_record.get("rule") or {}
                        domain_score = int(best_record.get("domain_score") or 0)
                        host_token_hit = bool(best_record.get("host_token_hit"))
                        strong_domain_host = bool(best_record.get("strong_domain_host"))
                        name_present = bool(rule_details.get("name_present"))
                        strong_domain = bool(rule_details.get("strong_domain"))
                        addr_hit = bool(rule_details.get("address_match"))
                        pref_hit = bool(rule_details.get("prefecture_match"))
                        zip_hit = bool(rule_details.get("postal_code_match"))
                        address_ok = addr_hit or pref_hit or zip_hit
                        if host_token_hit or domain_score >= 2 or name_present or strong_domain or address_ok:
                            log.info("[%s] タイムアウトで暫定公式として保存: %s", cid, normalized_url)
                            homepage = normalized_url
                            info = best_record.get("info")
                            primary_cands = best_record.get("extracted") or {}
                            homepage_official_flag = 0
                            homepage_official_source = homepage_official_source or "provisional_timeout"
                            homepage_official_score = float(rule_details.get("score") or 0.0)
                            chosen_domain_score = domain_score
                            force_review = True

                official_phase_end = elapsed()
                priority_docs: dict[str, dict[str, Any]] = {}

                phone = ""
                found_address = ""
                rep_name_val = scraper.clean_rep_name(company.get("rep_name")) or ""
                # description は常に AI に生成させるため、既存値は参照しない
                description_val = ""
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
                verify_result_source = "none"
                confidence = 0.0
                rule_phone = None
                rule_address = None
                rule_rep = None

                need_listing = not bool(listing_val)
                need_capital = not bool(capital_val)
                need_revenue = not bool(revenue_val)
                need_profit = not bool(profit_val)
                need_fiscal = not bool(fiscal_val)
                need_founded = not bool(founded_val)
                need_description = not bool(description_val)

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
                        candidate = (cc["listings"][0] or "").strip()
                        cleaned_listing = clean_listing_value(candidate)
                        if cleaned_listing:
                            listing_val = cleaned_listing
                            need_listing = False
                    if not capital_val and cc.get("capitals"):
                        candidate = (cc["capitals"][0] or "").strip()
                        cleaned_capital = clean_amount_value(candidate)
                        if cleaned_capital:
                            capital_val = cleaned_capital
                            need_capital = False
                    if not revenue_val and cc.get("revenues"):
                        candidate = (cc["revenues"][0] or "").strip()
                        cleaned_revenue = clean_amount_value(candidate)
                        if cleaned_revenue:
                            revenue_val = cleaned_revenue
                            need_revenue = False
                    if not profit_val and cc.get("profits"):
                        candidate = (cc["profits"][0] or "").strip()
                        cleaned_profit = clean_amount_value(candidate)
                        if cleaned_profit:
                            profit_val = cleaned_profit
                            need_profit = False
                    if not fiscal_val and cc.get("fiscal_months"):
                        cleaned_fiscal = clean_fiscal_month(cc["fiscal_months"][0] or "")
                        if cleaned_fiscal:
                            fiscal_val = cleaned_fiscal
                            need_fiscal = False
                    if not founded_val and cc.get("founded_years"):
                        cleaned_founded = clean_founded_year(cc["founded_years"][0] or "")
                        if cleaned_founded:
                            founded_val = cleaned_founded
                            need_founded = False

                def refresh_need_flags() -> tuple[int, int]:
                    nonlocal need_phone, need_addr, need_rep
                    nonlocal need_listing, need_capital, need_revenue
                    nonlocal need_profit, need_fiscal, need_founded, need_description
                    need_phone = not bool(phone or rule_phone)
                    need_addr = not bool(found_address or rule_address or addr)
                    need_rep = not bool(rep_name_val or rule_rep)
                    need_listing = not bool(listing_val)
                    need_capital = not bool(capital_val)
                    need_revenue = not bool(revenue_val)
                    need_profit = not bool(profit_val)
                    need_fiscal = not bool(fiscal_val)
                    need_founded = not bool(founded_val)
                    need_description = not bool(description_val)
                    missing_contact = int(need_phone) + int(need_addr) + int(need_rep)
                    missing_extra = sum([
                        int(need_listing), int(need_capital), int(need_revenue),
                        int(need_profit), int(need_fiscal), int(need_founded), int(need_description),
                    ])
                    return missing_contact, missing_extra

                def update_description_candidate(candidate: str | None) -> bool:
                    nonlocal description_val, need_description
                    if not candidate:
                        return False
                    cleaned = clean_description_value(candidate)
                    if not cleaned:
                        return False
                    banned_terms = (
                        "お問い合わせ",
                        "お問合せ",
                        "採用情報",
                        "求人",
                        "ニュース",
                        "お知らせ",
                        "アクセス",
                        "所在地",
                        "電話番号",
                        "メール",
                    )
                    if any(term in cleaned for term in banned_terms):
                        return False
                    lower = cleaned.lower()
                    if lower.startswith(("contact", "recruit", "news")):
                        return False
                    if "http://" in lower or "https://" in lower:
                        return False
                    if len(cleaned) < 10:
                        return False
                    if len(cleaned) > 120:
                        cleaned = cleaned[:120].rstrip()
                    if cleaned == description_val:
                        return False
                    description_val = cleaned
                    need_description = False
                    return True

                if homepage and info_dict:
                    absorb_doc_data(info_url, info_dict)

                missing_contact, missing_extra = refresh_need_flags()

                def quick_verify_from_docs(phone_val: str | None, addr_val: str | None) -> dict[str, Any]:
                    result = {"phone_ok": False, "address_ok": False}
                    phone_pat = scraper._phone_variants_regex(phone_val) if phone_val else None  # type: ignore
                    addr_key = CompanyScraper._addr_key(addr_val) if addr_val else ""
                    if not phone_pat and not addr_key:
                        return result

                    def check_text(text: str) -> None:
                        if phone_pat and not result["phone_ok"] and phone_pat.search(text):
                            result["phone_ok"] = True
                        if addr_key and not result["address_ok"]:
                            text_key = CompanyScraper._addr_key(text)
                            if addr_key and addr_key in text_key:
                                result["address_ok"] = True

                    payloads: list[dict[str, Any]] = []
                    if info_dict:
                        payloads.append(info_dict)
                    payloads.extend(priority_docs.values())
                    for payload in payloads:
                        text = (payload.get("text", "") or "")
                        if not text and payload.get("html"):
                            text = payload.get("html", "") or ""
                        if text:
                            check_text(text)
                        if result["phone_ok"] and result["address_ok"]:
                            break
                    return result

                fully_filled = False
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
                    if rule_rep:
                        if not rep_name_val or len(rule_rep) > len(rep_name_val):
                            rep_name_val = rule_rep
                        if not src_rep:
                            src_rep = info_url
                    if listings and not listing_val:
                        cleaned_listing = clean_listing_value(listings[0] or "")
                        if cleaned_listing:
                            listing_val = cleaned_listing
                    if capitals and not capital_val:
                        cleaned_capital = clean_amount_value(capitals[0] or "")
                        if cleaned_capital:
                            capital_val = cleaned_capital
                    if revenues and not revenue_val:
                        cleaned_revenue = clean_amount_value(revenues[0] or "")
                        if cleaned_revenue:
                            revenue_val = cleaned_revenue
                    if profits and not profit_val:
                        cleaned_profit = clean_amount_value(profits[0] or "")
                        if cleaned_profit:
                            profit_val = cleaned_profit
                    if fiscals and not fiscal_val:
                        cleaned_fiscal = clean_fiscal_month(fiscals[0] or "")
                        if cleaned_fiscal:
                            fiscal_val = cleaned_fiscal
                    if founded_years and not founded_val:
                        cleaned_founded = clean_founded_year(founded_years[0] or "")
                        if cleaned_founded:
                            founded_val = cleaned_founded

                    need_listing = not bool(listing_val)
                    need_capital = not bool(capital_val)
                    need_revenue = not bool(revenue_val)
                    need_profit = not bool(profit_val)
                    need_fiscal = not bool(fiscal_val)
                    need_founded = not bool(founded_val)
                    need_description = not bool(description_val)

                    missing_contact, missing_extra = refresh_need_flags()

                    if over_time_limit():
                        timed_out = True

                    fully_filled = homepage and missing_contact == 0 and missing_extra == 0

                    try:
                        # 深掘りは不足があるときだけ最小限に実施
                        priority_limit = 0
                        if not timed_out and not fully_filled:
                            priority_limit = 3 if missing_contact else (2 if missing_extra else 0)
                        site_docs = (
                            {}
                            if priority_limit == 0
                            else await scraper.fetch_priority_documents(
                                homepage,
                                info_dict.get("html", ""),
                                max_links=priority_limit,
                                concurrency=FETCH_CONCURRENCY,
                            )
                        )
                    except Exception:
                        site_docs = {}
                    for url, pdata in site_docs.items():
                        priority_docs[url] = pdata
                        absorb_doc_data(url, pdata)
                    if site_docs:
                        missing_contact, missing_extra = refresh_need_flags()

                ai_result = None
                ai_attempted = False
                ai_task: asyncio.Task | None = None

                missing_contact, missing_extra = refresh_need_flags()
                pre_ai_phone_ok = bool(rule_phone or phone)
                pre_ai_addr_ok = bool(rule_address or found_address)
                pre_ai_rep_ok = bool(rule_rep or rep_name_val)
                ai_needed = (
                    homepage
                    and USE_AI
                    and verifier is not None
                    and (missing_contact > 0 or missing_extra > 0 or not pre_ai_phone_ok or not pre_ai_addr_ok or not pre_ai_rep_ok)
                )
                if ai_needed and not timed_out:
                    ai_attempted = True
                    info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict, need_screenshot=OFFICIAL_AI_USE_SCREENSHOT)
                    info = info_dict
                    ai_text_payload = build_ai_text_payload(info_dict.get("text", ""))

                    async def run_ai_verify():
                        nonlocal ai_time_spent
                        ai_started = time.monotonic()
                        try:
                            res = await verifier.verify_info(
                                ai_text_payload,
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
                elif ai_needed and info_url and verifier is not None and USE_AI and not timed_out:
                    # 公式候補はあるがテキストが乏しい場合のフォールバック: 再取得してでもAIを1回回す
                    try:
                        info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict, need_screenshot=OFFICIAL_AI_USE_SCREENSHOT)
                        ai_started = time.monotonic()
                        ai_result = await verifier.verify_info(
                            build_ai_text_payload(info_dict.get("text", "")),
                            info_dict.get("screenshot"),
                            name,
                            addr,
                        )
                        ai_time_spent += time.monotonic() - ai_started
                        ai_attempted = True
                    except Exception:
                        ai_result = None
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
                            cleaned_ai_listing = clean_listing_value(listing_ai)
                            if cleaned_ai_listing:
                                listing_val = cleaned_ai_listing
                    if not capital_val:
                        capital_ai = ai_result.get("capital")
                        if isinstance(capital_ai, str) and capital_ai.strip():
                            cleaned_ai_capital = clean_amount_value(capital_ai)
                            if cleaned_ai_capital:
                                capital_val = cleaned_ai_capital
                    if not revenue_val:
                        revenue_ai = ai_result.get("revenue")
                        if isinstance(revenue_ai, str) and revenue_ai.strip():
                            cleaned_ai_revenue = clean_amount_value(revenue_ai)
                            if cleaned_ai_revenue:
                                revenue_val = cleaned_ai_revenue
                    if not profit_val:
                        profit_ai = ai_result.get("profit")
                        if isinstance(profit_ai, str) and profit_ai.strip():
                            cleaned_ai_profit = clean_amount_value(profit_ai)
                            if cleaned_ai_profit:
                                profit_val = cleaned_ai_profit
                    if not fiscal_val:
                        fiscal_ai = ai_result.get("fiscal_month")
                        if isinstance(fiscal_ai, str) and fiscal_ai.strip():
                            cleaned_ai_fiscal = clean_fiscal_month(fiscal_ai)
                            if cleaned_ai_fiscal:
                                fiscal_val = cleaned_ai_fiscal
                    if not founded_val:
                        founded_ai = ai_result.get("founded_year")
                        if isinstance(founded_ai, str) and founded_ai.strip():
                            cleaned_ai_founded = clean_founded_year(founded_ai)
                            if cleaned_ai_founded:
                                founded_val = cleaned_ai_founded
                    description = ai_result.get("description")
                    if isinstance(description, str) and description.strip():
                        update_description_candidate(description)
                else:
                    if ai_attempted and AI_COOLDOWN_SEC > 0:
                        await asyncio.sleep(jittered_seconds(AI_COOLDOWN_SEC, JITTER_RATIO))
                missing_contact, missing_extra = refresh_need_flags()
                if need_description:
                    payloads: list[dict[str, Any]] = []
                    if info_dict:
                        payloads.append(info_dict)
                    payloads.extend(priority_docs.values())
                    for pdata in payloads:
                        desc = extract_description_from_payload(pdata)
                        if desc:
                            description_val = desc
                            need_description = False
                            break

                # AI 2回目: まだ欠損がある場合に、優先リンクから集めたテキストで再度問い合わせ
                if USE_AI and verifier is not None and priority_docs and not timed_out:
                    provisional_contact_missing = int(not (phone or ai_phone or rule_phone)) + int(not (found_address or ai_addr or rule_address or addr)) + int(not (rep_name_val or ai_rep or rule_rep))
                    missing_extra = sum([
                        int(need_description),
                        int(need_listing),
                        int(need_capital),
                        int(need_revenue),
                        int(need_profit),
                        int(need_fiscal),
                        int(need_founded),
                    ])
                    missing_fields = (provisional_contact_missing > 0) or (missing_extra >= 2)
                    if missing_fields:
                        combined_sources = [v.get("text", "") or "" for v in priority_docs.values()]
                        combined_text = build_ai_text_payload(*combined_sources)
                        if combined_text.strip():
                            screenshot_payload = None
                            if info_url:
                                info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict, need_screenshot=OFFICIAL_AI_USE_SCREENSHOT)
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
                                        cleaned_ai_listing2 = clean_listing_value(listing_ai2)
                                        if cleaned_ai_listing2:
                                            listing_val = cleaned_ai_listing2
                                if not capital_val:
                                    capital_ai2 = ai_result2.get("capital")
                                    if isinstance(capital_ai2, str) and capital_ai2.strip():
                                        cleaned_ai_capital2 = clean_amount_value(capital_ai2)
                                        if cleaned_ai_capital2:
                                            capital_val = cleaned_ai_capital2
                                if not revenue_val:
                                    revenue_ai2 = ai_result2.get("revenue")
                                    if isinstance(revenue_ai2, str) and revenue_ai2.strip():
                                        cleaned_ai_revenue2 = clean_amount_value(revenue_ai2)
                                        if cleaned_ai_revenue2:
                                            revenue_val = cleaned_ai_revenue2
                                if not profit_val:
                                    profit_ai2 = ai_result2.get("profit")
                                    if isinstance(profit_ai2, str) and profit_ai2.strip():
                                        cleaned_ai_profit2 = clean_amount_value(profit_ai2)
                                        if cleaned_ai_profit2:
                                            profit_val = cleaned_ai_profit2
                                if not fiscal_val:
                                    fiscal_ai2 = ai_result2.get("fiscal_month")
                                    if isinstance(fiscal_ai2, str) and fiscal_ai2.strip():
                                        cleaned_ai_fiscal2 = clean_fiscal_month(fiscal_ai2)
                                        if cleaned_ai_fiscal2:
                                            fiscal_val = cleaned_ai_fiscal2
                                if not founded_val:
                                    founded_ai2 = ai_result2.get("founded_year")
                                    if isinstance(founded_ai2, str) and founded_ai2.strip():
                                        cleaned_ai_founded2 = clean_founded_year(founded_ai2)
                                        if cleaned_ai_founded2:
                                            founded_val = cleaned_ai_founded2
                                if ai_phone2 and not phone:
                                    phone = ai_phone2
                                    phone_source = "ai"
                                    src_phone = info_url
                                if ai_addr2 and not found_address:
                                    found_address = ai_addr2
                                    address_source = "ai"
                                    src_addr = info_url
                                if ai_rep2 and (not rep_name_val or len(ai_rep2) > len(rep_name_val)):
                                    rep_name_val = ai_rep2
                                    src_rep = info_url
                                if isinstance(desc2, str) and desc2.strip():
                                    update_description_candidate(desc2)

                    missing_contact, missing_extra = refresh_need_flags()
                    if need_description:
                        payloads: list[dict[str, Any]] = []
                        if info_dict:
                            payloads.append(info_dict)
                        payloads.extend(priority_docs.values())
                        for pdata in payloads:
                            desc = extract_description_from_payload(pdata)
                            if desc:
                                description_val = desc
                                need_description = False
                                break

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
                        if not rep_name_val or len(ai_rep) > len(rep_name_val):
                            rep_name_val = ai_rep
                            src_rep = info_url
                    elif rule_rep:
                        if not rep_name_val or len(rule_rep) > len(rep_name_val):
                            rep_name_val = rule_rep
                            if not src_rep:
                                src_rep = info_url

                    # 欠落情報があれば浅く探索して補完
                    missing_contact, missing_extra = refresh_need_flags()
                    need_extra_fields = missing_extra > 0
                    related_page_limit = 2 if missing_extra else 1
                    related = {}
                    if not timed_out and ((missing_contact > 0) or need_extra_fields):
                        if over_time_limit() or over_deep_limit():
                            timed_out = True
                        else:
                            more_pages = 1 if (need_phone or need_addr or need_rep or need_description) else 0
                            try:
                                related = await scraper.crawl_related(
                                    homepage,
                                    need_phone,
                                    need_addr,
                                    need_rep,
                                    max_pages=related_page_limit + more_pages,
                                    max_hops=2,
                                    need_listing=need_listing,
                                    need_capital=need_capital,
                                    need_revenue=need_revenue,
                                    need_profit=need_profit,
                                    need_fiscal=need_fiscal,
                                    need_founded=need_founded,
                                    need_description=need_description,
                                    initial_info=info_dict if info_url == homepage else None,
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
                                cleaned_listing = clean_listing_value(cc["listings"][0] or "")
                                if cleaned_listing:
                                    listing_val = cleaned_listing
                                    need_listing = False
                            if need_capital and cc.get("capitals"):
                                cleaned_capital = clean_amount_value(cc["capitals"][0] or "")
                                if cleaned_capital:
                                    capital_val = cleaned_capital
                                    need_capital = False
                            if need_revenue and cc.get("revenues"):
                                cleaned_revenue = clean_amount_value(cc["revenues"][0] or "")
                                if cleaned_revenue:
                                    revenue_val = cleaned_revenue
                                    need_revenue = False
                            if need_profit and cc.get("profits"):
                                cleaned_profit = clean_amount_value(cc["profits"][0] or "")
                                if cleaned_profit:
                                    profit_val = cleaned_profit
                                    need_profit = False
                            if need_fiscal and cc.get("fiscal_months"):
                                cleaned_fiscal = clean_fiscal_month(cc["fiscal_months"][0] or "")
                                if cleaned_fiscal:
                                    fiscal_val = cleaned_fiscal
                                    need_fiscal = False
                            if need_founded and cc.get("founded_years"):
                                cleaned_founded = clean_founded_year(cc["founded_years"][0] or "")
                                if cleaned_founded:
                                    founded_val = cleaned_founded
                                    need_founded = False
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
                                max_links=3,
                                concurrency=FETCH_CONCURRENCY,
                            )
                        except Exception:
                            extra_docs = {}
                        for url, pdata in extra_docs.items():
                            priority_docs[url] = pdata
                            absorb_doc_data(url, pdata)

                    deep_phase_end = elapsed()
                    quick_verify_result = quick_verify_from_docs(
                        phone or rule_phone,
                        found_address or addr or rule_address,
                    )
                    verify_result = dict(quick_verify_result)
                    verify_result_source = "docs" if any(verify_result.values()) else "skip"
                    need_online_verify = (
                        not timed_out
                        and homepage
                        and (phone or rule_phone or found_address or addr or rule_address)
                        and (not verify_result.get("phone_ok") or not verify_result.get("address_ok"))
                        and not over_after_official()
                    )
                    if need_online_verify:
                        try:
                            verify_result = await scraper.verify_on_site(
                                homepage,
                                phone or rule_phone or None,
                                found_address or addr or rule_address or None,
                                fetch_limit=3,
                            )
                            verify_result_source = "online"
                        except Exception:
                            log.warning("[%s] verify_on_site 失敗", cid, exc_info=True)
                            verify_result = quick_verify_result
                            verify_result_source = "docs"

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

                # 公式サイトが無い場合のみ、検索結果の非公式ページから連絡先を補完
                if not homepage and (not phone or not found_address or not rep_name_val):
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
                            cleaned_fallback_listing = clean_listing_value(values[0] or "")
                            if cleaned_fallback_listing:
                                listing_val = cleaned_fallback_listing
                                break
                if not capital_val:
                    for url, data in fallback_cands:
                        values = data.get("capitals") or []
                        if values:
                            cleaned_fallback_capital = clean_amount_value(values[0] or "")
                            if cleaned_fallback_capital:
                                capital_val = cleaned_fallback_capital
                                break
                if not revenue_val:
                    for url, data in fallback_cands:
                        values = data.get("revenues") or []
                        if values:
                            cleaned_fallback_revenue = clean_amount_value(values[0] or "")
                            if cleaned_fallback_revenue:
                                revenue_val = cleaned_fallback_revenue
                                break
                if not profit_val:
                    for url, data in fallback_cands:
                        values = data.get("profits") or []
                        if values:
                            cleaned_fallback_profit = clean_amount_value(values[0] or "")
                            if cleaned_fallback_profit:
                                profit_val = cleaned_fallback_profit
                                break
                if not fiscal_val:
                    for url, data in fallback_cands:
                        values = data.get("fiscal_months") or []
                        if values:
                            cleaned_fallback_fiscal = clean_fiscal_month(values[0] or "")
                            if cleaned_fallback_fiscal:
                                fiscal_val = cleaned_fallback_fiscal
                                break
                if not founded_val:
                    for url, data in fallback_cands:
                        values = data.get("founded_years") or []
                        if values:
                            cleaned_fallback_founded = clean_founded_year(values[0] or "")
                            if cleaned_fallback_founded:
                                founded_val = cleaned_fallback_founded
                                break

                if found_address and not looks_like_address(found_address):
                    found_address = ""
                normalized_found_address = normalize_address(found_address) if found_address else ""
                if not normalized_found_address and addr:
                    normalized_found_address = normalize_address(addr) or ""
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

                had_verify_target = bool(
                    phone or rule_phone or found_address or rule_address or addr
                )

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
                if (
                    status == "done"
                    and had_verify_target
                    and not verify_result.get("phone_ok")
                    and not verify_result.get("address_ok")
                    and chosen_domain_score < 5
                ):
                    status = "review"
                if force_review and status != "error":
                    status = "review"
                if status == "done" and chosen_domain_score and chosen_domain_score < 4 and homepage_official_source in ("ai", "rule"):
                    status = "review"
                if status == "review" and homepage_official_source == "provisional" and not verify_result.get("phone_ok") and not verify_result.get("address_ok"):
                    # 暫定URLで検証できない場合でも、深掘り/AI結果があれば保持し、ホームページは空に戻さない
                    pass

                if status == "done":
                    strong_official = bool(
                        homepage
                        and homepage_official_flag == 1
                        and (chosen_domain_score or 0) >= 4
                        and (not addr or not found_address or addr_compatible(addr, found_address))
                        and (
                            not had_verify_target
                            or verify_result.get("phone_ok")
                            or verify_result.get("address_ok")
                        )
                    )
                    if not strong_official:
                        status = "review"

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
                    "[%s] timings: search=%.1fs official=%.1fs deep=%.1fs ai=%.1fs total=%.1fs verify=%s",
                    cid,
                    search_time,
                    official_time,
                    deep_time,
                    ai_time_spent,
                    total_elapsed,
                    verify_result_source,
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
