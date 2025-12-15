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
# 検索フェーズ全体の早期タイムアウト（0で無効）
SEARCH_PHASE_TIMEOUT_SEC = float(os.getenv("SEARCH_PHASE_TIMEOUT_SEC", "30"))
# 深掘りページ/ホップを環境変数で制御（デフォルトは軽め）
RELATED_BASE_PAGES = max(0, int(os.getenv("RELATED_BASE_PAGES", "1")))
RELATED_EXTRA_PHONE = max(0, int(os.getenv("RELATED_EXTRA_PHONE", "1")))
RELATED_MAX_HOPS_BASE = max(1, int(os.getenv("RELATED_MAX_HOPS_BASE", "2")))
RELATED_MAX_HOPS_PHONE = max(1, int(os.getenv("RELATED_MAX_HOPS_PHONE", "2")))
# 全体のタイムアウトは使わず、フェーズ別で管理する（デフォルトを短めにし停滞を防止）
TIME_LIMIT_SEC = float(os.getenv("TIME_LIMIT_SEC", "60"))
TIME_LIMIT_FETCH_ONLY = float(os.getenv("TIME_LIMIT_FETCH_ONLY", "10"))  # 公式未確定で候補取得フェーズ（0で無効）
TIME_LIMIT_WITH_OFFICIAL = float(os.getenv("TIME_LIMIT_WITH_OFFICIAL", "40"))  # 公式確定後、主要項目未充足（0で無効）
TIME_LIMIT_DEEP = float(os.getenv("TIME_LIMIT_DEEP", "45"))  # 深掘り専用の上限（公式確定後）（0で無効）
# 全体のハード上限（デフォルト60秒で次ジョブへスキップ）
GLOBAL_HARD_TIMEOUT_SEC = float(os.getenv("GLOBAL_HARD_TIMEOUT_SEC", "60"))
# 単社処理のハード上限（candidate取得で固まるのを避けるための保険、0で無効）
COMPANY_HARD_TIMEOUT_SEC = float(os.getenv("COMPANY_HARD_TIMEOUT_SEC", "60"))
OFFICIAL_AI_USE_SCREENSHOT = os.getenv("OFFICIAL_AI_USE_SCREENSHOT", "true").lower() == "true"
AI_ADDRESS_ENABLED = os.getenv("AI_ADDRESS_ENABLED", "false").lower() == "true"
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
ADDRESS_JS_NOISE_RE = re.compile(
    r"(window\.\w+|dataLayer\s*=|gtm\.|googletagmanager|nr-data\.net|newrelic|bam\.nr-data\.net|function\s*\(|<script|</script>)",
    re.IGNORECASE,
)
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
DESCRIPTION_MIN_LEN = max(6, int(os.getenv("DESCRIPTION_MIN_LEN", "10")))
DESCRIPTION_MAX_LEN = max(DESCRIPTION_MIN_LEN, int(os.getenv("DESCRIPTION_MAX_LEN", "200")))
DESCRIPTION_BIZ_KEYWORDS = (
    "事業", "製造", "開発", "販売", "提供", "サービス", "運営", "支援", "施工", "設計", "製作",
    "物流", "建設", "工事", "コンサル", "consulting", "solution", "ソリューション",
    "product", "製品", "プロダクト", "システム", "プラント", "加工", "レンタル", "運送",
    "IT", "デジタル", "ITソリューション", "プロジェクト", "アウトソーシング", "研究", "技術",
    "人材", "教育", "ヘルスケア", "医療", "食品", "エネルギー", "不動産", "金融", "EC", "通販",
    "プラットフォーム", "クラウド", "SaaS", "DX", "AI", "データ分析", "セキュリティ", "インフラ",
    "基盤", "ソフトウェア", "ハードウェア", "ロボット", "IoT", "モビリティ", "物流DX",
)

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
    s = re.sub(r"<[^>]+>", " ", s)
    # CSSスタイル断片の除去（スクレイプ時に混入する background: などを落とす）
    s = re.sub(r"(background|color|font-family|font-size|display|position)\s*:\s*[^;]+;?", " ", s, flags=re.I)
    # JSやトラッキング断片をカット（window.dataLayer 等が混入するケース対策）
    m_noise = ADDRESS_JS_NOISE_RE.search(s)
    if m_noise:
        s = s[: m_noise.start()]
    # 全角英数字・記号を半角に寄せる
    s = s.translate(str.maketrans("０１２３４５６７８９－ー―‐／", "0123456789----/"))
    # 漢数字を簡易的に算用数字へ
    def convert_kanji_numbers(text: str) -> str:
        digit_map = {"〇": 0, "零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
        unit_map = {"十": 10, "百": 100, "千": 1000}

        def repl(match: re.Match) -> str:
            chars = match.group(0)
            total = 0
            current = 0
            for ch in chars:
                if ch in unit_map:
                    base = current if current > 0 else 1
                    total += base * unit_map[ch]
                    current = 0
                else:
                    current = current * 10 + digit_map.get(ch, 0)
            total += current
            return str(total)

        # 丁目/番地/号などの直前に現れる数のみ変換し、地名（千代田/三郷など）を壊さない
        pattern = re.compile(r"[〇零一二三四五六七八九十百千]+(?=(丁目|番地|番|号|条|[-‐―ー−/0-9]))")
        return pattern.sub(repl, text)
    s = convert_kanji_numbers(s)
    s = re.sub(r"[‐―－ー]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    m = re.search(r"(\d{3}-\d{4})\s*(.*)", s)
    if m:
        body = m.group(2).strip()
        # 郵便番号だけの場合は住所とみなさない
        if not body:
            return None
        return f"〒{m.group(1)} {body}"
    return s if s else None


def sanitize_text_block(text: str | None) -> str:
    """
    軽量なサニタイズ: HTMLタグ除去・制御文字除去・空白圧縮。
    住所や説明など、DBに入れる前に通してノイズを落とす。
    """
    if not text:
        return ""
    t = html_mod.unescape(str(text))
    t = t.replace("[TABLE]", "")
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\bbr\s*/?\b", " ", t, flags=re.I)
    t = re.sub(r'\b(?:class|id|style|data-[\w-]+)\s*=\s*"[^"]*"', " ", t, flags=re.I)
    t = t.replace(">", " ").replace("<", " ")
    t = t.replace("|", " ").replace("｜", " ")
    t = re.sub(r"[\r\n\t]+", " ", t)
    t = re.sub(r"[\x00-\x1f\x7f]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


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
        # 郵便番号/市区町村/丁目を多く含むものを優先
        def _score(addr: str) -> int:
            score = 0
            if ZIP_CODE_RE.search(addr):
                score += 6
            if CITY_RE.search(addr):
                score += 4
            if re.search(r"丁目|番地|号", addr):
                score += 2
            if re.search(r"(ビル|マンション)", addr):
                score += 1
            return score
        normalized_candidates.sort(key=lambda a: (_score(a), len(a)), reverse=True)
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
        if CITY_RE.search(cand):
            score += 3
        if re.search(r"(丁目|番地|号)", cand):
            score += 2
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

def pick_best_phone(candidates: list[str]) -> str | None:
    best = None
    for cand in candidates:
        norm = normalize_phone(cand)
        if not norm:
            continue
        is_table = isinstance(cand, str) and cand.startswith("[TABLE]")
        norm_val = norm
        # prefer non-navi numbers over 0120/0570
        if best is None:
            best = norm_val
            continue
        if re.match(r"^(0120|0570)", norm_val) and not re.match(r"^(0120|0570)", best):
            continue
        if re.match(r"^(0120|0570)", best) and not re.match(r"^(0120|0570)", norm_val):
            best = norm_val
            continue
        # prefer table-derived
        if is_table and not (isinstance(best, str) and best.startswith("[TABLE]")):
            best = norm_val
            continue
        # prefer standard 12-13 chars (including hyphens)
        if len(norm_val) == len(best):
            continue
        if abs(len(norm_val) - 12) < abs(len(best) - 12):
            best = norm_val
    return best

def pick_best_rep(names: list[str], source_url: str | None = None) -> str | None:
    role_keywords = ("代表", "取締役", "社長", "理事長", "会長", "院長", "学長", "園長", "代表社員", "CEO", "COO")
    blocked = ("スタッフ", "紹介", "求人", "採用", "ニュース", "退任", "就任", "人事", "異動", "お知らせ", "プレス", "取引")
    url_bonus = 3 if source_url and any(seg in source_url for seg in ("/company", "/about", "/corporate")) else 0
    best = None
    best_score = float("-inf")
    for raw in names:
        if not raw:
            continue
        cleaned = str(raw).strip()
        is_table = cleaned.startswith("[TABLE]")
        if is_table:
            cleaned = cleaned.replace("[TABLE]", "", 1).strip()
        if not cleaned:
            continue
        if any(b in cleaned for b in blocked):
            continue
        score = len(cleaned) + url_bonus
        if is_table:
            score += 5
        if any(k in cleaned for k in role_keywords):
            score += 8
        if score > best_score:
            best_score = score
            best = cleaned
    return best

def _score_amount_for_choice(val: str) -> int:
    if not val:
        return -10
    # prefer larger units/longer numbers up to 20 chars
    score = min(len(val), 20)
    if "兆" in val:
        score += 6
    if "億" in val:
        score += 4
    if "万" in val:
        score += 2
    return score

def pick_best_amount(candidates: list[str]) -> str | None:
    best = None
    best_score = float("-inf")
    for cand in candidates:
        is_table = False
        value = cand
        if isinstance(cand, str) and cand.startswith("[TABLE]"):
            is_table = True
            value = cand.replace("[TABLE]", "", 1)
        cleaned = clean_amount_value(value)
        if not cleaned:
            continue
        score = _score_amount_for_choice(cleaned)
        if is_table:
            score += 3
        if score > best_score:
            best_score = score
            best = cleaned
    return best

def pick_best_listing(candidates: list[str]) -> str | None:
    best = None
    best_len = -1
    for cand in candidates:
        is_table = False
        value = cand
        if isinstance(cand, str) and cand.startswith("[TABLE]"):
            is_table = True
            value = cand.replace("[TABLE]", "", 1)
        cleaned = clean_listing_value(value)
        if not cleaned:
            continue
        # prefer shorter market labels / 4-digit codes
        effective_len = len(cleaned) - (2 if is_table else 0)
        if best is None or effective_len < best_len:
            best = cleaned
            best_len = effective_len
    return best

def select_relevant_paragraphs(text: str, limit: int = 3) -> str:
    """
    説明抽出用に、事業系キーワードを含む上位段落を抽出する。
    入力テキスト全体をAIに渡さずに短縮し、時間を抑える。
    """
    if not text:
        return ""
    paragraphs = [p.strip() for p in re.split(r"[\r\n]+", text) if p.strip()]
    if not paragraphs:
        return ""
    biz_keywords = (
        "事業", "サービス", "製造", "開発", "販売", "提供", "運営", "支援",
        "ソリューション", "product", "製品", "システム", "物流", "建設", "工事",
        "コンサル", "研究", "技術", "教育", "医療", "食品", "エネルギー", "不動産",
    )
    scored: list[tuple[int, str]] = []
    for para in paragraphs:
        score = 0
        for kw in biz_keywords:
            if kw.lower() in para.lower():
                score += 2
        score += min(len(para), 200) // 50  # 長さで軽くスコア
        scored.append((score, para))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [p for _, p in scored[:limit]]
    return "\n".join(top)

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


def _truncate_description(text: str) -> str:
    if len(text) <= DESCRIPTION_MAX_LEN:
        return text
    truncated = text[:DESCRIPTION_MAX_LEN]
    truncated = re.sub(r"[、。．,;]+$", "", truncated)
    trimmed = re.sub(r"\s+\S*$", "", truncated).strip()
    return trimmed if len(trimmed) >= DESCRIPTION_MIN_LEN else truncated.rstrip()


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
    if len(stripped) < DESCRIPTION_MIN_LEN:
        return ""
    if re.fullmatch(r"(会社概要|事業概要|法人概要|沿革|会社案内|企業情報)", stripped):
        return ""
    # 事業内容を示すキーワードが全く無い場合だけ除外
    if not any(k in stripped for k in DESCRIPTION_BIZ_KEYWORDS):
        return ""
    return _truncate_description(stripped)

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
    base_page_timeout_ms = getattr(scraper, "page_timeout_ms", 9000) or 9000
    PAGE_FETCH_TIMEOUT_SEC = max(5.0, (base_page_timeout_ms / 1000.0) + 5.0)
    # CompanyScraper に start()/close() が無い実装でも動くように安全に呼ぶ
    if hasattr(scraper, "start") and callable(getattr(scraper, "start")):
        try:
            await scraper.start()
        except Exception:
            log.warning("scraper.start() はスキップ（未実装または失敗）", exc_info=True)

    verifier = AIVerifier() if USE_AI else None
    manager = DatabaseManager(worker_id=WORKER_ID)

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
            input_addr_has_zip = bool(ZIP_CODE_RE.search(addr))
            input_addr_has_city = bool(CITY_RE.search(addr))
            input_addr_has_pref = any(pref in addr for pref in CompanyScraper.PREFECTURE_NAMES) if addr else False
            input_addr_pref_only = bool(input_addr_has_pref and not input_addr_has_city and not input_addr_has_zip)

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
            hard_timeout_candidates = [t for t in (GLOBAL_HARD_TIMEOUT_SEC, COMPANY_HARD_TIMEOUT_SEC, TIME_LIMIT_SEC) if t > 0]
            hard_timeout_sec = min(hard_timeout_candidates) if hard_timeout_candidates else 60.0
            hard_deadline = started_at + hard_timeout_sec

            def elapsed() -> float:
                return time.monotonic() - started_at

            def over_time_limit() -> bool:
                if TIME_LIMIT_SEC > 0 and elapsed() > TIME_LIMIT_SEC:
                    return True
                if COMPANY_HARD_TIMEOUT_SEC > 0 and elapsed() > COMPANY_HARD_TIMEOUT_SEC:
                    return True
                if GLOBAL_HARD_TIMEOUT_SEC > 0 and elapsed() > GLOBAL_HARD_TIMEOUT_SEC:
                    return True
                return False

            def over_hard_deadline() -> bool:
                return time.monotonic() > hard_deadline

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
                try:
                    if SEARCH_PHASE_TIMEOUT_SEC > 0:
                        urls = await asyncio.wait_for(
                            scraper.search_company(name, addr, num_results=candidate_limit),
                            timeout=SEARCH_PHASE_TIMEOUT_SEC,
                        )
                    else:
                        urls = await scraper.search_company(name, addr, num_results=candidate_limit)
                except asyncio.TimeoutError:
                    log.info(
                        "[%s] search_company timeout (%.1fs) -> review",
                        cid,
                        SEARCH_PHASE_TIMEOUT_SEC,
                    )
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
                        "error_code": "search_timeout",
                    })
                    manager.save_company_data(company, status="review")
                    if csv_writer:
                        csv_writer.writerow({k: company.get(k, "") for k in CSV_FIELDNAMES})
                        csv_file.flush()
                    processed += 1
                    try:
                        await scraper.reset_context()
                    except Exception:
                        pass
                    if SLEEP_BETWEEN_SEC > 0:
                        await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))
                    continue
                url_flags_map, host_flags_map = manager.get_url_flags_batch(urls)
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
                    normalized_flag_url, host_for_flag = manager._normalize_flag_target(candidate)
                    flag_info = url_flags_map.get(normalized_flag_url)
                    if not flag_info and host_for_flag:
                        flag_info = host_flags_map.get(host_for_flag)
                    domain_score_for_flag = scraper._domain_score(company_tokens, url_for_flag)  # type: ignore
                    if flag_info and flag_info.get("is_official") is False:
                        log.info("[%s] 既知の非公式URLを除外: %s (domain_score=%s)", cid, candidate, domain_score_for_flag)
                        return None
                    async with fetch_sem:
                        try:
                            candidate_info = await asyncio.wait_for(
                                scraper.get_page_info(candidate),
                                timeout=PAGE_FETCH_TIMEOUT_SEC,
                            )
                        except asyncio.TimeoutError:
                            log.info("[%s] get_page_info timeout -> skip candidate: %s", cid, candidate)
                            return None
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
                    try:
                        if SEARCH_PHASE_TIMEOUT_SEC > 0:
                            prepared = await asyncio.wait_for(
                                asyncio.gather(*prepare_tasks, return_exceptions=True),
                                timeout=SEARCH_PHASE_TIMEOUT_SEC,
                            )
                        else:
                            prepared = await asyncio.gather(*prepare_tasks, return_exceptions=True)
                    except asyncio.TimeoutError:
                        for t in prepare_tasks:
                            t.cancel()
                        await asyncio.gather(*prepare_tasks, return_exceptions=True)
                        log.info("[%s] prepare_candidate timeout -> review/search_timeout", cid)
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
                            "error_code": "search_timeout",
                        })
                        manager.save_company_data(company, status="review")
                        if csv_writer:
                            csv_writer.writerow({k: company.get(k, "") for k in CSV_FIELDNAMES})
                            csv_file.flush()
                        processed += 1
                        try:
                            await scraper.reset_context()
                        except Exception:
                            pass
                        if SLEEP_BETWEEN_SEC > 0:
                            await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))
                        continue
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
                        # 公式候補の強さを上げる: 社名トークンがホストに入りドメインスコアが高ければ AI 否定を上書きできるようにする
                        record["strong_domain_host"] = (domain_score_val >= 5 or (company_has_corp and domain_score_val >= 4)) and host_token_hit
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
                        "error_code": "search_timeout",
                    })
                    manager.save_company_data(company, status="review")
                    log.info("[%s] 候補ゼロ -> review/search_timeout で保存", cid)
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

                    if ai_tasks:
                        await asyncio.gather(*ai_tasks, return_exceptions=True)

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
                    # 社名トークンなし＆住所一致だけの候補は公式扱いしない
                    if not host_token_hit and not name_hit and address_ok:
                        manager.upsert_url_flag(
                            normalized_url,
                            is_official=False,
                            source="rule",
                            reason="address_only_no_name_host",
                        )
                        fallback_cands.append((record.get("url"), extracted))
                        log.info("[%s] ホスト社名なし・名称一致なし・住所一致のみのため非公式扱い: %s", cid, record.get("url"))
                        continue
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
                                or (domain_score >= 5 and host_token_hit)
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
                            if input_addr_pref_only and not (host_token_hit or name_hit or strong_domain_host or domain_score >= 4):
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="pref_only_input_no_name_host",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式だが都道府県だけの入力で社名/ホスト根拠なしのため除外: %s", cid, record.get("url"))
                                continue
                            if addr and not address_ok:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="address_mismatch_input",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式でも入力住所と一致せず除外: %s", cid, record.get("url"))
                                continue
                            # ホストに社名トークンも名称ヒットも無ければ公式にしない
                            if not host_token_hit and not name_hit:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="no_host_token_no_name",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式でも社名トークン/名称なしのためスキップ: %s", cid, record.get("url"))
                                continue
                            # ドメイン/住所/名称一致が弱い公式判定は採用しない
                            if (not host_token_hit) and domain_score < 3 and not address_ok:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="weak_domain_no_address",
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] AI公式でもドメイン/住所一致なしのためスキップ: %s", cid, record.get("url"))
                                continue
                            name_or_domain_ok = (
                                name_hit
                                or strong_domain_host
                                or rule_details.get("strong_domain")
                                or domain_score >= 3
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
                            # 低ドメイン一致は name/address/host が強い場合のみ採用
                            if domain_score < 3 and not strong_domain_host and not rule_details.get("strong_domain") and not address_ok and not name_hit and not host_token_hit:
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
                        if input_addr_pref_only and not (host_token_hit or name_hit or strong_domain_host or domain_score >= 4):
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason="pref_only_input_no_name_host",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] 公式判定だが都道府県だけの入力で社名/ホスト根拠なしのため除外: %s", cid, record.get("url"))
                            continue
                        if addr and not address_ok:
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason="address_mismatch_input",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] 公式判定だが入力住所と一致せず除外: %s", cid, record.get("url"))
                            continue
                        if not host_token_hit and not name_hit:
                            manager.upsert_url_flag(
                                normalized_url,
                                is_official=False,
                                source="rule",
                                reason="no_host_token_no_name",
                            )
                            fallback_cands.append((record.get("url"), extracted))
                            log.info("[%s] 名称/ホストトークンなしのため公式判定を除外: %s", cid, record.get("url"))
                            continue
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
                        # 低ドメイン一致は name/address/host が強い場合のみ採用
                        if domain_score < 3 and not strong_domain_host and not rule_details.get("strong_domain") and not address_ok and not name_hit and not host_token_hit:
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
                provisional_host_token = False
                provisional_name_present = False
                provisional_address_ok = False
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
                        host_token_hit = bool(record.get("host_token_hit"))
                        name_present = bool(rule_details.get("name_present"))
                        strong_domain_host = bool(record.get("strong_domain_host"))
                        addr_hit = bool(rule_details.get("address_match"))
                        pref_hit = bool(rule_details.get("prefecture_match"))
                        zip_hit = bool(rule_details.get("postal_code_match"))
                        address_ok = (not addr) or addr_hit or pref_hit or zip_hit
                        allow_without_host = (
                            strong_domain_host
                            or name_present
                            or domain_score >= 4
                            or (address_ok and domain_score >= 3)
                        )
                        if not host_token_hit and not allow_without_host:
                            continue
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
                        provisional_host_token = bool(best_record.get("host_token_hit"))
                        provisional_name_present = name_present
                        allow_without_host = (
                            provisional_name_present
                            or strong_domain_host
                            or domain_score >= 4
                            or (address_ok and domain_score >= 3)
                        )
                        # 完全に根拠が無い暫定URLのみ破棄
                        if not provisional_host_token and not allow_without_host:
                            log.info("[%s] 社名トークン/名称/強ドメイン/住所一致なしのため暫定URL候補を破棄: %s", cid, normalized_url)
                            best_record = None
                            provisional_homepage = ""
                            provisional_info = None
                            provisional_cands = {}
                            provisional_domain_score = 0
                            provisional_address_ok = False
                            break
                        # 公式昇格の予備候補だが保存はしない。強条件のみ後で昇格。
                        provisional_homepage = normalized_url
                        provisional_info = best_record.get("info")
                        provisional_cands = best_record.get("extracted") or {}
                        provisional_domain_score = domain_score
                        provisional_address_ok = address_ok
                        # ログだけ出して深掘りターゲットとする
                        log.info("[%s] 公式未確定のため暫定URLで深掘り: %s (domain_score=%s name=%s addr=%s host_token=%s)",
                                 cid, provisional_homepage, domain_score, name_present, address_ok, strong_domain_host)

                # 暫定URLは深掘りにのみ使用し、保存は公式昇格条件を満たした場合に限定
                if not homepage and provisional_homepage:
                    weak_provisional = (
                        provisional_domain_score < 3
                        and not provisional_host_token
                        and not provisional_name_present
                        and not provisional_address_ok
                    )
                    if weak_provisional:
                        homepage = ""
                        primary_cands = {}
                        provisional_info = None
                        provisional_cands = {}
                        force_review = True
                        provisional_homepage = ""
                    else:
                        homepage = provisional_homepage
                        info = provisional_info
                        primary_cands = provisional_cands
                        homepage_official_flag = 0
                        homepage_official_source = homepage_official_source or "provisional"
                        homepage_official_score = 0.0
                        chosen_domain_score = provisional_domain_score

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
                        allow_without_host = (
                            name_present
                            or strong_domain_host
                            or domain_score >= 4
                            or (address_ok and domain_score >= 3)
                        )
                        if (host_token_hit or allow_without_host) and (host_token_hit or domain_score >= 2 or name_present or strong_domain or address_ok):
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
                    if cc.get("phone_numbers"):
                        cand = pick_best_phone(cc["phone_numbers"])
                        if cand and not rule_phone:
                            rule_phone = cand
                            src_phone = url
                    if cc.get("addresses"):
                        cand_addr = pick_best_address(addr, cc["addresses"])
                        if cand_addr and not rule_address:
                            rule_address = cand_addr
                            src_addr = url
                    if cc.get("rep_names"):
                        cand_rep = pick_best_rep(cc["rep_names"], url)
                        cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                        if cand_rep:
                            if not rule_rep or len(cand_rep) > len(rule_rep):
                                rule_rep = cand_rep
                                src_rep = url
                    if cc.get("listings"):
                        candidate = pick_best_listing(cc["listings"])
                        if candidate and not listing_val:
                            listing_val = candidate
                            need_listing = False
                    if cc.get("capitals"):
                        candidate = pick_best_amount(cc["capitals"])
                        if candidate and (not capital_val or len(candidate) > len(capital_val)):
                            capital_val = candidate
                            need_capital = False
                    if cc.get("revenues"):
                        candidate = pick_best_amount(cc["revenues"])
                        if candidate and (not revenue_val or len(candidate) > len(revenue_val)):
                            revenue_val = candidate
                            need_revenue = False
                    if cc.get("profits"):
                        candidate = pick_best_amount(cc["profits"])
                        if candidate and (not profit_val or len(candidate) > len(profit_val)):
                            profit_val = candidate
                            need_profit = False
                    if cc.get("fiscal_months"):
                        cleaned_fiscal = clean_fiscal_month(cc["fiscal_months"][0] or "")
                        if cleaned_fiscal and not fiscal_val:
                            fiscal_val = cleaned_fiscal
                            need_fiscal = False
                    if cc.get("founded_years"):
                        for cand in cc["founded_years"]:
                            cleaned_founded = clean_founded_year(cand or "")
                            if cleaned_founded and not founded_val:
                                founded_val = cleaned_founded
                                need_founded = False
                                break

                def refresh_need_flags() -> tuple[int, int]:
                    nonlocal need_phone, need_addr, need_rep
                    nonlocal need_listing, need_capital, need_revenue
                    nonlocal need_profit, need_fiscal, need_founded, need_description
                    need_phone = not bool(phone or rule_phone)
                    # 入力住所があってもサイト住所未取得なら取りに行く（found/ruleのみで判定）
                    need_addr = not bool(found_address or rule_address)
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
                    # まず「会社概要/企業情報/会社情報」系の優先リンクを先に巡回して主要情報を拾う
                    if over_hard_deadline() or over_time_limit():
                        timed_out = True
                        priority_docs = {}
                    else:
                        should_fetch_priority = (
                            missing_contact > 0
                            or need_description
                            or need_founded
                            or need_listing
                            or need_revenue
                            or need_profit
                            or need_capital
                            or need_fiscal
                        )
                        try:
                            early_priority_docs = await asyncio.wait_for(
                                scraper.fetch_priority_documents(
                                    homepage,
                                    info_dict.get("html", ""),
                                    max_links=3 if should_fetch_priority else 0,
                                    concurrency=FETCH_CONCURRENCY,
                                    target_types=["about", "contact", "finance"] if should_fetch_priority else None,
                                ),
                                timeout=PAGE_FETCH_TIMEOUT_SEC,
                            ) if should_fetch_priority else {}
                        except Exception:
                            early_priority_docs = {}
                        for url, pdata in early_priority_docs.items():
                            priority_docs[url] = pdata
                            absorb_doc_data(url, pdata)
                        if site_docs := {}:
                            priority_docs.update(site_docs)
                    if timed_out:
                        missing_contact, missing_extra = refresh_need_flags()
                        fully_filled = False
                        related = {}
                        goto_finalize = True
                    else:
                        goto_finalize = False

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

                    rule_phone = pick_best_phone(phones) if phones else None
                    rule_address = pick_best_address(addr, addrs) if addrs else None
                    rule_rep = pick_best_rep(reps, info_url) if reps else None
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
                        candidate = pick_best_listing(listings)
                        if candidate:
                            listing_val = candidate
                    if capitals and not capital_val:
                        candidate = pick_best_amount(capitals)
                        if candidate:
                            capital_val = candidate
                    if revenues and not revenue_val:
                        candidate = pick_best_amount(revenues)
                        if candidate:
                            revenue_val = candidate
                    if profits and not profit_val:
                        candidate = pick_best_amount(profits)
                        if candidate:
                            profit_val = candidate
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
                        # 深掘りは不足がある場合のみ。揃っていれば追加巡回しない。
                        priority_limit = 0
                        # 不足項目に応じて深掘り対象リンクを絞り込む
                        target_types: list[str] = []
                        if not timed_out and not fully_filled:
                            if missing_contact > 0:
                                priority_limit = 3
                                target_types.append("contact")
                            if need_description or need_founded or need_listing:
                                priority_limit = max(priority_limit, 2)
                                target_types.append("about")
                            if need_revenue or need_profit or need_capital or need_fiscal:
                                priority_limit = max(priority_limit, 2)
                                target_types.append("finance")
                        site_docs = {}
                        # early_priority_docs で取得済みの場合は重複を避ける
                        if priority_limit > 0 and not priority_docs:
                            site_docs = await asyncio.wait_for(
                                scraper.fetch_priority_documents(
                                    homepage,
                                    info_dict.get("html", ""),
                                    max_links=priority_limit,
                                    concurrency=FETCH_CONCURRENCY,
                                    target_types=target_types or None,
                                ),
                                timeout=PAGE_FETCH_TIMEOUT_SEC,
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
                ai_needed = bool(
                    homepage
                    and USE_AI
                    and verifier is not None
                    and (
                        missing_contact > 0
                        or need_description
                        or need_listing
                        or need_capital
                        or need_revenue
                        or need_profit
                        or need_fiscal
                        or need_founded
                    )
                )
                if ai_needed and not timed_out:
                    ai_attempted = True
                    info_dict = await ensure_info_has_screenshot(scraper, info_url, info_dict, need_screenshot=OFFICIAL_AI_USE_SCREENSHOT)
                    info = info_dict
                    desc_source_text = select_relevant_paragraphs(info_dict.get("text", ""))
                    ai_text_payload = build_ai_text_payload(desc_source_text)

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
                ai_desc: str | None = None
                def consume_ai_result(res: dict[str, Any] | None) -> None:
                    nonlocal ai_used, ai_model, ai_phone, ai_addr, ai_rep
                    if not res:
                        return
                    ai_used = 1
                    ai_model = AI_MODEL_NAME
                    phone_candidate = normalize_phone(res.get("phone_number"))
                    if phone_candidate:
                        ai_phone = phone_candidate
                    if AI_ADDRESS_ENABLED:
                        addr_candidate = normalize_address(res.get("address"))
                        if addr_candidate:
                            ai_addr = addr_candidate
                    rep_candidate = res.get("rep_name") or res.get("representative")
                    rep_candidate = scraper.clean_rep_name(rep_candidate) if rep_candidate else None
                    if rep_candidate:
                        ai_rep = rep_candidate
                    desc_candidate = res.get("description")
                    if isinstance(desc_candidate, str) and desc_candidate.strip():
                        update_description_candidate(desc_candidate)
                if ai_task:
                    ai_result = await ai_task
                    consume_ai_result(ai_result)
                    if ai_desc and not description_val:
                        cleaned_ai_desc = clean_description_value(ai_desc)
                        if cleaned_ai_desc:
                            description_val = cleaned_ai_desc
                            need_description = False
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
                    consume_ai_result(ai_result)
                else:
                    if ai_attempted and AI_COOLDOWN_SEC > 0:
                        await asyncio.sleep(jittered_seconds(AI_COOLDOWN_SEC, JITTER_RATIO))
                missing_contact, missing_extra = refresh_need_flags()
                if need_description and verifier is not None and info_dict and not timed_out:
                    desc_source = select_relevant_paragraphs(info_dict.get("text", "") or "")
                    ai_desc = await verifier.generate_description(
                        build_ai_text_payload(desc_source),
                        info_dict.get("screenshot"),
                        name,
                        addr,
                    )
                    if ai_desc:
                        description_val = ai_desc
                        need_description = False
                        ai_used = 1
                        ai_model = AI_MODEL_NAME

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
                        combined_sources = [select_relevant_paragraphs(v.get("text", "") or "") for v in priority_docs.values()]
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
                                ai_addr2 = normalize_address(ai_result2.get("address")) if AI_ADDRESS_ENABLED else None
                                ai_rep2 = ai_result2.get("rep_name") or ai_result2.get("representative")
                                ai_rep2 = scraper.clean_rep_name(ai_rep2) if ai_rep2 else None
                                desc2 = ai_result2.get("description")
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
                    if need_description and verifier is not None:
                        combined_sources = [select_relevant_paragraphs(v.get("text", "") or "") for v in priority_docs.values()]
                        combined_text = build_ai_text_payload(*combined_sources)
                        screenshot_payload = (info_dict or {}).get("screenshot") if info_dict else None
                        ai_desc2 = await verifier.generate_description(combined_text, screenshot_payload, name, addr)
                        if ai_desc2:
                            description_val = ai_desc2
                            need_description = False
                            ai_used = 1
                            ai_model = AI_MODEL_NAME
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

                    if rule_address:
                        found_address = rule_address
                        address_source = "rule"
                        if not src_addr:
                            src_addr = info_url
                    elif ai_addr:
                        found_address = ai_addr
                        address_source = "ai"
                        src_addr = info_url
                    else:
                        found_address = rule_address or ""
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

                    # ???????????????????????????????
                    missing_contact, missing_extra = refresh_need_flags()
                    need_extra_fields = missing_extra > 0
                    if missing_contact == 0 and not need_extra_fields:
                        related = {}
                    else:
                        related_page_limit = RELATED_BASE_PAGES + (1 if missing_extra else 0)
                        if need_phone:
                            related_page_limit += RELATED_EXTRA_PHONE
                        related_page_limit = max(0, related_page_limit)
                        related = {}
                        weak_provisional_target = (
                            homepage_official_flag == 0
                            and homepage_official_source.startswith("provisional")
                            and chosen_domain_score < 3
                            and not provisional_host_token
                            and not provisional_name_present
                            and not provisional_address_ok
                        )
                        if not weak_provisional_target and not timed_out and ((missing_contact > 0) or need_extra_fields):
                            if over_time_limit() or over_deep_limit() or over_hard_deadline():
                                timed_out = True
                            else:
                                max_hops = RELATED_MAX_HOPS_PHONE if need_phone else RELATED_MAX_HOPS_BASE
                                try:
                                    related = await asyncio.wait_for(
                                        scraper.crawl_related(
                                            homepage,
                                            need_phone,
                                            need_addr,
                                            need_rep,
                                            max_pages=related_page_limit,
                                            max_hops=max_hops,
                                            need_listing=need_listing,
                                            need_capital=need_capital,
                                            need_revenue=need_revenue,
                                            need_profit=need_profit,
                                            need_fiscal=need_fiscal,
                                            need_founded=need_founded,
                                            need_description=need_description,
                                            initial_info=info_dict if info_url == homepage else None,
                                        ),
                                        timeout=PAGE_FETCH_TIMEOUT_SEC,
                                    )
                                except Exception:
                                    related = {}
                        for url, data in related.items():
                            text = data.get("text", "") or ""
                            html_content = data.get("html", "") or ""
                            cc = scraper.extract_candidates(text, html_content)
                            if need_phone and cc.get("phone_numbers"):
                                cand = pick_best_phone(cc["phone_numbers"])
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
                                cand_rep = pick_best_rep(cc["rep_names"], url)
                                cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                                if cand_rep:
                                    rep_name_val = cand_rep
                                    src_rep = url
                                    need_rep = False
                            if need_description and cc.get("description"):
                                desc = clean_description_value(cc["description"])
                                if desc:
                                    description_val = desc
                                    need_description = False
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

                    if homepage and (need_addr or not found_address) and not timed_out and not priority_docs:
                        try:
                            extra_docs = await asyncio.wait_for(
                                scraper.fetch_priority_documents(
                                    homepage,
                                    info_dict.get("html", ""),
                                    max_links=3,
                                    concurrency=FETCH_CONCURRENCY,
                                ),
                                timeout=PAGE_FETCH_TIMEOUT_SEC,
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
                            verify_result = await asyncio.wait_for(
                                scraper.verify_on_site(
                                    homepage,
                                    phone or rule_phone or None,
                                    found_address or addr or rule_address or None,
                                    fetch_limit=3,
                                ),
                                timeout=PAGE_FETCH_TIMEOUT_SEC,
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
                            cand = pick_best_phone(data["phone_numbers"])
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
                            cand_rep = pick_best_rep(data["rep_names"], url)
                            cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                            if cand_rep:
                                rep_name_val = cand_rep
                                src_rep = url
                        if phone and found_address and rep_name_val:
                            break

                if not listing_val:
                    for url, data in fallback_cands:
                        values = data.get("listings") or []
                        candidate = pick_best_listing(values)
                        if candidate:
                            listing_val = candidate
                            break
                if not capital_val:
                    for url, data in fallback_cands:
                        values = data.get("capitals") or []
                        candidate = pick_best_amount(values)
                        if candidate:
                            capital_val = candidate
                            break
                if not revenue_val:
                    for url, data in fallback_cands:
                        values = data.get("revenues") or []
                        candidate = pick_best_amount(values)
                        if candidate:
                            revenue_val = candidate
                            break
                if not profit_val:
                    for url, data in fallback_cands:
                        values = data.get("profits") or []
                        candidate = pick_best_amount(values)
                        if candidate:
                            profit_val = candidate
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

                found_address = sanitize_text_block(found_address)
                rule_address = sanitize_text_block(rule_address)
                if found_address and not looks_like_address(found_address):
                    found_address = ""
                if not found_address and rule_address:
                    found_address = rule_address
                if found_address and not looks_like_address(found_address):
                    found_address = ""
                normalized_found_address = normalize_address(found_address) if found_address else ""
                if not normalized_found_address and addr:
                    normalized_found_address = normalize_address(addr) or ""
                rep_name_val = scraper.clean_rep_name(rep_name_val) or ""
                description_val = clean_description_value(sanitize_text_block(description_val))
                listing_val = clean_listing_value(listing_val)
                capital_val = ai_normalize_amount(capital_val) or clean_amount_value(capital_val)
                revenue_val = ai_normalize_amount(revenue_val) or clean_amount_value(revenue_val)
                profit_val = ai_normalize_amount(profit_val) or clean_amount_value(profit_val)
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

                # 暫定URLは強いドメイン/社名ヒットが無ければ保存しない
                if (
                    homepage
                    and homepage_official_flag == 0
                    and homepage_official_source.startswith("provisional")
                ):
                    strong_provisional = (
                        (chosen_domain_score >= 4)
                        or (provisional_host_token and chosen_domain_score >= 3)
                        or (provisional_name_present and chosen_domain_score >= 3)
                        or (provisional_address_ok and chosen_domain_score >= 4)
                    )
                    if not strong_provisional:
                        log.info(
                            "[%s] 暫定URLを保存しません (domain_score=%s host_token=%s name=%s addr=%s): %s",
                            cid,
                            chosen_domain_score,
                            provisional_host_token,
                            provisional_name_present,
                            provisional_address_ok,
                            homepage,
                        )
                        homepage = ""
                        homepage_official_source = ""
                        homepage_official_score = 0.0
                        chosen_domain_score = 0

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
