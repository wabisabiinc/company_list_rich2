# main.py
import asyncio
import os
import csv
import logging
import json
import re
import random
import time
import datetime as dt
import html as html_mod
import threading
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict
from urllib.parse import urlparse, unquote
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    def load_dotenv() -> None:  # type: ignore
        return None
from bs4 import BeautifulSoup

from src.database_manager import DatabaseManager
from src.company_scraper import CompanyScraper, CITY_RE, NAME_CHUNK_RE, KANA_NAME_RE
from src.ai_verifier import (
    AIVerifier,
    DEFAULT_MODEL as AI_MODEL_NAME,
    AI_CALL_TIMEOUT_SEC,
    _normalize_amount as ai_normalize_amount,
)
from src.homepage_policy import apply_provisional_homepage_policy
from src.industry_classifier import IndustryClassifier
from scripts.extract_contact_urls import _pick_contact_url, _build_ai_signals, AI_MIN_CONFIDENCE_DEFAULT
from src.reference_checker import ReferenceChecker
from src.jp_number import normalize_kanji_numbers

class HardTimeout(Exception):
    """Raised when the per-company hard time limit is exceeded."""
    pass


class SkipCompany(Exception):
    """Raised to skip remaining processing for the current company."""
    pass

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
if not DOTENV_AVAILABLE and os.path.exists(".env"):
    log.warning("python-dotenv が未導入のため .env を読み込めません（venv有効化 or `pip install -r requirements.txt` を実行してください）")
load_dotenv()

# --------------------------------------------------
# 実行オプション（.env）
# --------------------------------------------------
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
USE_AI = os.getenv("USE_AI", "true").lower() == "true"
# AI公式判定。デフォルトは候補URLを広く判定し、公式採用の最終判断をAI優先にする。
USE_AI_OFFICIAL = os.getenv("USE_AI_OFFICIAL", "true").lower() == "true"
# AI公式判定の対象範囲（true: 候補を広く / false: 従来どおり上位3件のみ）
AI_OFFICIAL_ALL_CANDIDATES = os.getenv("AI_OFFICIAL_ALL_CANDIDATES", "true").lower() == "true"
# AI公式判定が使える場合、公式採用の最終判断をAI優先にする
AI_OFFICIAL_PRIMARY = os.getenv("AI_OFFICIAL_PRIMARY", "true").lower() == "true"
# AI公式判定の候補数上限（0以下で無制限）。デフォルトは3件。
AI_OFFICIAL_CANDIDATE_LIMIT = int(os.getenv("AI_OFFICIAL_CANDIDATE_LIMIT", "3"))
# AI公式判定の同時実行数（API/モデル負荷対策）。デフォルトは3。
AI_OFFICIAL_CONCURRENCY = max(1, int(os.getenv("AI_OFFICIAL_CONCURRENCY", "3")))
# description はAIで常時生成（verify_infoで同時生成）。追加の説明専用AI呼び出しを有効にしたい場合のみ true。
USE_AI_DESCRIPTION = os.getenv("USE_AI_DESCRIPTION", "false").lower() == "true"
# description を「毎回AIで作る」方針（既定: true）。false の場合は既存値を保持し、必要時のみ作る。
AI_DESCRIPTION_ALWAYS = os.getenv("AI_DESCRIPTION_ALWAYS", "true").lower() == "true"
# AI_DESCRIPTION_ALWAYS=true でも、AI由来descriptionが取れなかったときに追加のAI呼び出しで補うか（既定: true）
# 呼び出し回数/時間は増えるが、description 欠損を確実に潰すため既定は true
AI_DESCRIPTION_FALLBACK_CALL = os.getenv("AI_DESCRIPTION_FALLBACK_CALL", "true").lower() == "true"
# AI_DESCRIPTION_ALWAYS=true のとき、毎回 description 生成AIを呼ぶか（既定: true）
# 速度/コストよりも「常にAIで読みやすい事業説明」を優先したい場合に有効。
AI_DESCRIPTION_ALWAYS_CALL = os.getenv("AI_DESCRIPTION_ALWAYS_CALL", "true").lower() == "true"
# requeue/retry時に既存descriptionを破棄して再生成したい場合のみ true（AI_DESCRIPTION_ALWAYS が優先）
REGENERATE_DESCRIPTION = os.getenv("REGENERATE_DESCRIPTION", "false").lower() == "true"
AI_FINAL_WITH_OFFICIAL = os.getenv("AI_FINAL_WITH_OFFICIAL", "false").lower() == "true"
# USE_AI_OFFICIAL=true の場合でも、最終AI(select_company_fields)を毎回許可する（既定: false / 追加コスト）
AI_FINAL_ALWAYS = os.getenv("AI_FINAL_ALWAYS", "false").lower() == "true"
# 代表者名の誤格納（「コンテンツ」「キーワード」等）を構造的に防ぐため、
# 代表者は TABLE/LABEL/ROLE/JSONLD 等の「構造化ソース」由来のみ採用する（既定: true）
REP_REQUIRE_STRUCTURED_SOURCE = os.getenv("REP_REQUIRE_STRUCTURED_SOURCE", "true").lower() == "true"
WORKER_ID = os.getenv("WORKER_ID", "w1")  # 並列識別子
COMPANIES_DB_PATH = os.getenv("COMPANIES_DB_PATH", "data/companies.db")

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
# 優先docs（会社概要等）の取得上限（時間爆発防止）
PRIORITY_DOCS_MAX_LINKS_CAP = max(1, int(os.getenv("PRIORITY_DOCS_MAX_LINKS_CAP", "5")))
PROFILE_DISCOVERY_MAX_LINKS_CAP = max(1, int(os.getenv("PROFILE_DISCOVERY_MAX_LINKS_CAP", "4")))
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
# 会社ごとの絶対上限（この時間を超えたら部分保存して次へ）
ABSOLUTE_COMPANY_DEADLINE_SEC = float(os.getenv("ABSOLUTE_COMPANY_DEADLINE_SEC", "60"))
# 全体のハード上限（デフォルト60秒で次ジョブへスキップ）
GLOBAL_HARD_TIMEOUT_SEC = float(os.getenv("GLOBAL_HARD_TIMEOUT_SEC", "60"))
# 単社処理のハード上限（candidate取得で固まるのを避けるための保険、0で無効）
COMPANY_HARD_TIMEOUT_SEC = float(os.getenv("COMPANY_HARD_TIMEOUT_SEC", "60"))
DEEP_PHASE_TIMEOUT_SEC = float(os.getenv("DEEP_PHASE_TIMEOUT_SEC", "120"))
OFFICIAL_AI_USE_SCREENSHOT = os.getenv("OFFICIAL_AI_USE_SCREENSHOT", "true").lower() == "true"
# AIに渡すスクショの方針（always/never/auto）。auto はテキスト量が十分ならスクショ無しを優先。
AI_SCREENSHOT_POLICY = (os.getenv("AI_SCREENSHOT_POLICY", "auto") or "auto").strip().lower()
# 公式判定は誤判定回避を優先し、既定でスクショを付ける（要求: トップ候補3件）。
OFFICIAL_AI_SCREENSHOT_POLICY = (os.getenv("OFFICIAL_AI_SCREENSHOT_POLICY", "always") or "always").strip().lower()
VERIFY_AI_SCREENSHOT_POLICY = (os.getenv("VERIFY_AI_SCREENSHOT_POLICY", AI_SCREENSHOT_POLICY) or AI_SCREENSHOT_POLICY).strip().lower()
# auto判定用の最低量（これ以上ならスクショ無しでも十分とみなす）
AI_SCREENSHOT_TEXT_MIN = max(0, int(os.getenv("AI_SCREENSHOT_TEXT_MIN", "900")))
AI_SCREENSHOT_HTML_MIN = max(0, int(os.getenv("AI_SCREENSHOT_HTML_MIN", "4000")))
# 住所はAIの出力も取り込みつつ、DB側で都道府県不一致の上書きを厳格に制限する
AI_ADDRESS_ENABLED = os.getenv("AI_ADDRESS_ENABLED", "true").lower() == "true"
SECOND_PASS_ENABLED = os.getenv("SECOND_PASS_ENABLED", "false").lower() == "true"
SECOND_PASS_RETRY_STATUSES = [s.strip() for s in os.getenv("SECOND_PASS_RETRY_STATUSES", "review,no_homepage,error").split(",") if s.strip()]
LONG_PAGE_TIMEOUT_MS = int(os.getenv("LONG_PAGE_TIMEOUT_MS", os.getenv("PAGE_TIMEOUT_MS", "9000")))
LONG_SLOW_PAGE_THRESHOLD_MS = int(os.getenv("LONG_SLOW_PAGE_THRESHOLD_MS", os.getenv("SLOW_PAGE_THRESHOLD_MS", "9000")))
LONG_TIME_LIMIT_FETCH_ONLY = float(os.getenv("LONG_TIME_LIMIT_FETCH_ONLY", os.getenv("TIME_LIMIT_FETCH_ONLY", "30")))
LONG_TIME_LIMIT_WITH_OFFICIAL = float(os.getenv("LONG_TIME_LIMIT_WITH_OFFICIAL", os.getenv("TIME_LIMIT_WITH_OFFICIAL", "45")))
LONG_TIME_LIMIT_DEEP = float(os.getenv("LONG_TIME_LIMIT_DEEP", os.getenv("TIME_LIMIT_DEEP", "60")))
AI_MIN_REMAINING_SEC = float(os.getenv("AI_MIN_REMAINING_SEC", "2.5"))
AI_VERIFY_MIN_CONFIDENCE = float(os.getenv("AI_VERIFY_MIN_CONFIDENCE", "0.60"))
# url_flags に保存された「AI非公式」判定を、候補URL取得前に強制スキップするための閾値。
# 低confidenceのAI判定は誤爆しやすいので、一定以上のみハードに扱う（それ未満は再評価対象）。
AI_SKIP_NEGATIVE_FLAG_MIN_CONFIDENCE = float(os.getenv("AI_SKIP_NEGATIVE_FLAG_MIN_CONFIDENCE", "0.85"))
AI_CLEAR_NEGATIVE_FLAGS = os.getenv("AI_CLEAR_NEGATIVE_FLAGS", "false").lower() == "true"
INDUSTRY_CLASSIFY_ENABLED = os.getenv("INDUSTRY_CLASSIFY_ENABLED", "true").lower() == "true"
INDUSTRY_CLASSIFY_AFTER_HOMEPAGE = os.getenv("INDUSTRY_CLASSIFY_AFTER_HOMEPAGE", "true").lower() == "true"
INDUSTRY_RULE_MIN_SCORE = max(1, int(os.getenv("INDUSTRY_RULE_MIN_SCORE", "2")))
INDUSTRY_AI_ENABLED = os.getenv("INDUSTRY_AI_ENABLED", "true").lower() == "true"
INDUSTRY_AI_TOP_N = max(5, int(os.getenv("INDUSTRY_AI_TOP_N", "12")))
INDUSTRY_AI_MIN_CONFIDENCE = float(os.getenv("INDUSTRY_AI_MIN_CONFIDENCE", "0.50"))
INDUSTRY_MINOR_DIRECT_MAX_CANDIDATES = max(20, int(os.getenv("INDUSTRY_MINOR_DIRECT_MAX_CANDIDATES", "60")))
INDUSTRY_DETAIL_MIN_CONFIDENCE = float(os.getenv("INDUSTRY_DETAIL_MIN_CONFIDENCE", "0.72"))
INDUSTRY_DETAIL_MIN_EVIDENCE_CHARS = max(0, int(os.getenv("INDUSTRY_DETAIL_MIN_EVIDENCE_CHARS", "200")))
INDUSTRY_FORCE_CLASSIFY = os.getenv("INDUSTRY_FORCE_CLASSIFY", "false").lower() == "true"
INDUSTRY_FORCE_DEFAULT_MINOR_CODES = [
    s.strip()
    for s in os.getenv("INDUSTRY_FORCE_DEFAULT_MINOR_CODES", "9599,9299,9999").split(",")
    if s.strip()
]
INDUSTRY_REQUIRE_AI = os.getenv("INDUSTRY_REQUIRE_AI", "true").lower() == "true"
INDUSTRY_RULE_FALLBACK_ENABLED = os.getenv("INDUSTRY_RULE_FALLBACK_ENABLED", "false").lower() == "true"
INDUSTRY_RULE_FALLBACK_MIN_SCORE = max(1, int(os.getenv("INDUSTRY_RULE_FALLBACK_MIN_SCORE", "5")))
INDUSTRY_RULE_FALLBACK_MARGIN = max(1, int(os.getenv("INDUSTRY_RULE_FALLBACK_MARGIN", "2")))
INDUSTRY_NAME_LOOKUP_ENABLED = os.getenv("INDUSTRY_NAME_LOOKUP_ENABLED", "true").lower() == "true"
CONTACT_URL_IN_MAIN = os.getenv("CONTACT_URL_IN_MAIN", "true").lower() == "true"
CONTACT_URL_FORCE = os.getenv("CONTACT_URL_FORCE", "false").lower() == "true"
CONTACT_URL_AI_ENABLED = os.getenv("CONTACT_URL_AI_ENABLED", "true").lower() == "true"
CONTACT_URL_AI_MIN_CONFIDENCE = float(os.getenv("CONTACT_URL_AI_MIN_CONFIDENCE", str(AI_MIN_CONFIDENCE_DEFAULT)))

# 暫定URL（provisional_*）を homepage として保存するかどうか。
# - false の場合でも provisional_homepage/final_homepage には記録する。
SAVE_PROVISIONAL_HOMEPAGE = os.getenv("SAVE_PROVISIONAL_HOMEPAGE", "false").lower() == "true"
# 暫定URL（provisional/ai_provisional）を落とすポリシー判定を適用するか（既定: true）
APPLY_PROVISIONAL_HOMEPAGE_POLICY = os.getenv("APPLY_PROVISIONAL_HOMEPAGE_POLICY", "true").lower() == "true"
# official 判定できない場合、homepage を空欄にする（誤保存を防ぐ。既定: true）
REQUIRE_OFFICIAL_HOMEPAGE = os.getenv("REQUIRE_OFFICIAL_HOMEPAGE", "true").lower() == "true"
# 公式サイトの更新チェック（差分が無ければ本クロールをスキップ）
UPDATE_CHECK_ENABLED = os.getenv("UPDATE_CHECK_ENABLED", "true").lower() == "true"
UPDATE_CHECK_TIMEOUT_MS = int(os.getenv("UPDATE_CHECK_TIMEOUT_MS", "2500"))
UPDATE_CHECK_ALLOW_SLOW = os.getenv("UPDATE_CHECK_ALLOW_SLOW", "false").lower() == "true"
UPDATE_CHECK_FINAL_ONLY = os.getenv("UPDATE_CHECK_FINAL_ONLY", "true").lower() == "true"
# directory_like を強く疑う場合のハード拒否閾値（既定: 9）
DIRECTORY_HARD_REJECT_SCORE = int(os.getenv("DIRECTORY_HARD_REJECT_SCORE", "9"))
DEFAULT_TIME_LIMIT_FETCH_ONLY = TIME_LIMIT_FETCH_ONLY
DEFAULT_TIME_LIMIT_WITH_OFFICIAL = TIME_LIMIT_WITH_OFFICIAL
DEFAULT_TIME_LIMIT_DEEP = TIME_LIMIT_DEEP

MIRROR_TO_CSV = os.getenv("MIRROR_TO_CSV", "false").lower() == "true"
OUTPUT_CSV_PATH = os.getenv("OUTPUT_CSV_PATH", "data/output.csv")
CSV_FIELDNAMES = [
    "id", "company_name", "address", "employee_count",
    "homepage", "phone", "found_address", "rep_name", "description",
    "listing", "revenue", "profit", "capital", "fiscal_month", "founded_year"
]

# CSV（Excel）インジェクション対策:
# セル先頭が = + - @ の場合、Excel が式として解釈し得るため先頭に ' を付けて無害化する。
_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")

def _csv_safe_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return value
    s = str(value)
    stripped = s.lstrip(" \t\r\n")
    if stripped and stripped[0] in _CSV_FORMULA_PREFIXES:
        return "'" + s
    return s

def _csv_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _csv_safe_cell(v) for k, v in (row or {}).items()}
PHASE_METRICS_PATH = os.getenv("PHASE_METRICS_PATH", "logs/phase_metrics.csv")
NO_OFFICIAL_LOG_PATH = os.getenv("NO_OFFICIAL_LOG_PATH", "logs/no_official.jsonl")
EXTRACT_DEBUG_JSONL_PATH = os.getenv("EXTRACT_DEBUG_JSONL_PATH", "")

REFERENCE_CHECKER: ReferenceChecker | None = None
if REFERENCE_CSVS:
    try:
        REFERENCE_CHECKER = ReferenceChecker.from_csvs(REFERENCE_CSVS)
        log.info("Reference data loaded: %s rows", len(REFERENCE_CHECKER))
    except Exception:
        log.exception("Reference data loading failed")

INDUSTRY_CLASSIFIER = IndustryClassifier()

ZIP_CODE_RE = re.compile(r"(\d{3}-\d{4})")
JAPANESE_RE = re.compile(r"[ぁ-んァ-ン一-龥]")
MOJIBAKE_LATIN_RE = re.compile(r"[ÃÂãâæçïðñöøûüÿ]")
ADDRESS_JS_NOISE_RE = re.compile(
    r"(window\.\w+|dataLayer\s*=|gtm\.|googletagmanager|nr-data\.net|newrelic|bam\.nr-data\.net|function\s*\(|<script|</script>)",
    re.IGNORECASE,
)
ADDRESS_FORM_NOISE_RE = re.compile(
    # フォーム由来のラベル/説明文だけを狙う（単語の素朴な出現は弾かない）
    r"("
    r"住所検索|郵便番号\s*[（(]?\s*半角|マンション・?ビル名|市区町村・番地|"
    r"都道府県\b|都道府県\s*(?:選択|入力|[:：])|市区町村\s*(?:選択|入力|[:：])|"
    r"住所\s*(?:を)?\s*(?:入力|選択)\b|番地\s*(?:を)?\s*入力|建物(?:名)?\s*(?:を)?\s*入力|"
    r"(?:必須|入力してください|例[:：]|記入例)"
    r")",
    re.IGNORECASE,
)
EMPLOYEE_VALUE_RE = re.compile(r"(約|およそ)?\s*([0-9０-９]{1,6})\s*(?:名|人)\b")
EMPLOYEE_RANGE_RE = re.compile(r"([0-9０-９]{1,6})\s*(?:-|〜|～|~)\s*([0-9０-９]{1,6})\s*(?:名|人)?")
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
# DBへ保存する「最終description」の統一ルール（AI/ルール問わず）
FINAL_DESCRIPTION_MIN_LEN = max(20, int(os.getenv("FINAL_DESCRIPTION_MIN_LEN", "80")))
FINAL_DESCRIPTION_MAX_LEN = max(FINAL_DESCRIPTION_MIN_LEN, int(os.getenv("FINAL_DESCRIPTION_MAX_LEN", "160")))
# 「どんな事業・業種か」を毎回推定して格納する（AIの追加呼び出し無し）
INFER_INDUSTRY_ALWAYS = os.getenv("INFER_INDUSTRY_ALWAYS", "true").lower() == "true"
DESCRIPTION_BIZ_KEYWORDS = (
    "事業", "製造", "開発", "販売", "提供", "サービス", "運営", "支援", "施工", "設計", "製作",
    "卸", "卸売", "小売", "商社", "代理", "代理店", "仲介",
    "広告", "マーケティング", "企画", "制作", "運用", "運用支援", "保守", "メンテナンス", "maintenance",
    "清掃", "警備",
    "物流", "建設", "工事", "コンサル", "consulting", "solution", "ソリューション",
    "product", "製品", "プロダクト", "システム", "プラント", "加工", "レンタル", "運送",
    "IT", "デジタル", "ITソリューション", "プロジェクト", "アウトソーシング", "研究", "技術",
    "人材", "教育", "ヘルスケア", "医療", "介護", "保育", "福祉",
    "宿泊", "ホテル", "旅行", "観光", "飲食", "レストラン", "カフェ",
    "食品", "エネルギー", "不動産", "金融", "EC", "通販",
    "プラットフォーム", "クラウド", "SaaS", "DX", "AI", "データ分析", "セキュリティ", "インフラ",
    "基盤", "ソフトウェア", "ハードウェア", "ロボット", "IoT", "モビリティ", "物流DX",
)

def looks_mojibake(text: str | None) -> bool:
    if not text:
        return False
    if "\ufffd" in text:
        return True
    s = str(text)
    if JAPANESE_RE.search(s):
        # Japanese is present; only reject if obvious mojibake markers are also present.
        return bool(MOJIBAKE_LATIN_RE.search(s)) and s.count("\ufffd") >= 1
    latin_count = sum(1 for ch in s if "\u00c0" <= ch <= "\u00ff")
    if latin_count >= 3 and latin_count / max(len(s), 1) >= 0.15:
        return True
    return bool(MOJIBAKE_LATIN_RE.search(s) and latin_count >= 2)

# --------------------------------------------------
# 正規化 & 一致判定
# --------------------------------------------------
def normalize_phone(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"(内線|ext|extension)\s*[:：]?\s*\d+$", "", s, flags=re.I)
    # ハイフン類を統一
    s = re.sub(r"[‐―－ー–—]+", "-", s)
    # 文字列上の区切りがある場合は、それを優先して分割（03-1234-5678 等の誤分割を防ぐ）
    m_sep = re.search(r"(0\d{1,4})\D+?(\d{1,4})\D+?(\d{3,4})", s)
    if m_sep:
        return f"{m_sep.group(1)}-{m_sep.group(2)}-{m_sep.group(3)}"
    digits = re.sub(r"\D", "", s)
    if digits.startswith("81") and len(digits) >= 10:
        digits = "0" + digits[2:]
    # 国内番号は0始まりで10〜11桁のみ許容
    if not digits.startswith("0") or len(digits) not in (10, 11):
        return None
    # digits-only は可変長の市外局番のせいで誤分割しやすいので、代表的な形を優先する
    if len(digits) == 10:
        if digits.startswith(("03", "06")):
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
        if digits.startswith(("0120", "0570")):
            return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    # 11桁（携帯/050等）は 3-4-4 を基本にする
    return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"

def clean_homepage_url(url: str | None) -> str:
    if not url:
        return ""
    raw = re.sub(r"\s+", "", str(url))
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered.startswith(("mailto:", "tel:", "javascript:")):
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme:
        candidate = f"https://{raw}"
        parsed = urlparse(candidate)
        if parsed.scheme and parsed.netloc:
            raw = candidate
        else:
            return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""
    return raw.split("#", 1)[0]

def normalize_address(s: str | None) -> str | None:
    if not s:
        return None
    if looks_mojibake(s):
        return None
    # Wix等の埋め込みJSON断片（postalCode等）を住所として誤採用しない
    s_str = str(s)
    if re.search(r"\"[A-Za-z0-9_]{2,}\"\s*:", s_str) and (s_str.count('":') >= 2 or "{" in s_str or "}" in s_str):
        return None
    def _strip_address_label(text: str) -> str:
        out = text.strip()
        for _ in range(3):
            out2 = re.sub(r"^(?:【\s*)?(?:本社|本店)?(?:所在地|住所)(?:】\s*)?\s*[:：]?\s*", "", out)
            if out2 == out:
                break
            out = out2.strip()
        return out

    def _cut_trailing_non_address(text: str) -> str:
        out = text
        tail_re = re.compile(
            r"\s*(?:"
            r"従業員(?:数)?|社員(?:数)?|職員(?:数)?|スタッフ(?:数)?|人数|"
            r"営業時間|受付時間|定休日|"
            r"代表者|代表取締役|取締役|社長|会長|理事長|代表|rep(?:\s|$)|rep\s*[:=]?|"
            r"資本金|設立|創業|沿革|"
            r"(?:一般|特定)?(?:貨物|運送|建設|産廃|産業廃棄物|古物)?(?:業)?(?:許可|免許|登録|届出)|"
            r"ホーム|home|トップ|top|最新情報|お知らせ|ニュース|news|ブログ|blog|"
            r"会社概要|会社情報|企業情報|会社案内|"
            r"事業内容|サービス|"
            r"お問い合わせ|お問合せ|問い合わせ|採用|求人|Google|"
            r"次のリンク|別ウィンドウ|クリック|タップ|"
            r"最寄り駅|(?:JR)?[一-龥ァ-ンA-Za-z0-9]{0,12}駅(?:より|から)|駅(?:より|から)|徒歩\s*\d{1,3}\s*分|"
            r"交差点|右折|左折|直進"
            r")",
            re.IGNORECASE,
        )
        m_tail = tail_re.search(out)
        if m_tail:
            out = out[: m_tail.start()].strip()
        out = out.strip(" 　\t,，;；。．|｜/／・-‐―－ー:：")
        return out

    def _remove_noise_parentheticals(text: str) -> str:
        """
        住所に混入しやすい「（地図）」「(TEL...)」等の括弧要素だけを落とす。
        住所の一部になり得る階数等（例: （2F））は残しやすいよう、ノイズ語がある括弧のみ除去する。
        """
        if not text:
            return text
        noise_kw = (
            "tel",
            "fax",
            "電話",
            "メール",
            "e-mail",
            "mail",
            "お問い合わせ",
            "お問合せ",
            "問い合わせ",
            "地図",
            "マップ",
            "map",
            "google",
            "アクセス",
            "行き方",
            "ルート",
            "経路",
            "営業時間",
            "受付時間",
            "定休日",
            "市区町村コード",
            "自治体コード",
        )

        def _repl(m: re.Match) -> str:
            inner = (m.group(2) or "").strip()
            low = inner.lower()
            if any(k in low for k in noise_kw):
                return " "
            return m.group(0)

        # 丸括弧/全角括弧のみ対象（角括弧は JSON 断片等に使われることがあるため別処理）
        text = re.sub(r"([（(])([^）)]{0,80})([)）])", _repl, text)
        return re.sub(r"\s+", " ", text).strip()
    s = s.strip().replace("　", " ")
    s = html_mod.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    # タグが壊れている/途中で切れている場合の残骸を軽く除去（div/nav 等）
    s = s.replace("<", " ").replace(">", " ")
    s = re.sub(r"\b(?:div|nav|footer|header|main|section|article|span|ul|li|br|href|class|id|style)\b", " ", s, flags=re.I)
    s = re.sub(r"=\s*(?:\"[^\"]*\"|'[^']*'|\\\"[^\\\"]*\\\")", " ", s)
    s = re.sub(r"\s*=\s*", " ", s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    # CSSスタイル断片の除去（スクレイプ時に混入する background: などを落とす）
    s = re.sub(r"(background|color|font-family|font-size|display|position)\s*:\s*[^;]+;?", " ", s, flags=re.I)
    # JSやトラッキング断片をカット（window.dataLayer 等が混入するケース対策）
    m_noise = ADDRESS_JS_NOISE_RE.search(s)
    if m_noise:
        s = s[: m_noise.start()]
    s = _remove_noise_parentheticals(s)
    # URL 等の混入を早めに除去（住所の末尾に付くケース）
    s = re.sub(r"https?://\S+", " ", s, flags=re.I)
    s = re.sub(r"\bmailto:\S+", " ", s, flags=re.I)
    s = re.sub(r"\btel:\S+", " ", s, flags=re.I)
    # 住所ラベルが含まれる場合は、そこから先を優先する（先頭にTEL等があっても住所を拾う）
    m_label = None
    for m in re.finditer(r"(?:本社|本店)?(?:所在地|住所)\s*[:：]\s*", s):
        m_label = m
    if m_label:
        s = s[m_label.end():]
    # 連絡先や地図系キーワードが混入した場合はそれ以降をカット
    contact_pattern = re.compile(r"(TEL|電話|☎|℡|FAX|ファックス|メール|E[-\s]?mail|Mail|連絡先)", re.IGNORECASE)
    contact_match = contact_pattern.search(s)
    if contact_match:
        s = s[: contact_match.start()]
    map_pattern = re.compile(
        r"(地図アプリ|地図で見る|地図|マップ|Google\s*マップ|Google\s*Map|アクセス|アクセスマップ|"
        r"ルート|経路|Route|Directions|行き方|"
        r"次のリンク|別ウィンドウ|クリック|タップ|"
        r"最寄り駅|(?:JR)?[一-龥ァ-ンA-Za-z0-9]{0,12}駅(?:より|から)|駅(?:より|から)|徒歩\s*\d{1,3}\s*分|"
        r"交差点|右折|左折|直進)",
        re.IGNORECASE,
    )
    map_match = map_pattern.search(s)
    if map_match:
        s = s[: map_match.start()]
    arrow_idx = min([idx for idx in (s.find("→"), s.find("⇒")) if idx >= 0], default=-1)
    if arrow_idx >= 0:
        s = s[:arrow_idx]
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
    # （市区町村コード:12208）等の「住所ではない補助情報」を除去（フォームノイズ判定に引っかかるのを防ぐ）
    # ※ 正規化で「コード」の長音が '-' になるケースがあるので両対応
    s = re.sub(r"[（(]\s*(?:市区町村|自治体)コ[-ー]ド\s*[:：]\s*\d+\s*[)）]", "", s)
    s = re.sub(r"(?:市区町村|自治体)コ[-ー]ド\s*[:：]\s*\d+", "", s)
    s = _strip_address_label(s)
    s = _cut_trailing_non_address(s)
    # Cut at first illegal symbol for address
    illegal_re = re.compile(r"[<>\uFF1C\uFF1E\{\}\uFF5B\uFF5D\[\]\uFF3B\uFF3D\(\)\uFF08\uFF09\u300C\u300D\u300E\u300F\u3010\u3011\u3014\u3015\u3008\u3009\u300A\u300B\"'=\uFF1D+\uFF0B*\uFF0A\^\uFF3E$\uFF04#\uFF03@\uFF20&\uFF06|\uFF5C\\\\\uFF3C~\uFF5E`!\uFF01?\uFF1F;\uFF1B:\uFF1A/\uFF0F\u203B\u2605\u2606\u25CF\u25A0\u25C6\u2022\u2026]")
    m_illegal = illegal_re.search(s)
    if m_illegal:
        s = s[: m_illegal.start()]
    # 住所入力フォームのラベル/候補一覧が混入したケースを除外
    if ADDRESS_FORM_NOISE_RE.search(s):
        return None
    if ("郵便番号" in s) and not ZIP_CODE_RE.search(s):
        return None
    try:
        pref_hits = sum(1 for pref in CompanyScraper.PREFECTURE_NAMES if pref in s)
    except Exception:
        pref_hits = 0
    if pref_hits >= 3:
        return None
    m = re.search(r"(\d{3}-\d{4})\s*(.*)", s)
    if m:
        body = _cut_trailing_non_address(m.group(2).strip())
        # 郵便番号だけの場合は住所とみなさない
        if not body:
            return None
        return f"〒{m.group(1)} {body}"
    return s if s else None


def is_prefecture_only_address(text: str | None) -> bool:
    if not text:
        return False
    s = unicodedata.normalize("NFKC", str(text))
    s = re.sub(r"\s+", "", s)
    if not s:
        return False
    s = re.sub(r"^〒?\d{3}[-\s]?\d{4}", "", s)
    s = s.strip(" 　\t,，;；。．|｜/／・-‐―－ー:：")
    if not s:
        return False
    try:
        return s in CompanyScraper.PREFECTURE_NAMES
    except Exception:
        return bool(re.fullmatch(r".+(都|道|府|県)", s)) and len(s) <= 4


def is_address_verifiable(text: str | None) -> bool:
    """
    verify_on_site の検証対象にできる程度に、住所が具体的かどうか。
    - 都道府県だけ等の低品質住所は False（マッチが容易すぎて誤判定を招く）
    """
    normalized = normalize_address(text)
    if not normalized:
        return False
    if is_prefecture_only_address(normalized):
        return False
    if ZIP_CODE_RE.search(normalized):
        return True
    if CITY_RE.search(normalized):
        return True
    if re.search(r"\d", normalized) and re.search(r"(丁目|番地|番|号)", normalized):
        return True
    return False


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
    # スタイル/コメント断片は早めに除去
    t = re.sub(r"(?is)<style.*?>.*?</style>", " ", t)
    t = re.sub(r"(?is)<!--.*?-->", " ", t)
    t = re.sub(r"\bbr\s*/?\b", " ", t, flags=re.I)
    t = re.sub(r'\b(?:class|id|style|data-[\w-]+)\s*=\s*"[^"]*"', " ", t, flags=re.I)
    t = re.sub(r'\b(?:width|height|alt|href|src|title|rel)\s*=\s*"[^"]*"', " ", t, flags=re.I)
    t = re.sub(r"\b(?:width|height|alt|href|src|title|rel)\s*=\s*'[^']*'", " ", t, flags=re.I)
    t = t.replace(">", " ").replace("<", " ")
    t = t.replace("|", " ").replace("｜", " ")
    t = re.sub(r"[\r\n\t]+", " ", t)
    t = re.sub(r"[\x00-\x1f\x7f]", " ", t)
    t = re.sub(r"\s+", " ", t)
    t = t.strip()
    # 典型的なUTF-8モジバケを検知したら破棄
    if looks_mojibake(t):
        return ""
    # 「地図/マップ/アクセス」やスクリプト断片が出たらそこまででカット
    map_noise_re = re.compile(
        r"(地図アプリ|地図で見る|マップ|Google\s*マップ|map|アクセス|ルート|拡大地図|gac?\.push|gtag|_gaq|googletagmanager|<script|function\s*\()",
        re.I,
    )
    m_map = map_noise_re.search(t)
    if m_map:
        t = t[: m_map.start()].strip()
    if not t:
        return ""
    return t


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
    if ADDRESS_FORM_NOISE_RE.search(s):
        return False
    if ("郵便番号" in s) and not ZIP_CODE_RE.search(s):
        return False
    try:
        pref_hits = sum(1 for pref in CompanyScraper.PREFECTURE_NAMES if pref in s)
    except Exception:
        pref_hits = 0
    if pref_hits >= 3:
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

    if (has_pref or has_city) and re.search(r"(丁目|番地|号)", s) and re.search(r"\d", s):
        return True
    if (has_pref or has_city) and re.search(r"(ビル|マンション)", s) and re.search(r"\d", s):
        return True
    # 「京都市下京区和気町21-1」のようなハイフン番地（丁目/番地/号が無い表記）も住所として扱う
    if (has_pref or has_city) and re.search(r"\d{1,4}\s*[‐―－ー-]\s*\d{1,4}", s):
        return True
    return False

def addr_compatible(input_addr: str, found_addr: str) -> bool:
    input_addr = normalize_address(input_addr)
    found_addr = normalize_address(found_addr)
    if not input_addr or not found_addr:
        return True
    return input_addr[:8] in found_addr or found_addr[:8] in input_addr


FREE_HOST_SUFFIXES = (
    ".wixsite.com", ".ameblo.jp", ".fc2.com", ".jimdo.com", ".blogspot.com",
    ".note.jp", ".hatena.ne.jp", ".weebly.com", ".wordpress.com", ".tumblr.com",
    "jp-hp.com",
)


def _is_free_host(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    return any(host.endswith(suf) for suf in FREE_HOST_SUFFIXES)

def should_skip_by_url_flag(flag_info: dict | None) -> bool:
    """
    url_flags の「非公式」判定を候補URLの事前スキップに使うかどうか。
    - ルール由来（directory_like 等）や高confidenceのAIはハードに扱う
    - 低confidenceのAI非公式は誤爆しやすいので再評価対象としてスキップしない
    """
    if not flag_info:
        return False
    if flag_info.get("is_official") is not False:
        return False
    source = (flag_info.get("judge_source") or "").strip().lower()
    if source.startswith("ai_provisional"):
        return False
    if source.startswith("ai_conflict"):
        return False
    conf = flag_info.get("confidence")
    try:
        conf_f = float(conf) if conf is not None else None
    except Exception:
        conf_f = None
    if source == "ai" and (conf_f is None or conf_f < AI_SKIP_NEGATIVE_FLAG_MIN_CONFIDENCE):
        return False
    return True


def _official_signal_ok(
    *,
    host_token_hit: bool,
    strong_domain_host: bool,
    domain_score: int,
    name_hit: bool,
    address_ok: bool,
    official_evidence_score: int = 0,
) -> bool:
    strong_domain = host_token_hit or strong_domain_host or domain_score >= 4
    name_or_addr = name_hit or address_ok or official_evidence_score >= 9
    if strong_domain and name_or_addr:
        return True
    # ロジスティクス系では「社名(日本語)とドメイン(英字)が一致しにくい」ケースが多いため、
    # domain_score==3 でも社名/住所の根拠が強ければ公式扱いを許容する（directory対策は前段で実施済み）。
    medium_domain = domain_score >= 3
    if medium_domain and name_hit and (address_ok or official_evidence_score >= 7):
        return True
    if medium_domain and official_evidence_score >= 11:
        return True
    return False


def sanitize_input_address_raw(raw: str | None) -> str:
    """
    入力住所（CSV由来）に混入しがちな「複数住所/部署名/会社概要の断片」を落とし、
    公式判定・照合に使える形へ整形する。DBの生データは保持し、内部照合だけで使う想定。
    """
    s = (raw or "").strip().replace("　", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    # 住所が2つ以上連結されているケース（例: 〒...A棟 〒...B棟）は最初の1件だけ使う
    zip_positions = [m.start() for m in re.finditer(r"〒\s*\d{3}-\d{4}", s)]
    if len(zip_positions) >= 2:
        s = s[zip_positions[0] : zip_positions[1]].strip()

    # 「会社概要の項目」や「部署/担当者名」等が混入している場合、住所らしい部分だけ残す
    cut_markers = (
        "Google",
        "営業時間",
        "設立",
        "資本金",
        "代表者",
        "代表番号",
        "代表",
        "代表取締役",
        "執行役員",
        "役員",
        "取締役",
        "管理本部",
        "電話",
        "TEL",
        "ＴＥＬ",
        "FAX",
        "メール",
        "E-mail",
        "お問い合わせ",
        "メ-ルでのお問い合わせ",
        "■",
    )
    for marker in cut_markers:
        pos = s.find(marker)
        if pos == -1:
            continue
        head = s[:pos].strip()
        # ある程度長く、住所っぽい形を満たす場合のみ切る（過剰カットを避ける）
        if len(head) >= 10 and (looks_like_address(head) or normalize_address(head)):
            s = head
            break

    # 括弧の取りこぼしを軽く修正
    s = s.strip().strip(")）]").strip()
    s = re.sub(r"[()（）\\[\\]]", "", s).strip()

    # 住所として使えないものは空にする（誤判定を防ぐ）
    norm = normalize_address(s) or ""
    if not norm:
        return ""
    if not looks_like_address(norm):
        return ""
    return s

def pick_best_address(expected_addr: str | None, candidates: list[str]) -> str | None:
    def _parse_source(raw: str) -> tuple[str, bool, str]:
        if not raw:
            return "OTHER", False, ""
        s = str(raw).strip()
        tags: list[str] = []
        rest = s
        while True:
            m = re.match(r"^\[([A-Z_]+)\]", rest)
            if not m:
                break
            tags.append(m.group(1))
            rest = rest[m.end():].lstrip()
        source = next((t for t in tags if t in {"JSONLD", "MICRODATA", "TABLE", "LABEL", "HEADER", "FOOTER", "TEXT"}), "OTHER")
        is_hq = "HQ" in tags or "HEADQUARTERS" in tags
        # remove any remaining bracket tags that might slip through
        rest = re.sub(r"^\[[A-Z_]+\]\s*", "", rest)
        return source, is_hq, rest.strip()

    source_bonus = {
        "JSONLD": 6.0,
        "MICRODATA": 6.0,
        "TABLE": 5.0,
        "LABEL": 4.0,
        "FOOTER": 2.0,
        "HEADER": 1.0,
        "TEXT": 0.0,
        "OTHER": 0.0,
    }

    normalized_candidates: list[tuple[str, str, bool]] = []  # (normalized, source, is_hq)
    for cand in candidates:
        source, is_hq, raw_val = _parse_source(cand)
        norm = normalize_address(raw_val)
        if norm:
            normalized_candidates.append((norm, source, is_hq))
    if not normalized_candidates:
        return None

    # TABLE/LABEL/JSONLD 等がある場合は HEADER/FOOTER 由来のノイズを避ける（値の誤格納を減らす）
    has_structured = any(src in {"JSONLD", "MICRODATA", "TABLE", "LABEL"} for _, src, _ in normalized_candidates)
    if has_structured:
        structured_only = [(n, s, hq) for (n, s, hq) in normalized_candidates if s not in {"HEADER", "FOOTER"}]
        if structured_only:
            normalized_candidates = structured_only

    # dedupe while keeping the best source bonus for the same normalized address
    best_by_norm: dict[str, tuple[str, bool]] = {}
    for norm, src, is_hq in normalized_candidates:
        if norm not in best_by_norm:
            best_by_norm[norm] = (src, is_hq)
            continue
        prev_src, prev_hq = best_by_norm[norm]
        prev_score = source_bonus.get(prev_src, 0.0) + (4.0 if prev_hq else 0.0)
        cur_score = source_bonus.get(src, 0.0) + (4.0 if is_hq else 0.0)
        if cur_score > prev_score:
            best_by_norm[norm] = (src, is_hq)
    normalized_candidates = [(n, s, hq) for n, (s, hq) in best_by_norm.items()]
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
        normalized_candidates.sort(
            key=lambda pair: (_score(pair[0]) + source_bonus.get(pair[1], 0.0) + (4.0 if pair[2] else 0.0), len(pair[0])),
            reverse=True,
        )
        return normalized_candidates[0][0]

    expected_norm = normalize_address(expected_addr)
    if not expected_norm:
        normalized_candidates.sort(key=lambda pair: (source_bonus.get(pair[1], 0.0) + (4.0 if pair[2] else 0.0)), reverse=True)
        return normalized_candidates[0][0]

    expected_pref = CompanyScraper._extract_prefecture(expected_norm)
    expected_key = CompanyScraper._addr_key(expected_norm)
    expected_zip_match = ZIP_CODE_RE.search(expected_norm)
    expected_zip = expected_zip_match.group(1) if expected_zip_match else ""
    expected_tokens = KANJI_TOKEN_RE.findall(expected_norm)

    best = normalized_candidates[0][0]
    best_score = float("-inf")
    for cand, src, is_hq in normalized_candidates:
        key = CompanyScraper._addr_key(cand)
        score = 0.0
        if is_hq:
            score += 6.0
        if expected_pref:
            if expected_pref in cand:
                score += 3.0
            else:
                # 都道府県不一致は採用リスクが高いので強めに減点（ただし候補としては保持する）
                score -= 10.0
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
        score += source_bonus.get(src, 0.0)
        if score > best_score:
            best_score = score
            best = cand
    return best

def _strip_leading_tags(value: str) -> str:
    out = value or ""
    while True:
        m = re.match(r"^\[[A-Z_]+\]", out)
        if not m:
            break
        out = out[m.end():].lstrip()
    return out

def _candidate_address_norms(candidates: list[str]) -> list[str]:
    norms: list[str] = []
    for raw in candidates:
        if not raw:
            continue
        norm = normalize_address(_strip_leading_tags(str(raw)))
        if norm:
            norms.append(norm)
    return norms

def _has_hq_tag_for_address(candidates: list[str], target_norm: str) -> bool:
    for raw in candidates:
        if isinstance(raw, str) and "[HQ]" in raw:
            norm = normalize_address(_strip_leading_tags(raw))
            if norm and norm == target_norm:
                return True
    return False

def _has_strong_tag_for_address(candidates: list[str], target_norm: str) -> bool:
    """
    住所候補に [TABLE]/[LABEL]/[JSONLD] など強い構造タグが付いているか。
    """
    for raw in candidates:
        if not isinstance(raw, str):
            continue
        tag_ok = any(tag in raw for tag in ("[TABLE]", "[LABEL]", "[JSONLD]"))
        if not tag_ok:
            continue
        norm = normalize_address(_strip_leading_tags(raw))
        if norm and norm == target_norm:
            return True
    return False

def _address_candidate_ok(
    candidate_norm: str,
    candidates: list[str],
    page_type: str,
    input_addr: str,
    ai_official_selected: bool,
) -> tuple[bool, str]:
    if not candidate_norm:
        return False, "no_valid_address"
    if ai_official_selected:
        return True, ""
    has_hq_tag = _has_hq_tag_for_address(candidates, candidate_norm)
    if has_hq_tag:
        return True, ""
    has_strong_tag = _has_strong_tag_for_address(candidates, candidate_norm)
    if has_strong_tag:
        return True, ""
    addr_match = bool(input_addr) and addr_compatible(input_addr, candidate_norm)
    has_zip = bool(ZIP_CODE_RE.search(candidate_norm))
    has_city = bool(CITY_RE.search(candidate_norm))
    only_one = len(set(_candidate_address_norms(candidates))) == 1
    page_ok = page_type in {"COMPANY_PROFILE", "ACCESS_CONTACT"}
    if addr_match and (page_ok or has_zip or has_city):
        return True, ""
    if page_ok and only_one and (has_zip or has_city):
        return True, ""
    return False, "no_hq_marker"

def _strip_rep_tags(value: str) -> tuple[str, list[str]]:
    out = value or ""
    tags: list[str] = []
    while True:
        m = re.match(r"^\[([A-Z_]+)\]", out)
        if not m:
            break
        tags.append(m.group(1))
        out = out[m.end():].lstrip()
    out = re.sub(r"\s+", " ", out).strip()
    return out, tags

def _rep_candidate_meta(candidates: list[str], chosen: str) -> dict[str, bool]:
    chosen_norm = re.sub(r"\s+", " ", chosen or "").strip()
    if not chosen_norm:
        return {"low_role": False, "table": False, "label": False, "role": False, "jsonld": False}
    for raw in candidates:
        base, tags = _strip_rep_tags(str(raw))
        if base == chosen_norm:
            tag_set = set(tags)
            return {
                "low_role": "LOWROLE" in tag_set,
                "table": "TABLE" in tag_set,
                "label": "LABEL" in tag_set,
                "role": "ROLE" in tag_set,
                "jsonld": "JSONLD" in tag_set,
            }
    return {"low_role": False, "table": False, "label": False, "role": False, "jsonld": False}

def _is_profile_like_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        path = urlparse(url).path or ""
        try:
            path = unquote(path)
        except Exception:
            pass
        path = path.lower()
    except Exception:
        return False
    return any(
        seg in path
        for seg in (
            # 英語/よくあるパス
            "/company",
            "/company-info",
            "/companyinfo",
            "/about",
            "/about-us",
            "/aboutus",
            "/corporate",
            "/profile",
            "/overview",
            "/summary",
            "/outline",
            "/guide",
            # 日本語/よくあるローマ字揺れ
            "/会社概要",
            "/会社案内",
            "/会社情報",
            "/企業情報",
            "/企業概要",
            "/法人概要",
            "/gaiyo",
            "/gaiyou",
            "/kaisya",
            "/kaisha",
            "/annai",
        )
    )


def _pick_reference_homepage(page_type_per_url: dict[str, str] | None) -> str:
    """
    「真」として扱う参照URL（会社概要/会社案内/企業情報系）を一意に決める。
    例外が起きても処理全体を落とさない（参照URLは空欄で続行）。
    """
    if not page_type_per_url:
        return ""
    candidates: list[tuple[int, int, int, str]] = []
    try:
        for u, pt in (page_type_per_url or {}).items():
            if not u:
                continue
            pt_s = str(pt or "")
            if pt_s != "COMPANY_PROFILE" and not _is_profile_like_url(u):
                continue
            try:
                path = urlparse(u).path or ""
                path = unquote(path)
                path_low = path.lower()
            except Exception:
                path_low = ""
            candidates.append(
                (
                    0 if pt_s == "COMPANY_PROFILE" else 1,
                    0 if any(seg in path_low for seg in ("/company/overview", "/overview", "/outline", "/summary")) else 1,
                    len(u),
                    u,
                )
            )
    except Exception:
        return ""
    if not candidates:
        return ""
    candidates.sort()
    return candidates[0][3] or ""

def _is_news_like_url(url: str | None) -> bool:
    """
    公式判定で「記事/リリース/ニュース」系が強く混ざるURLを検出する。
    子会社紹介/グループニュース等は住所が一致しても誤って公式採用されやすいので、早期確定を抑止する。
    """
    if not url:
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(seg in path for seg in (
        "/news",
        "/release",
        "/press",
        "/topics",
        "/media",
        "/information",
        "/blog",
        "/article",
        "/post",
    ))

def _is_contact_like_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(seg in path for seg in ("/contact", "/contactus", "/inquiry", "/toiawase", "/otoiawase", "/support", "/form"))

def _is_greeting_like_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(seg in path for seg in ("/message", "/greeting", "/aisatsu", "/goaisatsu", "/president", "/topmessage", "/ceo"))

def _rep_candidate_ok(
    chosen: str | None,
    candidates: list[str],
    page_type: str,
    source_url: str | None,
) -> tuple[bool, str]:
    if not chosen:
        return False, "no_rep"
    # 代表者名は contact 系ページからは採用しない（誤取得/誤格納を避ける）
    if _is_contact_like_url(source_url):
        return False, "contact_like_url"
    meta = _rep_candidate_meta(candidates, chosen)
    low_role = meta.get("low_role", False)
    profile_like = _is_profile_like_url(source_url)
    greeting_like = _is_greeting_like_url(source_url)
    strong_source = (
        meta.get("table", False)
        or meta.get("label", False)
        or meta.get("role", False)
        or meta.get("jsonld", False)
    )
    if low_role:
        return False, "low_role"
    # 挨拶/メッセージ系ページは page_type が OTHER でも採用を許可する
    if greeting_like:
        if low_role:
            return False, "low_role_greeting"
        # ただし「役職語とのペア」由来（TABLE/LABEL/ROLE/JSONLD）のみ採用する
        if strong_source:
            return True, ""
        return False, "greeting_not_paired"
    if REP_REQUIRE_STRUCTURED_SOURCE and (not strong_source):
        return False, "not_structured_source"
    if page_type == "COMPANY_PROFILE":
        return True, ""
    if page_type == "ACCESS_CONTACT":
        return False, "contact_forbidden"
    if page_type == "BASES_LIST":
        if low_role:
            return False, "low_role_bases"
        if profile_like or strong_source:
            return True, ""
        return False, "bases_not_profile"
    if page_type == "OTHER":
        if low_role:
            return False, "low_role_other"
        # OTHER は誤爆が多い（検索UI/フッター断片等）。URLがプロフィール導線である場合のみ許可する。
        if profile_like:
            return True, ""
        return False, "other_not_profile"
    return False, f"not_profile:{page_type}"


def _split_bracket_tags(raw: str) -> tuple[set[str], str]:
    rest = str(raw or "").strip()
    tags: list[str] = []
    while True:
        m = re.match(r"^\[([A-Z_]+)\]", rest)
        if not m:
            break
        tags.append(m.group(1))
        rest = rest[m.end() :].lstrip()
    rest = re.sub(r"^\[[A-Z_]+\]\s*", "", rest)
    return set(tags), rest.strip()


def pick_best_phone(candidates: list[str]) -> str | None:
    def _split_tags(raw: str) -> tuple[set[str], str]:
        return _split_bracket_tags(raw)

    source_bonus = {
        "TELHREF": 6.0,
        "TABLE": 5.0,
        "JSONLD": 5.0,
        "LABEL": 4.0,
        "TEL": 3.5,
        "FOOTER": 1.5,
        "HEADER": 1.0,
        "ATTR": 0.5,
        "TEXT": 0.0,
        # 文脈タグ（CompanyScraper.extract_candidates が付与）
        "HQ": 4.0,        # 本社/本店/本部
        "REP": 3.5,       # 代表/代表電話
        "SOUMU": 3.0,     # 総務
        "KEIRI": 3.0,     # 経理
        "ADMIN": 2.0,     # 管理
        "BRANCH": -2.5,   # 支店/営業所/倉庫/センター等
        "RECRUIT": -6.0,  # 採用/求人
        "SUPPORT": -2.0,  # サポート/コールセンター等
    }

    # TABLE/LABEL/JSONLD/TELHREF がある場合は HEADER/FOOTER/ATTR 由来を避ける
    try:
        tag_sets = []
        for cand in candidates or []:
            tags, _ = _split_tags(str(cand))
            tag_sets.append(tags)
        has_structured = any(bool(t & {"TELHREF", "TABLE", "JSONLD", "LABEL"}) for t in tag_sets)
        if has_structured:
            priority_ctx = {"HQ", "REP", "SOUMU", "KEIRI", "ADMIN"}
            filtered: list[str] = []
            for cand in candidates or []:
                tags, _ = _split_tags(str(cand))
                # footer/header/attr はノイズになりやすいが、「本社/代表/管理」等の明確な文脈があれば残す
                if (tags & {"FOOTER", "HEADER", "ATTR"}) and not (tags & priority_ctx):
                    continue
                filtered.append(str(cand))
            if filtered:
                candidates = filtered
    except Exception:
        pass

    best: str | None = None
    best_score = float("-inf")
    for cand in candidates:
        if not cand:
            continue
        raw = str(cand)
        tags, raw_value = _split_tags(raw)
        is_fax = "FAX" in tags
        is_tel = "TEL" in tags or "TELHREF" in tags
        if is_fax and not is_tel:
            continue
        norm = normalize_phone(raw_value)
        if not norm:
            continue
        norm_val = norm

        score = 0.0
        for t in tags:
            score += source_bonus.get(t, 0.0)
        if re.match(r"^(0120|0570)", norm_val):
            score -= 4.0
        score -= abs(len(norm_val) - 12) * 0.2

        if best is None or score > best_score:
            best = norm_val
            best_score = score
            continue
        if score < best_score:
            continue

        # tie-breakers
        if re.match(r"^(0120|0570)", norm_val) and not re.match(r"^(0120|0570)", best):
            continue
        if re.match(r"^(0120|0570)", best) and not re.match(r"^(0120|0570)", norm_val):
            best = norm_val
            continue
        if len(norm_val) == len(best):
            continue
        if abs(len(norm_val) - 12) < abs(len(best) - 12):
            best = norm_val
    return best


def score_phone_candidate(raw_candidate: str, page_type: str | None = None) -> tuple[str | None, float]:
    """
    1つの電話候補（CompanyScraper.extract_candidates の raw 文字列）をスコアリングする。
    - 返り値: (normalized_phone or None, score)
    """
    tags, raw_value = _split_bracket_tags(raw_candidate)
    is_fax = "FAX" in tags
    is_tel = "TEL" in tags or "TELHREF" in tags
    if is_fax and not is_tel:
        return None, float("-inf")
    norm = normalize_phone(raw_value)
    if not norm:
        return None, float("-inf")

    pt = (page_type or "OTHER").strip().upper()
    # page_type が弱くても「構造化ソース」または「本社/代表/管理」文脈なら許可する
    strong_source = bool(tags & {"TELHREF", "TABLE", "JSONLD", "LABEL"})
    priority_ctx = bool(tags & {"HQ", "REP", "SOUMU", "KEIRI", "ADMIN"})
    if pt not in {"COMPANY_PROFILE", "ACCESS_CONTACT"} and not (strong_source or priority_ctx):
        return None, float("-inf")

    source_bonus = {
        "TELHREF": 6.0,
        "TABLE": 5.0,
        "JSONLD": 5.0,
        "LABEL": 4.0,
        "TEL": 3.5,
        "FOOTER": 1.5,
        "HEADER": 1.0,
        "ATTR": 0.5,
        "TEXT": 0.0,
        # 文脈タグ（CompanyScraper.extract_candidates が付与）
        "HQ": 4.0,
        "REP": 3.5,
        "SOUMU": 3.0,
        "KEIRI": 3.0,
        "ADMIN": 2.0,
        "BRANCH": -2.5,
        "RECRUIT": -6.0,
        "SUPPORT": -2.0,
    }
    page_type_bonus = {
        "COMPANY_PROFILE": 2.0,
        "ACCESS_CONTACT": 1.0,
        "BASES_LIST": -1.0,
        "DIRECTORY_DB": -2.0,
        "OTHER": 0.0,
    }

    score = page_type_bonus.get(pt, 0.0)
    for t in tags:
        score += source_bonus.get(t, 0.0)

    if re.match(r"^(0120|0570)", norm):
        score -= 4.0
    score -= abs(len(norm) - 12) * 0.2
    return norm, score


def pick_best_phone_from_entries(entries: list[tuple[str, str, str]]) -> tuple[str | None, str | None]:
    """
    複数ページにまたがる電話候補から、最も良い1つを選ぶ。
    entries: [(raw_candidate, url, page_type)]
    """
    best_phone: str | None = None
    best_url: str | None = None
    best_score = float("-inf")
    for raw, url, pt in entries or []:
        norm, score = score_phone_candidate(raw, pt)
        if not norm:
            continue
        if best_phone is None or score > best_score:
            best_phone = norm
            best_url = url
            best_score = score
            continue
        if score < best_score:
            continue
        # tie-breakers: prefer non 0120/0570
        if re.match(r"^(0120|0570)", norm) and not re.match(r"^(0120|0570)", best_phone):
            continue
        if re.match(r"^(0120|0570)", best_phone) and not re.match(r"^(0120|0570)", norm):
            best_phone = norm
            best_url = url
            best_score = score
            continue
        if abs(len(norm) - 12) < abs(len(best_phone) - 12):
            best_phone = norm
            best_url = url
            best_score = score
    return best_phone, best_url

def _has_strong_phone_source(candidates: list[str]) -> bool:
    """
    page_type が誤分類でも採用できる程度に強い電話ソースか。
    """
    for raw in candidates or []:
        if not isinstance(raw, str):
            continue
        if raw.startswith(("[TELHREF]", "[TABLE]", "[LABEL]", "[JSONLD]")):
            return True
    return False


def ai_official_hint_from_judge(ai_judge: dict[str, Any] | None, min_conf: float) -> bool:
    """
    AIが公式可能性が高いと返した候補を「除外」ではなく暫定候補として扱うための判定。
    """
    if not isinstance(ai_judge, dict):
        return False
    is_official_site = ai_judge.get("is_official_site")
    if is_official_site is None:
        is_official_site = ai_judge.get("is_official")
    conf = ai_judge.get("official_confidence")
    if conf is None:
        conf = ai_judge.get("confidence")
    try:
        conf_f = float(conf) if conf is not None else 0.0
    except Exception:
        conf_f = 0.0
    return bool(is_official_site is True and conf_f >= float(min_conf or 0.0))

def is_over_deep_limit(total_elapsed: float, homepage: str | None, official_phase_end: float, time_limit_deep: float) -> bool:
    if time_limit_deep <= 0 or not homepage:
        return False
    if official_phase_end > 0:
        return (total_elapsed - official_phase_end) > time_limit_deep
    return total_elapsed > time_limit_deep

def pick_best_rep(names: list[str], source_url: str | None = None) -> str | None:
    role_keywords = ("代表", "取締役", "社長", "理事長", "会長", "院長", "学長", "園長", "代表社員", "CEO", "COO")
    blocked = (
        "スタッフ",
        "紹介",
        "求人",
        "採用",
        "ニュース",
        "退任",
        "就任",
        "人事",
        "異動",
        "お知らせ",
        "プレス",
        "取引",
        # 代表者名として誤爆しやすいラベル類
        "従業員",
        "社員数",
        "職員数",
        "人数",
    )
    cta_words = ("こちら", "詳しく", "クリック", "タップ", "link")
    era_words = ("昭和", "平成", "令和", "西暦")
    rep_noise_words = (
        "社是",
        "社訓",
        "スローガン",
        "理念",
        "方針",
        "ビジョン",
        "ミッション",
        "バリュー",
        "利用者",
        "お客様",
        "皆様",
        "方々",
        "の方",
    )
    url_bonus = 3 if source_url and any(seg in source_url for seg in ("/company", "/about", "/corporate")) else 0
    best = None
    best_score = float("-inf")
    for raw in names:
        if not raw:
            continue
        cleaned_raw = str(raw).strip()
        cleaned, tags = _strip_rep_tags(cleaned_raw)
        tag_set = set(tags)
        is_table = "TABLE" in tag_set
        is_label = "LABEL" in tag_set
        low_role = "LOWROLE" in tag_set
        if not cleaned:
            continue
        # 会社名/法人名の混入（例: 2文字程度の漢字や社名語尾）を弾く
        if re.search(r"(従業員|社員|職員|スタッフ).{0,2}数$", cleaned):
            continue
        if cleaned in {"従業員", "従業員数", "社員数", "職員数", "人数"}:
            continue
        if re.search(r"(株式会社|有限会社|合同会社|合名会社|合資会社|グループ|ホールディングス)", cleaned):
            continue
        if low_role:
            continue
        lower_cleaned = cleaned.lower()
        if any(w in cleaned for w in cta_words) or any(w in lower_cleaned for w in cta_words):
            continue
        if any(w in cleaned for w in era_words):
            continue
        if re.search(r"\\d", cleaned) and re.search(r"(年|月|日)", cleaned):
            continue
        if re.search(r"\\d", cleaned):
            continue
        has_hiragana = bool(re.search(r"[\u3041-\u3096]", cleaned))
        has_kanji = bool(re.search(r"[\u4E00-\u9FFF]", cleaned))
        if has_hiragana and has_kanji:
            continue
        if not (NAME_CHUNK_RE.search(cleaned) or KANA_NAME_RE.search(cleaned)):
            continue
        if any(w in cleaned for w in rep_noise_words):
            continue
        if any(b in cleaned for b in blocked):
            continue
        score = len(cleaned) + url_bonus
        if is_table:
            score += 5
        if is_label:
            score += 3
        if any(k in cleaned for k in role_keywords):
            score += 8
        token_count = len([t for t in cleaned.split() if t])
        if token_count >= 3:
            score -= 6
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
    raw = normalize_kanji_numbers(raw)
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


def clean_employee_value(val: str) -> str:
    raw = unicodedata.normalize("NFKC", (val or "").strip())
    if not raw:
        return ""
    raw = raw.replace(",", "").replace("，", "")
    range_m = EMPLOYEE_RANGE_RE.search(raw)
    if range_m:
        a = range_m.group(1)
        b = range_m.group(2)
        if a and b:
            return f"{a}-{b}名"
    m = EMPLOYEE_VALUE_RE.search(raw)
    if m:
        prefix = m.group(1) or ""
        num = m.group(2)
        if num:
            return f"{prefix}{num}名"
    if re.fullmatch(r"[0-9]{1,6}", raw) and not re.search(r"(年|月|日|年度|期)", raw):
        return f"{raw}名"
    return ""


def pick_best_employee(candidates: list[str]) -> str | None:
    best = None
    best_score = float("-inf")
    for cand in candidates or []:
        if not cand:
            continue
        is_table = False
        value = str(cand)
        if value.startswith("[TABLE]"):
            is_table = True
            value = value.replace("[TABLE]", "", 1)
        cleaned = clean_employee_value(value)
        if not cleaned:
            continue
        score = 0.0
        if is_table:
            score += 3.0
        if "約" in cleaned:
            score -= 0.2
        if "-" in cleaned:
            score -= 0.3
        score += min(len(cleaned), 10) * 0.1
        if score > best_score:
            best_score = score
            best = cleaned
    return best


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
    # 企業DB/まとめサイトの定型文を除外
    if ("サイト" in stripped or "ページ" in stripped) and any(
        w in stripped
        for w in (
            "データベース",
            "登録企業",
            "掲載",
            "企業詳細",
            "会社情報を掲載",
            "企業情報を掲載",
            "口コミ",
            "評判",
            "ランキング",
        )
    ):
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
    if len(stripped) < DESCRIPTION_MIN_LEN:
        return ""
    if re.fullmatch(r"(会社概要|事業概要|法人概要|沿革|会社案内|企業情報)", stripped):
        return ""
    # 複数文の場合、使える1文だけを拾う（末尾に採用/問い合わせ等が付いても落としすぎない）
    candidates = [stripped]
    if "。" in stripped or "．" in stripped:
        parts = [p.strip() for p in re.split(r"[。．]", stripped) if p.strip()]
        candidates = parts or candidates

    for cand in candidates:
        if cand in GENERIC_DESCRIPTION_TERMS:
            continue
        if len(cand) < DESCRIPTION_MIN_LEN:
            continue
        if any(term in cand for term in ("お問い合わせ", "お問合せ", "アクセス", "予約", "営業時間")):
            continue
        if any(word in cand for word in policy_blocks):
            continue
        if ("サイト" in cand or "ページ" in cand) and any(
            w in cand
            for w in (
                "データベース",
                "登録企業",
                "掲載",
                "企業詳細",
                "会社情報を掲載",
                "企業情報を掲載",
                "口コミ",
                "評判",
                "ランキング",
            )
        ):
            continue
        if re.search(r"https?://|mailto:|@|＠|tel[:：]|電話|ＴＥＬ|ＴＥＬ：", cand, flags=re.I):
            continue
        # 事業内容を示すキーワードが全く無い場合だけ除外
        if not any(k in cand for k in DESCRIPTION_BIZ_KEYWORDS):
            continue
        return _truncate_description(cand)
    return ""

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
        r"会社概要", r"事業概要", r"法人概要", r"沿革", r"アクセス", r"お問い合わせ", r"お問合せ", r"営業時間"
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
    noise_re = re.compile(r"(お問い合わせ|お問合せ|アクセス|採用|求人|募集|news|menu|nav|http|https|tel[:：]|電話)", re.I)
    for tag in soup.find_all(["h1", "h2", "p"], limit=8):
        text = tag.get_text(separator=" ", strip=True)
        if noise_re.search(text):
            continue
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

def _truncate_final_description(text: str, max_len: int = FINAL_DESCRIPTION_MAX_LEN) -> str:
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    truncated = text[:max_len]
    truncated = re.sub(r"[、。．,;]+$", "", truncated)
    trimmed = re.sub(r"\s+\S*$", "", truncated).strip()
    return trimmed if len(trimmed) >= max(10, FINAL_DESCRIPTION_MIN_LEN // 2) else truncated.rstrip()


def _ensure_name_industry_in_description(description: str, company_name: str, industry: str) -> str:
    desc = (description or "").strip()
    if not desc:
        return ""
    name = (company_name or "").strip()
    ind = (industry or "").strip()
    if ind == "不明":
        ind = ""
    if name and name in desc:
        if ind and (ind not in desc):
            # 最初の企業名だけに業種を付与
            return desc.replace(name, f"{name}（{ind}）", 1)
        return desc
    # 先頭が一人称だと「会社名は、当社は…」になりやすいので削る
    desc_body = desc.rstrip("。").strip()
    desc_body = re.sub(r"^(?:当社|弊社|私たち|わたしたち|当グループ)\s*(?:は|が)?[、,]?\s*", "", desc_body)
    if not name:
        return (desc_body + "。") if desc_body and not desc_body.endswith("。") else desc_body
    if ind:
        return f"{name}（{ind}）は、{desc_body}。"
    return f"{name}は、{desc_body}。"

def build_final_description_from_payloads(
    payloads: list[dict[str, Any]],
    *,
    min_len: int = FINAL_DESCRIPTION_MIN_LEN,
    max_len: int = FINAL_DESCRIPTION_MAX_LEN,
) -> str:
    """
    既取得テキストから「事業内容のみ」の description を 80〜160字（既定）に寄せて組み立てる。
    追加AI呼び出しはしない。
    """
    if not payloads:
        return ""

    candidates: list[str] = []
    seen: set[str] = set()

    def _push(raw: str | None) -> None:
        if not raw:
            return
        cleaned = clean_description_value(raw)
        if not cleaned:
            return
        if looks_mojibake(cleaned):
            return
        if cleaned in seen:
            return
        seen.add(cleaned)
        candidates.append(cleaned)

    for p in payloads:
        try:
            _push(extract_description_from_payload(p))
        except Exception:
            pass
        try:
            text = p.get("text", "") or ""
            if text:
                biz = select_relevant_paragraphs(text, limit=6)
                for line in [x.strip() for x in biz.splitlines() if x.strip()]:
                    _push(line)
        except Exception:
            pass

    if not candidates:
        return ""

    def _cand_score(s: str) -> tuple[int, int, int]:
        kw_hits = sum(1 for kw in DESCRIPTION_BIZ_KEYWORDS if kw and kw in s)
        # 120字付近を好む
        target_penalty = abs(len(s) - 120)
        return (kw_hits, -target_penalty, len(s))

    candidates.sort(key=_cand_score, reverse=True)
    chosen: list[str] = []
    used: set[str] = set()

    for cand in candidates:
        if cand in used:
            continue
        if not chosen:
            chosen.append(cand)
            used.add(cand)
            if len(cand) >= min_len:
                break
            continue
        # 2文までに抑える
        if len(chosen) >= 2:
            break
        joined_probe = "。".join([*chosen, cand]).strip("。") + "。"
        joined_probe = re.sub(r"\s+", " ", joined_probe).strip()
        if len(joined_probe) > max_len:
            continue
        chosen.append(cand)
        used.add(cand)
        if len(joined_probe) >= min_len:
            break

    out = "。".join([c.strip("。") for c in chosen if c.strip()]).strip()
    if out and not out.endswith("。") and any(k in out for k in ("。", "．")):
        # 既に文末が句点系ならそのまま
        pass
    elif out and not out.endswith("。"):
        out += "。"

    out = re.sub(r"\s+", " ", out).strip()
    if not out:
        return ""
    out = _truncate_final_description(out, max_len=max_len)
    # 最終的に事業キーワードが無いものは落とす（DB汚染を防ぐ）
    if not any(k in out for k in DESCRIPTION_BIZ_KEYWORDS):
        return ""
    # min_len 未満でも、材料不足のときは空欄よりマシなので返す（保存側で上書き条件を調整）
    return out

def _collect_business_text_blocks(payloads: list[dict[str, Any]]) -> list[str]:
    blocks: list[str] = []
    for p in payloads or []:
        try:
            text = (p.get("text", "") or "").strip()
            if text:
                biz = select_relevant_paragraphs(text, limit=3)
                if biz:
                    blocks.append(biz[:1200])
        except Exception:
            pass
        try:
            html = p.get("html", "") or ""
            meta = extract_meta_description(html)
            if meta:
                blocks.append(meta)
        except Exception:
            pass
    # 重複除去（順序保持）
    out: list[str] = []
    seen2: set[str] = set()
    for b in blocks:
        bb = (b or "").strip()
        if not bb or bb in seen2:
            continue
        seen2.add(bb)
        out.append(bb)
    return out[:10]

def infer_industry_and_business_tags(text_blocks: list[str]) -> tuple[str, list[str]]:
    """
    既取得のテキストから業種とタグを推定（追加AI呼び出し無し）。
    """
    joined = "\n".join([b for b in (text_blocks or []) if b and str(b).strip()]).strip()
    if not joined:
        return ("", [])
    s = unicodedata.normalize("NFKC", joined)
    s_lower = s.lower()

    rules: dict[str, list[tuple[str, int]]] = {
        "物流・運送": [("物流", 4), ("運送", 4), ("配送", 3), ("倉庫", 3), ("輸送", 2), ("通関", 2), ("フォワーダー", 2), ("海運", 2), ("陸運", 2), ("航空", 1)],
        "建設・設備": [("建設", 4), ("工事", 4), ("施工", 3), ("土木", 3), ("設備", 3), ("電気工事", 4), ("管工事", 4), ("解体", 3), ("リフォーム", 2), ("設計", 2)],
        "製造": [("製造", 4), ("加工", 3), ("工場", 2), ("部品", 2), ("金属", 2), ("樹脂", 2), ("組立", 2), ("生産", 2), ("機械", 1), ("装置", 1)],
        "IT・ソフトウェア": [("ソフトウェア", 4), ("システム", 3), ("it", 3), ("saas", 4), ("クラウド", 4), ("dx", 3), ("アプリ", 2), ("web", 2), ("開発", 1), ("ai", 1), ("データ", 1)],
        "不動産": [("不動産", 4), ("賃貸", 3), ("仲介", 3), ("売買", 3), ("管理", 2), ("マンション", 2), ("テナント", 2), ("物件", 2)],
        "医療・福祉": [("医療", 4), ("介護", 4), ("福祉", 3), ("クリニック", 3), ("病院", 3), ("訪問", 1), ("看護", 2)],
        "飲食・食品": [("食品", 4), ("飲食", 4), ("レストラン", 3), ("カフェ", 3), ("製菓", 2), ("惣菜", 2), ("給食", 2)],
        "小売・EC": [("小売", 4), ("販売", 2), ("通販", 4), ("ec", 3), ("ショップ", 2), ("店舗", 1), ("卸", 2)],
        "人材・HR": [("人材", 4), ("派遣", 4), ("紹介", 2), ("求人", 3), ("採用支援", 3), ("転職", 3)],
        "コンサルティング": [("コンサル", 4), ("コンサルティング", 4), ("顧問", 2), ("支援", 1)],
        "金融・保険": [("金融", 4), ("保険", 4), ("銀行", 4), ("証券", 3), ("融資", 2)],
        "教育": [("教育", 4), ("学習", 3), ("塾", 3), ("スクール", 3), ("研修", 2)],
        "広告・マーケ": [("広告", 4), ("マーケ", 4), ("マーケティング", 4), ("pr", 3), ("デザイン", 2), ("制作", 1)],
        "エネルギー": [("エネルギー", 4), ("電力", 4), ("ガス", 3), ("太陽光", 4), ("再生可能", 3)],
        "清掃・警備": [("清掃", 4), ("警備", 4), ("ビルメンテ", 3), ("施設管理", 3)],
        "士業": [("税理士", 4), ("弁護士", 4), ("行政書士", 4), ("社労士", 4), ("司法書士", 4)],
    }

    industry_scores: dict[str, int] = {}
    tag_scores: dict[str, int] = {}
    for industry, kws in rules.items():
        score = 0
        for kw, w in kws:
            needle = kw.lower()
            if not needle:
                continue
            if needle in s_lower:
                # 出現回数を軽く加味
                count = min(3, s_lower.count(needle))
                score += w * count
                # 代表的なタグ化（英字は上げすぎない）
                tag = kw.upper() if kw.isascii() and len(kw) <= 4 else kw
                tag_scores[tag] = max(tag_scores.get(tag, 0), w)
        if score:
            industry_scores[industry] = score

    if not industry_scores:
        return ("", [])

    best_industry = max(industry_scores.items(), key=lambda x: x[1])[0]
    best_score = industry_scores.get(best_industry, 0)
    # しきい値をやや緩め、弱いシグナルでもヒントを返す
    if best_score < 3:
        return ("", [])

    # タグは上位から最大5件
    tags = [t for t, _ in sorted(tag_scores.items(), key=lambda x: x[1], reverse=True)]
    # ノイズっぽい汎用語は落とす
    tags = [t for t in tags if t not in {"開発", "制作", "支援", "販売"}]
    dedup: list[str] = []
    seen3: set[str] = set()
    for t in tags:
        tt = (t or "").strip()
        if not tt or tt in seen3:
            continue
        seen3.add(tt)
        dedup.append(tt)
        if len(dedup) >= 5:
            break
    return (best_industry, dedup)

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
            continue
        if re.search(r"https?://|mailto:|@|＠|tel[:：]|電話|ＴＥＬ|ＴＥＬ：", line, flags=re.I):
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

def append_jsonl(path: str, payload: dict[str, Any]) -> None:
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        log.debug("append_jsonl failed: %s", path, exc_info=True)

def build_official_ai_text(text: str, html: str, signals: dict[str, Any] | None = None) -> str:
    """
    AI公式判定向けに、1回fetch済みの text/html から根拠を落としにくい形で短文化する。
    追加fetchはしない（CPUのみ）。
    """
    parts: list[str] = []
    if text:
        parts.append(str(text))
    if html:
        try:
            parts.append(CompanyScraper._meta_strings(html))
        except Exception:
            pass
        try:
            soup = BeautifulSoup(html, "html.parser")
            h1 = soup.find("h1")
            if h1:
                parts.append(f"[H1] {h1.get_text(' ', strip=True)}")
            header = soup.find("header")
            if header:
                header_text = header.get_text(" ", strip=True)
                if header_text:
                    parts.append(f"[HEADER] {header_text[:200]}")
                logo_hints: list[str] = []
                for node in header.find_all(["img", "a", "span", "div"]):
                    for attr in ("alt", "aria-label", "title"):
                        val = node.get(attr)
                        if not isinstance(val, str):
                            continue
                        val = val.strip()
                        if not val or len(val) > 40:
                            continue
                        if val not in logo_hints:
                            logo_hints.append(val)
                    if len(logo_hints) >= 5:
                        break
                if logo_hints:
                    parts.append(f"[LOGO] {' / '.join(logo_hints)}")
            footer = soup.find("footer")
            if footer:
                ft = footer.get_text(" ", strip=True)
                if ft:
                    parts.append(f"[FOOTER] {ft}")
        except Exception:
            pass
    if signals:
        try:
            keys = (
                "page_type",
                "host",
                "tld",
                "allowed_tld",
                "domain_score",
                "host_token_hit",
                "strong_domain_host",
                "name_match_ratio",
                "name_match_exact",
                "name_match_partial_only",
                "name_match_source",
                "official_evidence_score",
                "official_evidence",
                "title_match",
                "h1_match",
                "og_site_name_match",
                "directory_like",
            )
            sig_parts = []
            for key in keys:
                if key not in signals or signals[key] is None:
                    continue
                val = signals[key]
                if isinstance(val, bool):
                    val_str = "true" if val else "false"
                elif isinstance(val, float):
                    val_str = f"{val:.2f}"
                elif isinstance(val, (list, tuple, set)):
                    items = [str(x).strip() for x in val if str(x).strip()]
                    if not items:
                        continue
                    val_str = ",".join(items[:12])
                else:
                    val_str = str(val)
                if val_str:
                    sig_parts.append(f"{key}={val_str}")
            if sig_parts:
                parts.append(f"[SIGNALS] {' '.join(sig_parts)}")
        except Exception:
            pass
    joined = "\n".join([p for p in parts if p and str(p).strip()])
    try:
        joined = CompanyScraper._filter_noise_lines(joined)
    except Exception:
        pass
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


async def ensure_info_has_screenshot(
    scraper: CompanyScraper,
    url: str,
    info: dict[str, Any] | None,
    need_screenshot: bool = True,
    policy: str = "auto",
) -> dict[str, Any]:
    info = info or {}
    if info.get("screenshot") or not need_screenshot:
        return info

    policy = (policy or "auto").strip().lower()
    if policy in {"never", "off", "false", "0"}:
        return info

    if policy == "auto":
        text = (info.get("text") or "").strip()
        html = info.get("html") or ""
        spa_hint = any(token in html for token in ("__NEXT_DATA__", "data-reactroot", "id=\"root\"", "id=\"app\"", "nuxt"))
        if not spa_hint and (len(text) >= AI_SCREENSHOT_TEXT_MIN or len(html) >= AI_SCREENSHOT_HTML_MIN):
            return info

    def _timeout_sec() -> float:
        try:
            pt_ms = float(getattr(scraper, "page_timeout_ms", 7000) or 7000)
        except Exception:
            pt_ms = 7000.0
        return max(5.0, min(35.0, (pt_ms / 1000.0) + 8.0))

    try:
        refreshed = await asyncio.wait_for(
            scraper.get_page_info(url, need_screenshot=True),
            timeout=_timeout_sec(),
        )
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
    allow_slow: bool = False,
) -> dict[str, Any]:
    """
    テキスト/HTMLのみ不足している場合に軽量に再取得する（スクショは撮らない）。
    """
    info = info or {}
    if info.get("text") and info.get("html"):
        return info

    def _timeout_sec() -> float:
        try:
            pt_ms = float(getattr(scraper, "page_timeout_ms", 7000) or 7000)
        except Exception:
            pt_ms = 7000.0
        return max(4.0, min(25.0, (pt_ms / 1000.0) + 6.0))

    try:
        host = ""
        if allow_slow:
            try:
                host = urlparse(url).netloc.lower().split(":")[0]
            except Exception:
                host = ""
        is_slow = bool(host and allow_slow and scraper._is_slow_host(host))  # type: ignore[attr-defined]
        if is_slow:
            timeout_ms = min(getattr(scraper, "http_timeout_ms", 6000), 2500)
            refreshed = await scraper._fetch_http_info(url, timeout_ms=timeout_ms, allow_slow=True)
        else:
            refreshed = await asyncio.wait_for(
                scraper.get_page_info(url, need_screenshot=False, allow_slow=allow_slow),
                timeout=_timeout_sec(),
            )
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

def _now_utc_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _pick_update_check_url(company: Dict[str, Any]) -> tuple[str, str]:
    final_homepage = (company.get("final_homepage") or "").strip()
    if final_homepage:
        return final_homepage, "final_homepage"
    if UPDATE_CHECK_FINAL_ONLY:
        return "", ""
    homepage = (company.get("homepage") or "").strip()
    if homepage and int(company.get("homepage_official_flag") or 0) == 1:
        return homepage, "homepage"
    return "", ""

async def maybe_skip_if_unchanged(
    company: Dict[str, Any],
    scraper: CompanyScraper,
    manager: DatabaseManager,
) -> bool:
    if not UPDATE_CHECK_ENABLED:
        return False
    url, url_source = _pick_update_check_url(company)
    if not url:
        return False
    prev_url = (company.get("homepage_check_url") or "").strip()
    try:
        info = await scraper._fetch_http_info(  # type: ignore[attr-defined]
            url,
            timeout_ms=UPDATE_CHECK_TIMEOUT_MS,
            allow_slow=UPDATE_CHECK_ALLOW_SLOW,
        )
    except Exception:
        return False
    html = info.get("html") or ""
    text = info.get("text") or ""
    if not (html or text):
        return False
    fingerprint = scraper.compute_homepage_fingerprint(html, text)
    if not fingerprint:
        return False
    now_str = _now_utc_str()
    content_length = len(html or text)
    prev_fp = (company.get("homepage_fingerprint") or "").strip()
    if not prev_fp:
        company["homepage_check_url"] = url
        company["homepage_checked_at"] = now_str
        company["homepage_content_length"] = content_length
        company["homepage_fingerprint"] = fingerprint
        company["homepage_check_source"] = url_source
        return False
    url_matches = (not prev_url) or (prev_url == url)
    unchanged = bool(url_matches and prev_fp == fingerprint)
    if not unchanged:
        company["homepage_check_url"] = url
        company["homepage_checked_at"] = now_str
        company["homepage_content_length"] = content_length
        company["homepage_fingerprint"] = fingerprint
        company["homepage_check_source"] = url_source
        return False
    manager.save_update_check_result(
        company_id=int(company.get("id") or 0),
        status="done",
        homepage_fingerprint=fingerprint,
        homepage_content_length=content_length,
        homepage_checked_at=now_str,
        homepage_check_url=url,
        homepage_check_source=url_source,
        skip_reason="homepage_unchanged",
    )
    log.info("[skip] homepage unchanged -> done (id=%s url=%s)", company.get("id"), url)
    return True

# --------------------------------------------------
# メイン処理（ワーカー）
# --------------------------------------------------
async def process():
    global TIME_LIMIT_FETCH_ONLY, TIME_LIMIT_WITH_OFFICIAL, TIME_LIMIT_DEEP
    log.info(
        "=== Runner started (worker=%s) === HEADLESS=%s USE_AI=%s MAX_ROWS=%s "
        "ID_MIN=%s ID_MAX=%s AI_COOLDOWN_SEC=%s SLEEP_BETWEEN_SEC=%s JITTER_RATIO=%.2f "
        "MIRROR_TO_CSV=%s",
        WORKER_ID, HEADLESS, USE_AI, MAX_ROWS, ID_MIN, ID_MAX,
        AI_COOLDOWN_SEC, SLEEP_BETWEEN_SEC, JITTER_RATIO, MIRROR_TO_CSV
    )

    scraper = CompanyScraper(headless=HEADLESS)
    base_page_timeout_ms = getattr(scraper, "page_timeout_ms", 9000) or 9000
    normal_page_timeout_ms = getattr(scraper, "page_timeout_ms", base_page_timeout_ms)
    normal_slow_page_threshold_ms = getattr(scraper, "slow_page_threshold_ms", base_page_timeout_ms)
    PAGE_FETCH_TIMEOUT_SEC = max(5.0, (base_page_timeout_ms / 1000.0) + 5.0)
    # Playwright起動は get_page_info 内で必要時のみ行う（未導入環境での起動失敗や待ちを避ける）

    verifier = AIVerifier() if USE_AI else None
    manager = DatabaseManager(db_path=COMPANIES_DB_PATH, worker_id=WORKER_ID)
    if AI_CLEAR_NEGATIVE_FLAGS:
        cleared = manager.clear_ai_negative_url_flags()
        log.info("Cleared %s AI negative url_flags before evaluation.", cleared)

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
        timeouts_extended = False

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
                    TIME_LIMIT_FETCH_ONLY = LONG_TIME_LIMIT_FETCH_ONLY
                    TIME_LIMIT_WITH_OFFICIAL = LONG_TIME_LIMIT_WITH_OFFICIAL
                    TIME_LIMIT_DEEP = LONG_TIME_LIMIT_DEEP
                    try:
                        scraper.page_timeout_ms = LONG_PAGE_TIMEOUT_MS
                        scraper.slow_page_threshold_ms = LONG_SLOW_PAGE_THRESHOLD_MS
                    except Exception:
                        pass
                    timeouts_extended = True
                    continue
                log.info("キューが空です。終了。")
                break

            cid = company.get("id")
            name = (company.get("company_name") or "").strip()
            # 入力住所は csv_address を優先（古いDBは address に入っているためフォールバック）
            addr_raw_original = (company.get("csv_address") or company.get("address") or "").strip()
            if not (company.get("csv_address") or "").strip():
                company["csv_address"] = addr_raw_original
            # 照合用は「混入ノイズ」を落としたものを使う（DBの生値は保持）
            addr_raw = sanitize_input_address_raw(addr_raw_original) or addr_raw_original
            addr = normalize_address(addr_raw) or ""
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

            if await maybe_skip_if_unchanged(company, scraper, manager):
                processed += 1
                continue

            log.info("[%s] %s の処理開始 (worker=%s)", cid, name, WORKER_ID)

            started_at = time.monotonic()
            timed_out = False
            company_has_corp = any(suffix in name for suffix in CompanyScraper.CORP_SUFFIXES)

            # デフォルトを先に用意しておく（途中でタイムアウト/例外が起きても未定義にならないようにする）
            industry_val = ""
            industry_hint_val = ""
            business_tags_val = ""
            industry_major = ""
            industry_middle = ""
            industry_minor = ""
            industry_major_code = ""
            industry_middle_code = ""
            industry_minor_code = ""
            industry_class_source = ""
            industry_class_confidence = 0.0
            contact_url = ""
            contact_url_source = ""
            contact_url_score = 0.0
            contact_url_reason = ""
            contact_url_checked_at = ""
            contact_url_ai_verdict = ""
            contact_url_ai_confidence = 0.0
            contact_url_ai_reason = ""
            contact_url_status = ""
            payloads_for_desc: list[dict[str, Any]] = []
            hard_timeout_candidates = [t for t in (GLOBAL_HARD_TIMEOUT_SEC, COMPANY_HARD_TIMEOUT_SEC, TIME_LIMIT_SEC) if t > 0]
            hard_timeout_sec = min(hard_timeout_candidates) if hard_timeout_candidates else 60.0
            if ABSOLUTE_COMPANY_DEADLINE_SEC > 0:
                hard_timeout_sec = min(hard_timeout_sec, ABSOLUTE_COMPANY_DEADLINE_SEC)
            hard_deadline = started_at + hard_timeout_sec
            timeout_stage = ""
            timeout_saved = False

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

            deep_phase_deadline: float | None = None

            def over_deep_limit() -> bool:
                # 深掘りフェーズの専用上限（公式確定後）
                return is_over_deep_limit(elapsed(), homepage, official_phase_end, TIME_LIMIT_DEEP)

            def over_deep_phase_deadline() -> bool:
                return deep_phase_deadline is not None and time.monotonic() > deep_phase_deadline

            hard_timeout_logged = False
            ai_call_timeout = AI_CALL_TIMEOUT_SEC or 20.0

            def raise_hard_timeout(stage: str) -> None:
                nonlocal timed_out, hard_timeout_logged, timeout_stage
                timed_out = True
                timeout_stage = stage or timeout_stage
                if not company.get("error_code"):
                    company["error_code"] = "timeout"
                if not hard_timeout_logged:
                    log.info("[%s] hard timeout reached (%s) at %.1fs", cid, stage, elapsed())
                    hard_timeout_logged = True
                raise HardTimeout(stage)

            def ensure_global_time(stage: str = "") -> None:
                if over_hard_deadline():
                    raise_hard_timeout(stage or "global_deadline")

            def clamp_timeout(desired: float) -> float:
                remaining = hard_deadline - time.monotonic()
                if remaining <= 0:
                    raise_hard_timeout("global_deadline")
                if desired <= 0:
                    return max(0.1, remaining)
                return max(0.1, min(desired, remaining))

            def bulk_timeout(per_op: float, ops: int, *, slow: bool = False, overhead: float = 0.0) -> float:
                """
                複数ページ取得（priority docs / deep crawl / verify 等）の全体タイムアウト。
                wait_for に per-page 相当の短い値を渡すと、途中で強制中断され取りこぼしが増えるため、
                操作回数に応じてスケールさせる。
                """
                multiplier = 1.4 if slow else 1.15
                return clamp_timeout((per_op * max(1, int(ops or 0)) * multiplier) + float(overhead or 0.0))

            def remaining_time_budget() -> float:
                return hard_deadline - time.monotonic()

            def has_time_for_ai() -> bool:
                return remaining_time_budget() > AI_MIN_REMAINING_SEC

            # タイムアウト時に「分かっている範囲」を確実に保存するためのデフォルト初期化
            urls: list[str] = []
            candidate_records: list[dict[str, Any]] = []
            homepage = (company.get("homepage") or "").strip()
            info: dict[str, Any] | None = None
            primary_cands: dict[str, list[str]] = {}
            fallback_cands: list[tuple[str, dict[str, list[str]]]] = []
            homepage_official_flag = int(company.get("homepage_official_flag") or 0)
            homepage_official_source = company.get("homepage_official_source", "") or ""
            homepage_official_score = float(company.get("homepage_official_score") or 0.0)
            ai_official_selected = False
            ai_time_spent = 0.0
            chosen_domain_score = 0
            search_phase_end = 0.0
            official_phase_end = 0.0
            deep_phase_end = 0.0
            deep_pages_visited = int(company.get("deep_pages_visited") or 0)
            deep_fetch_count = int(company.get("deep_fetch_count") or 0)
            deep_fetch_failures = int(company.get("deep_fetch_failures") or 0)
            deep_skip_reason = company.get("deep_skip_reason", "") or ""
            deep_urls_visited: list[str] = []
            deep_phone_candidates = int(company.get("deep_phone_candidates") or 0)
            deep_address_candidates = int(company.get("deep_address_candidates") or 0)
            deep_rep_candidates = int(company.get("deep_rep_candidates") or 0)

            phone = (company.get("phone") or "").strip()
            found_address = (company.get("found_address") or "").strip()
            rep_name_val = (scraper.clean_rep_name(company.get("rep_name")) or "").strip()
            # description を毎回AI由来にする場合は、既存値でAI生成がブロックされないように初期値を空にする
            description_val = "" if AI_DESCRIPTION_ALWAYS else (company.get("description") or "").strip()
            listing_val = clean_listing_value(company.get("listing") or "")
            revenue_val = clean_amount_value(company.get("revenue") or "")
            profit_val = clean_amount_value(company.get("profit") or "")
            capital_val = clean_amount_value(company.get("capital") or "")
            employees_val = clean_employee_value(company.get("employees") or "")
            fiscal_val = clean_fiscal_month(company.get("fiscal_month") or "")
            founded_val = clean_founded_year(company.get("founded_year") or "")
            phone_source = company.get("phone_source", "") or "none"
            address_source = company.get("address_source", "") or "none"
            ai_used = int(company.get("ai_used") or 0)
            ai_model = company.get("ai_model", "") or ""
            src_phone = company.get("source_url_phone", "") or ""
            src_addr = company.get("source_url_address", "") or ""
            src_rep = company.get("source_url_rep", "") or ""
            verify_result: dict[str, Any] = {"phone_ok": False, "address_ok": False}
            verify_result_source = "none"
            force_review = False
            confidence = float(company.get("extract_confidence") or 0.0)
            address_ai_confidence: float | None = company.get("address_confidence")
            address_ai_evidence: str | None = company.get("address_evidence")
            rule_phone: str | None = None
            rule_address: str | None = None
            rule_rep: str | None = None

            def save_partial(reason: str) -> None:
                nonlocal timeout_saved
                if timeout_saved:
                    return
                try:
                    if not (company.get("error_code") or "").strip():
                        company["error_code"] = reason or "timeout"
                    if not deep_skip_reason and (reason or timeout_stage):
                        company["deep_skip_reason"] = f"{reason or 'timeout'}:{timeout_stage}".strip(":")
                    # 正規化（軽量）だけ実施して保存する
                    normalized_found_address = normalize_address(found_address) if found_address else ""
                    cleaned_rep = scraper.clean_rep_name(rep_name_val) or ""
                    company.update({
                        "homepage": homepage or "",
                        "phone": phone or "",
                        "found_address": normalized_found_address,
                        "rep_name": cleaned_rep,
                        "description": description_val or "",
                        "listing": listing_val or "",
                        "revenue": revenue_val or "",
                        "profit": profit_val or "",
                        "capital": capital_val or "",
                        "employees": employees_val or "",
                        "fiscal_month": fiscal_val or "",
                        "founded_year": founded_val or "",
                        "phone_source": phone_source or "",
                        "address_source": address_source or "",
                        "ai_used": int(ai_used or 0),
                        "ai_model": ai_model or "",
                        "extract_confidence": confidence,
                        "source_url_phone": src_phone or "",
                        "source_url_address": src_addr or "",
                        "source_url_rep": src_rep or "",
                        "homepage_official_flag": int(homepage_official_flag or 0),
                        "homepage_official_source": homepage_official_source or "",
                        "homepage_official_score": float(homepage_official_score or 0.0),
                        "address_confidence": address_ai_confidence,
                        "address_evidence": address_ai_evidence,
                        "deep_pages_visited": int(deep_pages_visited or 0),
                        "deep_fetch_count": int(deep_fetch_count or 0),
                        "deep_fetch_failures": int(deep_fetch_failures or 0),
                        "deep_skip_reason": company.get("deep_skip_reason", "") or deep_skip_reason or "",
                        "deep_urls_visited": json.dumps(list(deep_urls_visited or [])[:5], ensure_ascii=False),
                        "deep_phone_candidates": int(deep_phone_candidates or 0),
                        "deep_address_candidates": int(deep_address_candidates or 0),
                        "deep_rep_candidates": int(deep_rep_candidates or 0),
                        "timeout_stage": timeout_stage or "",
                    })
                    manager.save_company_data(company, status="review")
                    timeout_saved = True
                except Exception:
                    log.warning("[%s] timeout partial save failed", cid, exc_info=True)

            def save_no_homepage(reason: str) -> None:
                nonlocal timeout_saved
                if timeout_saved:
                    return
                try:
                    top3_urls = list(urls or [])[:3]
                    top3_records = sorted(candidate_records or [], key=lambda r: r.get("search_rank", 1e9))[:3]
                    all_directory_like = bool(top3_records) and all(
                        bool((r.get("rule") or {}).get("directory_like")) for r in top3_records
                    )
                    if not top3_urls:
                        skip_reason = "no_search_results_or_prefiltered"
                    elif all_directory_like:
                        skip_reason = "top3_all_directory_like"
                    else:
                        skip_reason = reason or "no_official_in_top3"
                    if not (company.get("error_code") or "").strip():
                        company["error_code"] = skip_reason
                    append_jsonl(
                        NO_OFFICIAL_LOG_PATH,
                        {
                            "id": cid,
                            "company_name": name,
                            "csv_address": addr,
                            "skip_reason": skip_reason,
                            "top3_urls": top3_urls,
                            "top3_candidates": [
                                {
                                    "url": (r.get("normalized_url") or r.get("url") or ""),
                                    "search_rank": int(r.get("search_rank") or 0),
                                    "domain_score": int(r.get("domain_score") or 0),
                                    "rule_score": float(((r.get("rule") or {}).get("score")) or 0.0),
                                    "directory_like": bool((r.get("rule") or {}).get("directory_like")),
                                    "directory_score": int((r.get("rule") or {}).get("directory_score") or 0),
                                    "directory_reasons": list((r.get("rule") or {}).get("directory_reasons") or [])[:8],
                                    "blocked_host": bool((r.get("rule") or {}).get("blocked_host")),
                                    "prefecture_mismatch": bool((r.get("rule") or {}).get("prefecture_mismatch")),
                                }
                                for r in top3_records
                            ],
                        },
                    )
                    normalized_found_address = normalize_address(found_address) if found_address else ""
                    cleaned_rep = scraper.clean_rep_name(rep_name_val) or ""
                    try:
                        _exclude = exclude_reasons  # type: ignore[name-defined]
                    except Exception:
                        _exclude = {}
                    try:
                        exclude_reasons_json = json.dumps(_exclude or {}, ensure_ascii=False)
                    except Exception:
                        exclude_reasons_json = "{}"
                    company.update(
                        {
                            "homepage": "",
                            "phone": phone or "",
                            "found_address": normalized_found_address,
                            "rep_name": cleaned_rep,
                            "description": description_val or "",
                            "listing": listing_val or "",
                            "revenue": revenue_val or "",
                            "profit": profit_val or "",
                            "capital": capital_val or "",
                            "fiscal_month": fiscal_val or "",
                            "founded_year": founded_val or "",
                            "phone_source": phone_source or "",
                            "address_source": address_source or "",
                            "ai_used": int(ai_used or 0),
                            "ai_model": ai_model or "",
                            "extract_confidence": confidence,
                            "source_url_phone": src_phone or "",
                            "source_url_address": src_addr or "",
                            "source_url_rep": src_rep or "",
                            "homepage_official_flag": 0,
                            "homepage_official_source": "",
                            "homepage_official_score": 0.0,
                            "address_confidence": address_ai_confidence,
                            "address_evidence": address_ai_evidence,
                            "deep_pages_visited": 0,
                            "deep_fetch_count": 0,
                            "deep_fetch_failures": 0,
                            "deep_skip_reason": f"{skip_reason}:no_official".strip(":"),
                            "deep_urls_visited": "[]",
                            "deep_phone_candidates": 0,
                            "deep_address_candidates": 0,
                            "deep_rep_candidates": 0,
                            "top3_urls": json.dumps(top3_urls, ensure_ascii=False),
                            "exclude_reasons": exclude_reasons_json,
                            "skip_reason": skip_reason,
                            "provisional_homepage": "",
                            "final_homepage": "",
                            "deep_enabled": 0,
                            "deep_stop_reason": "no_official",
                            "timeout_stage": timeout_stage or "",
                        }
                    )
                    manager.save_company_data(company, status="no_homepage")
                    timeout_saved = True
                except Exception:
                    log.warning("[%s] no_homepage save failed", cid, exc_info=True)

            fatal_error = False
            skip_company_reason = ""
            drop_reasons: dict[str, str] = {}
            drop_details_by_url: dict[str, dict[str, str]] = {}
            page_type_per_url: dict[str, str] = {}
            provisional_homepage = ""
            forced_provisional_homepage = ""
            forced_provisional_reason = ""
            provisional_info = None
            provisional_cands: dict[str, Any] = {}
            provisional_domain_score = 0
            provisional_address_ok = False
            provisional_evidence_score = 0
            provisional_profile_hit = False
            provisional_host_token = False
            provisional_name_present = False
            provisional_ai_hint = False
            try:
                try:
                    candidate_limit = SEARCH_CANDIDATE_LIMIT
                    company_tokens = scraper._company_tokens(name)  # type: ignore
                    try:
                        if SEARCH_PHASE_TIMEOUT_SEC > 0:
                            urls = await asyncio.wait_for(
                                scraper.search_company(name, addr, num_results=candidate_limit),
                                timeout=clamp_timeout(SEARCH_PHASE_TIMEOUT_SEC),
                            )
                        else:
                            ensure_global_time("search_company_start")
                            urls = await scraper.search_company(name, addr, num_results=candidate_limit)
                        ensure_global_time("search_company_end")
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
                            "rep_name": scraper.clean_rep_name(company.get("rep_name")) or "",
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
                            csv_writer.writerow(_csv_safe_row({k: company.get(k, "") for k in CSV_FIELDNAMES}))
                            csv_file.flush()
                        processed += 1
                        try:
                            await scraper.reset_context()
                        except Exception:
                            pass
                        if SLEEP_BETWEEN_SEC > 0:
                            await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))
                        continue
                    max_candidates = max(1, candidate_limit or 1)
                    if len(urls) > max_candidates:
                        log.info("[%s] limiting candidates to top %s (had %s)", cid, max_candidates, len(urls))
                        urls = urls[:max_candidates]
                    url_flags_map, host_flags_map = manager.get_url_flags_batch(urls)
                    exclude_reasons: dict[str, str] = {}
                    homepage = ""
                    info = None
                    primary_cands: dict[str, list[str]] = {}
                    fallback_cands: list[tuple[str, dict[str, list[str]]]] = []
                    homepage_official_flag = 0
                    homepage_official_source = ""
                    homepage_official_score = 0.0
                    ai_official_description: str | None = None
                    force_review = False
                    ai_time_spent = 0.0
                    chosen_domain_score = 0
                    search_phase_end = 0.0
                    official_phase_end = 0.0
                    deep_phase_end = 0.0
                    deep_pages_visited = 0
                    deep_fetch_count = 0
                    deep_fetch_failures = 0
                    deep_skip_reason = ""
                    deep_stop_reason = ""
                    deep_urls_visited: list[str] = []
                    deep_phone_candidates = 0
                    deep_address_candidates = 0
                    deep_rep_candidates = 0
                    # 「AI公式だが弱シグナルで公式確定できない」等のケースで、暫定URLとして保持するための退避先
                    forced_provisional_homepage = ""
                    forced_provisional_reason = ""

                    fetch_sem = asyncio.Semaphore(FETCH_CONCURRENCY)

                    async def fetch_candidate_page(candidate: str, allow_slow: bool) -> Dict[str, Any]:
                        try_timeout = PAGE_FETCH_TIMEOUT_SEC * (1.0 if not allow_slow else 1.4)
                        async with fetch_sem:
                            return await asyncio.wait_for(
                                scraper.get_page_info(candidate, allow_slow=allow_slow),
                                timeout=clamp_timeout(try_timeout),
                            )

                    async def prepare_candidate(idx: int, candidate: str):
                        # 取得前は軽い正規化のみ（canonical/og:url は HTML が必要）
                        normalized_candidate = scraper.normalize_homepage_url(candidate)
                        url_for_flag = normalized_candidate or candidate
                        normalized_flag_url, host_for_flag = manager._normalize_flag_target(url_for_flag)
                        flag_info = url_flags_map.get(normalized_flag_url)
                        if not flag_info and host_for_flag:
                            flag_info = host_flags_map.get(host_for_flag)
                        domain_score_for_flag = scraper._domain_score(company_tokens, url_for_flag)  # type: ignore
                        host_token_hit_for_flag = scraper._host_token_hit(company_tokens, url_for_flag)  # type: ignore
                        strong_domain_host_for_flag = bool(
                            host_token_hit_for_flag
                            and (
                                domain_score_for_flag >= 5
                                or (company_has_corp and domain_score_for_flag >= 4)
                            )
                        )
                        # fetch前のURL文字列だけで弾けるものは弾く（コスト最小化）
                        try:
                            dir_hint = scraper._detect_directory_like(url_for_flag, text="", html="")  # type: ignore[attr-defined]
                            if bool(dir_hint.get("is_directory_like")) and int(dir_hint.get("directory_score") or 0) >= 8 and domain_score_for_flag < 4:
                                exclude_reasons[candidate] = "prefilter_directory_like_url"
                                log.info("[%s] URLパターンで企業DB/ディレクトリ臭が強いのでfetch前に除外: %s", cid, candidate)
                                return None
                        except Exception:
                            pass
                        skip_by_flag = should_skip_by_url_flag(flag_info)
                        if skip_by_flag:
                            source = (flag_info.get("judge_source") or "").strip().lower() if isinstance(flag_info, dict) else ""
                            allow_tld = False
                            try:
                                host_tmp = (urlparse(url_for_flag).netloc or "").lower().split(":")[0]
                                if host_tmp.startswith("www."):
                                    host_tmp = host_tmp[4:]
                                allow_tld = any(host_tmp.endswith(suf) for suf in CompanyScraper.ALLOWED_OFFICIAL_TLDS)
                            except Exception:
                                allow_tld = False
                            # AIの「非公式」判定は誤爆があり得るため、強いドメインシグナルがある場合は再評価する
                            if source.startswith("ai") and (strong_domain_host_for_flag or (allow_tld and domain_score_for_flag >= 3 and not _is_free_host(url_for_flag))):
                                skip_by_flag = False

                        if skip_by_flag:
                            try:
                                exclude_reasons[candidate] = (
                                    f"url_flag:{(flag_info.get('judge_source') or '').strip()}:{(flag_info.get('reason') or '').strip()}"
                                ).strip(":")[:240]
                            except Exception:
                                exclude_reasons[candidate] = "url_flag"
                            log.info(
                                "[%s] 既知の非公式URLを除外: %s (domain_score=%s source=%s conf=%s reason=%s)",
                                cid,
                                candidate,
                                domain_score_for_flag,
                                flag_info.get("judge_source"),
                                flag_info.get("confidence"),
                                flag_info.get("reason"),
                            )
                            return None
                        candidate_info: Dict[str, Any] | None = None

                        async def _attempt_fetch(allow_slow: bool) -> Dict[str, Any] | None:
                            try:
                                info = await fetch_candidate_page(candidate, allow_slow=allow_slow)
                            except asyncio.TimeoutError:
                                log.info(
                                    "[%s] get_page_info timeout (allow_slow=%s) -> %s",
                                    cid,
                                    allow_slow,
                                    candidate,
                                )
                                try:
                                    http_info = await scraper._fetch_http_info(candidate)
                                    if http_info and (http_info.get("text") or http_info.get("html")):
                                        return {
                                            "url": candidate,
                                            "text": http_info.get("text", "") or "",
                                            "html": http_info.get("html", "") or "",
                                            "screenshot": b"",
                                        }
                                except Exception:
                                    pass
                                return None
                            except Exception:
                                log.warning("[%s] get_page_info failure -> %s", cid, candidate, exc_info=True)
                                return None
                            return info

                        for allow in (False, True):
                            info = await _attempt_fetch(allow)
                            if info:
                                candidate_info = info
                            if info and (info.get("text") or info.get("html")):
                                break
                        if not candidate_info:
                            return None
                        # HTML取得後に canonical/og:url を反映した正規化を再計算（追加fetchなし）
                        try:
                            normalized_candidate2 = scraper.normalize_homepage_url(candidate, candidate_info)
                        except Exception:
                            normalized_candidate2 = normalized_candidate
                        if normalized_candidate2:
                            url_for_flag = normalized_candidate2
                            normalized_flag_url, host_for_flag = manager._normalize_flag_target(url_for_flag)
                            # 旗の参照キーだけ更新（map自体は候補urlsで事前取得済みなので、無ければNoneのまま）
                            flag_info = url_flags_map.get(normalized_flag_url) or (host_flags_map.get(host_for_flag) if host_for_flag else None)
                            domain_score_for_flag = scraper._domain_score(company_tokens, url_for_flag)  # type: ignore
                        candidate_text = candidate_info.get("text", "") or ""
                        candidate_html = candidate_info.get("html") or ""
                        extracted = scraper.extract_candidates(candidate_text, candidate_html)
                        rule_details = scraper.is_likely_official_site(
                            name, candidate, candidate_info, addr, extracted, return_details=True
                        )
                        if not isinstance(rule_details, dict):
                            rule_details = {"is_official": bool(rule_details), "score": 0.0}
                        try:
                            log.info(
                                "[%s] official_candidate url=%s rule_score=%.1f evidence=%s directory=%s domain_score=%s name_ratio=%.2f exact=%s partial_only=%s pref_mismatch=%s addr_hit=%s pref_hit=%s zip_hit=%s",
                                cid,
                                candidate,
                                float(rule_details.get("score") or 0.0),
                                int(rule_details.get("official_evidence_score") or 0),
                                bool(rule_details.get("directory_like")),
                                domain_score_for_flag,
                                float(rule_details.get("name_match_ratio") or 0.0),
                                bool(rule_details.get("name_match_exact")),
                                bool(rule_details.get("name_match_partial_only")),
                                bool(rule_details.get("prefecture_mismatch")),
                                bool(rule_details.get("address_match")),
                                bool(rule_details.get("prefecture_match")),
                                bool(rule_details.get("postal_code_match")),
                            )
                        except Exception:
                            pass
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

                    async def _prepare_batch(
                        pairs: list[tuple[int, str]],
                        deadline: float | None,
                    ) -> tuple[list[dict[str, Any]], bool]:
                        prepared: list[Any] = []
                        prepare_timed_out = False
                        if not pairs:
                            return [], False
                        prepare_tasks = [
                            asyncio.create_task(prepare_candidate(idx, candidate))
                            for idx, candidate in pairs
                        ]
                        pending: set[asyncio.Task] = set(prepare_tasks)
                        try:
                            while pending:
                                if deadline is not None:
                                    remaining = deadline - time.monotonic()
                                    if remaining <= 0:
                                        prepare_timed_out = True
                                        break
                                else:
                                    remaining = None
                                done, pending = await asyncio.wait(
                                    pending,
                                    timeout=remaining,
                                    return_when=asyncio.FIRST_COMPLETED,
                                )
                                if not done:
                                    prepare_timed_out = True
                                    break
                                for task in done:
                                    try:
                                        result = task.result()
                                    except Exception:
                                        continue
                                    prepared.append(result)
                                if len(prepared) >= len(pairs):
                                    pending.clear()
                                    break
                        finally:
                            if pending:
                                for task in pending:
                                    task.cancel()
                                await asyncio.gather(*pending, return_exceptions=True)
                                prepare_timed_out = True
                        ordered: list[tuple[int, dict[str, Any]]] = []
                        for result in prepared:
                            if isinstance(result, Exception) or not result:
                                continue
                            ordered.append(result)
                        ordered.sort(key=lambda x: x[0])
                        records = [record for _, record in ordered]
                        if prepare_timed_out and records:
                            log.info(
                                "[%s] prepare_candidate partial timeout (recorded %d/%d)",
                                cid,
                                len(records),
                                len(prepared),
                            )
                        return records, prepare_timed_out

                    def _postprocess_candidates(records: list[dict[str, Any]]) -> None:
                        for record in records:
                            normalized_url = record.get("normalized_url") or record.get("url") or ""
                            domain_score_val = scraper._domain_score(company_tokens, normalized_url)  # type: ignore
                            host_token_hit = scraper._host_token_hit(company_tokens, normalized_url)  # type: ignore
                            record["domain_score"] = domain_score_val
                            record["host_token_hit"] = host_token_hit
                            # 公式候補の強さを上げる: 社名トークンがホストに入りドメインスコアが高ければ AI 否定を上書きできるようにする
                            record["strong_domain_host"] = (domain_score_val >= 5 or (company_has_corp and domain_score_val >= 4)) and host_token_hit
                            record.setdefault("order_idx", 0)
                            record.setdefault("search_rank", 0)

                    candidate_records: list[dict[str, Any]] = []
                    prepare_timed_out = False
                    url_pairs = list(enumerate(urls))
                    first_pairs = url_pairs[:3]
                    remaining_pairs = url_pairs[3:]
                    search_deadline = time.monotonic() + SEARCH_PHASE_TIMEOUT_SEC if SEARCH_PHASE_TIMEOUT_SEC > 0 else None
                    initial_records, timed_out = await _prepare_batch(first_pairs, search_deadline)
                    candidate_records.extend(initial_records)
                    prepare_timed_out = prepare_timed_out or timed_out
                    search_phase_end = elapsed()

                    if prepare_timed_out and not candidate_records:
                        log.info("[%s] prepare_candidate timeout -> review/search_timeout", cid)

                    if candidate_records:
                        _postprocess_candidates(candidate_records)
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
                        if over_hard_deadline():
                            raise_hard_timeout("candidate_phase")
                    if homepage and over_after_official():
                        timed_out = True

                    if not candidate_records:
                        company.update({
                            "homepage": "",
                            "phone": "",
                            "found_address": "",
                            "rep_name": scraper.clean_rep_name(company.get("rep_name")) or "",
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
                            csv_writer.writerow(_csv_safe_row({k: company.get(k, "") for k in CSV_FIELDNAMES}))
                            csv_file.flush()
                        processed += 1
                        if SLEEP_BETWEEN_SEC > 0:
                            await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))
                        continue

                    ensure_global_time("after_candidate_records")
                    ai_official_attempted = False
                    selected_candidate_record: dict[str, Any] | None = None
                    ai_official_rejected_record: dict[str, Any] | None = None
                    ai_official_rejected_conf: float = 0.0
                    ai_official_rejected_reason: str = ""
                    ai_official_enabled = bool(
                        USE_AI_OFFICIAL and USE_AI and verifier is not None and hasattr(verifier, "judge_official_homepage")
                    )
                    ai_official_primary = bool(AI_OFFICIAL_PRIMARY and ai_official_enabled)
                    processed_urls: set[str] = set()
                    fetched_remaining = False
                    while True:
                        if ai_official_enabled:
                            ai_tasks: list[asyncio.Task] = []
                            ai_sem = asyncio.Semaphore(AI_OFFICIAL_CONCURRENCY)

                            async def run_official_ai(record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
                                nonlocal ai_official_attempted, ai_time_spent
                                record["ai_checked"] = True
                                async with ai_sem:
                                    remaining = remaining_time_budget()
                                    if remaining <= AI_MIN_REMAINING_SEC:
                                        log.info(
                                            "[%s] skip AI公式判定（残り%.1fs）: %s",
                                            cid,
                                            max(0.0, remaining),
                                            record.get("url"),
                                        )
                                        return record, None
                                    normalized_for_ai = record.get("normalized_url") or record.get("url") or ""
                                    domain_score = int(record.get("domain_score") or 0)
                                    if normalized_for_ai and domain_score == 0:
                                        domain_score = scraper._domain_score(company_tokens, normalized_for_ai)  # type: ignore
                                        record["domain_score"] = domain_score
                                    info_payload = record.get("info") or {}
                                    rule_for_ai = record.get("rule") or {}
                                    allow_slow_ai = False
                                    if remaining > (AI_MIN_REMAINING_SEC + 2.0):
                                        evidence_score = int(rule_for_ai.get("official_evidence_score") or 0)
                                        allow_slow_ai = bool(
                                            rule_for_ai.get("is_official")
                                            or evidence_score >= 9
                                            or record.get("host_token_hit")
                                        )
                                    info_payload = await ensure_info_text(
                                        scraper,
                                        record.get("url") or "",
                                        info_payload,
                                        allow_slow=allow_slow_ai,
                                    )
                                    if OFFICIAL_AI_USE_SCREENSHOT:
                                        # 公式判定はトップ候補3件をスクショ付きで評価する（要求仕様）
                                        info_payload = await ensure_info_has_screenshot(
                                            scraper,
                                            record.get("url") or "",
                                            info_payload,
                                            need_screenshot=True,
                                            policy=OFFICIAL_AI_SCREENSHOT_POLICY,
                                        )
                                    record["info"] = info_payload
                                    remaining = remaining_time_budget()
                                    if remaining <= AI_MIN_REMAINING_SEC:
                                        log.info(
                                            "[%s] skip AI公式判定（残り%.1fs, info取得後）: %s",
                                            cid,
                                            max(0.0, remaining),
                                            record.get("url"),
                                        )
                                        return record, None
                                    pages_for_ai: list[dict[str, Any]] = []
                                    base_url = normalized_for_ai or record.get("url") or ""
                                    base_text = info_payload.get("text", "") or ""
                                    base_html = info_payload.get("html", "") or ""
                                    try:
                                        base_pt = scraper.classify_page_type(
                                            base_url, text=base_text, html=base_html
                                        ).get("page_type") or "OTHER"
                                    except Exception:
                                        base_pt = "OTHER"
                                    evidence_list = rule_for_ai.get("official_evidence") or []
                                    evidence_set = {str(x).strip() for x in evidence_list if str(x).strip()}
                                    title_match = None
                                    if "title" in evidence_set:
                                        title_match = "strong"
                                    elif "title_partial" in evidence_set:
                                        title_match = "partial"
                                    h1_match = None
                                    if "h1" in evidence_set:
                                        h1_match = "strong"
                                    elif "h1_partial" in evidence_set:
                                        h1_match = "partial"
                                    try:
                                        parsed_for_sig = urlparse(base_url or record.get("url") or "")
                                        host_for_sig = (parsed_for_sig.netloc or "").lower().split(":")[0]
                                    except Exception:
                                        host_for_sig = ""
                                    tld_for_sig = ""
                                    if host_for_sig:
                                        parts = host_for_sig.split(".")
                                        if len(parts) >= 2:
                                            tld_for_sig = "." + ".".join(parts[-2:])
                                    signals = {
                                        "page_type": base_pt,
                                        "host": host_for_sig or None,
                                        "tld": tld_for_sig or None,
                                        "allowed_tld": True if (tld_for_sig and tld_for_sig in CompanyScraper.ALLOWED_OFFICIAL_TLDS) else None,
                                        "domain_score": int(record.get("domain_score") or 0),
                                        "host_token_hit": bool(record.get("host_token_hit")),
                                        "strong_domain_host": bool(record.get("strong_domain_host")),
                                        "name_match_ratio": rule_for_ai.get("name_match_ratio"),
                                        "name_match_exact": rule_for_ai.get("name_match_exact"),
                                        "name_match_partial_only": rule_for_ai.get("name_match_partial_only"),
                                        "name_match_source": rule_for_ai.get("name_match_source"),
                                        "official_evidence_score": rule_for_ai.get("official_evidence_score"),
                                        "official_evidence": evidence_list if evidence_set else None,
                                        "title_match": title_match,
                                        "h1_match": h1_match,
                                        "og_site_name_match": True if "og:site_name" in evidence_set else None,
                                        "directory_like": rule_for_ai.get("directory_like"),
                                    }
                                    base_snippet_full = build_official_ai_text(base_text, base_html, signals=signals)
                                    base_snippet = base_snippet_full[:1800] if len(base_snippet_full) > 1800 else base_snippet_full
                                    pages_for_ai.append(
                                        {
                                            "url": base_url,
                                            "page_type": base_pt,
                                            "snippet": base_snippet,
                                            "screenshot": info_payload.get("screenshot"),
                                        }
                                    )
                                    priority_docs: dict[str, dict[str, Any]] = {}
                                    if remaining_time_budget() > AI_MIN_REMAINING_SEC:
                                        try:
                                            priority_docs = await scraper.fetch_priority_documents(
                                                base_url or record.get("url"),
                                                base_html,
                                                max_links=2,
                                                concurrency=FETCH_CONCURRENCY,
                                                # 会社概要系を優先（contactはdescription材料としてノイズになりやすいため優先しない）
                                                target_types=["about", "finance"],
                                                allow_slow=allow_slow_ai,
                                                exclude_urls={base_url} if base_url else None,
                                            )
                                        except Exception:
                                            priority_docs = {}
                                    for url, pdata in priority_docs.items():
                                        page_info = {
                                            "url": url,
                                            "text": pdata.get("text", "") or "",
                                            "html": pdata.get("html", "") or "",
                                        }
                                        ptext = page_info.get("text", "") or ""
                                        phtml = page_info.get("html", "") or ""
                                        try:
                                            pt = scraper.classify_page_type(url, text=ptext, html=phtml).get("page_type") or "OTHER"
                                        except Exception:
                                            pt = "OTHER"
                                        snippet_full = build_official_ai_text(ptext, phtml)
                                        snippet = snippet_full[:1800] if len(snippet_full) > 1800 else snippet_full
                                        pages_for_ai.append(
                                            {
                                                "url": url,
                                                "page_type": pt,
                                                "snippet": snippet,
                                                "screenshot": None,
                                            }
                                        )
                                    pages_for_ai = [p for p in pages_for_ai if p.get("snippet")] or pages_for_ai
                                    if len(pages_for_ai) > 3:
                                        pages_for_ai = pages_for_ai[:3]
                                    # deep起点/ページタイプ補正に使うため、AIに渡したページ情報（軽量）を保存
                                    try:
                                        record["_ai_pages_meta"] = [
                                            {"url": p.get("url"), "page_type": p.get("page_type")}
                                            for p in (pages_for_ai or [])
                                            if p.get("url")
                                        ]
                                    except Exception:
                                        record["_ai_pages_meta"] = []
                                    ai_started = time.monotonic()
                                    try:
                                        if remaining_time_budget() <= AI_MIN_REMAINING_SEC:
                                            return record, None
                                        if hasattr(verifier, "judge_official_homepage_multi") and pages_for_ai:
                                            ai_verdict = await asyncio.wait_for(
                                                verifier.judge_official_homepage_multi(
                                                    pages_for_ai,
                                                    name,
                                                    addr,
                                                    base_url or record.get("url"),
                                                ),
                                                timeout=clamp_timeout(max(ai_call_timeout, 5.0)),
                                            )
                                        else:
                                            ai_verdict = await asyncio.wait_for(
                                                verifier.judge_official_homepage(
                                                    base_snippet_full,
                                                    info_payload.get("screenshot"),
                                                    name,
                                                    addr,
                                                    record.get("normalized_url") or record.get("url"),
                                                ),
                                                timeout=clamp_timeout(max(ai_call_timeout, 5.0)),
                                            )
                                    except Exception:
                                        log.warning("[%s] AI公式判定失敗: %s", cid, record.get("url"), exc_info=True)
                                        return record, None
                                    ai_time_spent += time.monotonic() - ai_started
                                    ai_official_attempted = True
                                    # AI公式判定を呼んだ事実は保存しておく（descriptionの由来追跡などに使う）
                                    try:
                                        nonlocal ai_used, ai_model
                                        ai_used = 1
                                        if not ai_model:
                                            ai_model = AI_MODEL_NAME or ""
                                    except Exception:
                                        pass
                                    # AI公式のうち、ドメイン/ルール根拠が弱いものは「確定公式には使わない」が、
                                    # deep起点としては保持する（除外扱いに落とさない）。
                                    if ai_verdict and ai_verdict.get("is_official") and domain_score < 4 and not record.get("rule", {}).get("address_match"):
                                        rule = record.get("rule", {}) or {}
                                        evidence_score = int(rule.get("official_evidence_score") or 0)
                                        directory_like = bool(rule.get("directory_like"))
                                        if directory_like or evidence_score < 9:
                                            ai_verdict["weak_signals"] = True
                                    return record, ai_verdict

                            def _official_ai_rank_key(r: dict[str, Any]) -> tuple:
                                rd = r.get("rule") or {}
                                directory_like = bool(rd.get("directory_like"))
                                evidence = int(rd.get("official_evidence_score") or 0)
                                domain = int(r.get("domain_score") or 0)
                                strong_domain_host = bool(r.get("strong_domain_host"))
                                host_token_hit = bool(r.get("host_token_hit"))
                                return (
                                    directory_like,  # ディレクトリ系は後ろ
                                    not strong_domain_host,
                                    not host_token_hit,
                                    -domain,
                                    -evidence,
                                    int(r.get("search_rank", 1_000_000_000) or 1_000_000_000),
                                    int(r.get("order_idx", 1_000_000_000) or 1_000_000_000),
                                )

                            ranked_for_ai_official = sorted(candidate_records, key=_official_ai_rank_key)
                            if not AI_OFFICIAL_ALL_CANDIDATES:
                                ranked_for_ai_official = ranked_for_ai_official[:3]
                            elif AI_OFFICIAL_CANDIDATE_LIMIT > 0:
                                ranked_for_ai_official = ranked_for_ai_official[:AI_OFFICIAL_CANDIDATE_LIMIT]
                            if ai_official_primary:
                                for record in ranked_for_ai_official:
                                    if record.get("ai_judge") or record.get("ai_checked"):
                                        continue
                                    if bool((record.get("rule") or {}).get("directory_like")):
                                        continue
                                    if remaining_time_budget() <= AI_MIN_REMAINING_SEC:
                                        break
                                    rec, verdict = await run_official_ai(record)
                                    if verdict:
                                        rec["ai_judge"] = verdict
                                        if ai_official_hint_from_judge(verdict, AI_VERIFY_MIN_CONFIDENCE):
                                            log.info("[%s] AI公式判定で早期確定候補を取得 -> 以降のAI判定を省略", cid)
                                            break
                            else:
                                for record in ranked_for_ai_official:
                                    if record.get("ai_judge") or record.get("ai_checked"):
                                        continue
                                    if bool((record.get("rule") or {}).get("directory_like")):
                                        continue
                                    if remaining_time_budget() <= AI_MIN_REMAINING_SEC:
                                        break
                                    ai_tasks.append(asyncio.create_task(run_official_ai(record)))
                                if ai_tasks:
                                    results = await asyncio.gather(*ai_tasks, return_exceptions=True)
                                    for res in results:
                                        if not isinstance(res, tuple) or len(res) != 2:
                                            continue
                                        rec, verdict = res
                                        if verdict:
                                            rec["ai_judge"] = verdict

                            # AI公式が複数出るケースに備え、AI判定が強い候補を先に評価する
                            def _ai_select_score(rec: dict[str, Any]) -> float:
                                aj = rec.get("ai_judge") or {}
                                if not isinstance(aj, dict):
                                    aj = {}
                                ai_is = aj.get("is_official_site")
                                if ai_is is None:
                                    ai_is = aj.get("is_official")
                                conf = aj.get("official_confidence")
                                if conf is None:
                                    conf = aj.get("confidence")
                                try:
                                    conf_f = float(conf) if conf is not None else 0.0
                                except Exception:
                                    conf_f = 0.0
                                rd = rec.get("rule") or {}
                                evidence = float(rd.get("official_evidence_score") or 0.0)
                                domain = float(rec.get("domain_score") or 0.0)
                                directory_like = bool(rd.get("directory_like"))
                                host_token_hit = bool(rec.get("host_token_hit"))
                                strong_domain_host = bool(rec.get("strong_domain_host"))
                                name_present = bool(rd.get("name_present"))
                                # 公式と判定されたものを優先しつつ、低confは過信しない
                                score = 0.0
                                if ai_is is True and conf_f >= AI_VERIFY_MIN_CONFIDENCE:
                                    score += 1000.0 + conf_f * 100.0
                                elif ai_is is True:
                                    score += conf_f * 10.0
                                elif ai_is is False:
                                    score -= 200.0
                                score += domain * 5.0 + evidence * 2.0
                                score += 20.0 if host_token_hit else 0.0
                                score += 12.0 if strong_domain_host else 0.0
                                score += 10.0 if name_present else 0.0
                                score -= 500.0 if directory_like else 0.0
                                return score

                            candidate_records.sort(
                                key=lambda r: (
                                    -_ai_select_score(r),
                                    int(r.get("search_rank", 1_000_000_000) or 1_000_000_000),
                                    int(r.get("order_idx", 1_000_000_000) or 1_000_000_000),
                                )
                            )

                        for record in candidate_records:
                            normalized_url = record.get("normalized_url") or record.get("url")
                            if normalized_url and normalized_url in processed_urls:
                                continue
                            if normalized_url:
                                processed_urls.add(normalized_url)
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
                            # ドメイン一致だけで「社名一致」と扱うと誤採用しやすいので、ページ内の社名シグナルを重視する
                            name_present = bool(rule_details.get("name_present"))
                            name_match_exact = bool(rule_details.get("name_match_exact"))
                            name_match_partial_only = bool(rule_details.get("name_match_partial_only"))
                            try:
                                name_match_ratio = float(rule_details.get("name_match_ratio") or 0.0)
                            except Exception:
                                name_match_ratio = 0.0
                            name_match_source = str(rule_details.get("name_match_source") or "")
                            high_signal_sources = {"title", "h1", "og_site_name", "og_title", "app_name"}
                            official_evidence = rule_details.get("official_evidence") or []
                            strong_name_hit = bool(
                                name_match_exact
                                or ("jsonld:org_name" in official_evidence)
                                or ("h1" in official_evidence)
                                or ("title" in official_evidence)
                                or (
                                    name_match_ratio >= 0.92
                                    and not name_match_partial_only
                                    and name_match_source in high_signal_sources
                                )
                            )
                            name_hit = strong_name_hit
                            evidence_score = int(rule_details.get("official_evidence_score") or 0)
                            directory_like = bool(rule_details.get("directory_like"))
                            directory_score = int(rule_details.get("directory_score") or 0)
                            host_name, _, allowed_tld, whitelist_host, _ = CompanyScraper._allowed_official_host(normalized_url or "")
                            addr_weak = bool((not addr) or input_addr_pref_only)
                            pref_only_ok = bool(
                                input_addr_pref_only
                                and pref_hit
                                and (name_hit or strong_domain_host or host_token_hit or domain_score >= 4)
                            )
                            address_ok = bool(addr) and (addr_hit or zip_hit or pref_only_ok)
                            # 住所入力が弱い/空のケースは address_ok に寄らず、サイト側シグナル（title/h1/jsonld等）を強く評価する。
                            # 追加fetch/追加AIは増やさず、採用条件のみを緩和する（誤採用防止のため directory_like は除外）。
                            weak_addr_signal_ok = bool(
                                addr_weak
                                and (strong_name_hit or name_present)
                                and evidence_score >= 10
                                and (strong_domain_host or host_token_hit or domain_score >= 4 or allowed_tld or whitelist_host)
                                and not directory_like
                            )
                            content_strong = name_hit and (address_ok or weak_addr_signal_ok or evidence_score >= 9)
                            ai_judge = record.get("ai_judge")
                            flag_info = record.get("flag_info")


                            ai_is_official = None
                            ai_is_official_effective = None
                            ai_conf_f = 0.0
                            ai_official_hint = False

                            if ai_judge:
                                ai_is_official = ai_judge.get("is_official_site")
                                if ai_is_official is None:
                                    ai_is_official = ai_judge.get("is_official")
                                ai_conf = ai_judge.get("official_confidence")
                                if ai_conf is None:
                                    ai_conf = ai_judge.get("confidence")
                                try:
                                    ai_conf_f = float(ai_conf) if ai_conf is not None else 0.0
                                except Exception:
                                    ai_conf_f = 0.0
                                ai_official_hint = ai_official_hint_from_judge(ai_judge, AI_VERIFY_MIN_CONFIDENCE)
                                if ai_is_official is True:
                                    ai_is_official_effective = True
                                elif ai_is_official is False and ai_conf_f >= AI_VERIFY_MIN_CONFIDENCE:
                                    ai_is_official_effective = False
                                # AI negative with high confidence can exclude
                                if ai_is_official_effective is False:
                                    name_signal_ok = bool(
                                        strong_name_hit
                                        or (name_match_ratio >= 0.85 and not name_match_partial_only)
                                    )
                                    rule_strong_for_conflict = bool(
                                        content_strong
                                        or host_token_hit
                                        or strong_domain_host
                                        or rule_details.get("strong_domain")
                                        or domain_score >= 5
                                        or evidence_score >= 10
                                        or (name_signal_ok and evidence_score >= 3)
                                    )
                                    if not rule_strong_for_conflict:
                                        manager.upsert_url_flag(
                                            normalized_url,
                                            is_official=False,
                                            source="ai",
                                            reason=ai_judge.get("reason", "") or "ai_not_official",
                                            confidence=ai_conf_f,
                                        )
                                        fallback_cands.append((record.get("url"), extracted))
                                        log.info(
                                            "[%s] AI reject: %s (is_official=%s confidence=%.2f)",
                                            cid,
                                            record.get("url"),
                                            ai_is_official,
                                            ai_conf_f,
                                        )
                                        continue
                                    # AI negative conflict: keep as review-only
                                    record["ai_conflict"] = True
                                    record["ai_conflict_confidence"] = ai_conf_f
                                    force_review = True
                                    manager.upsert_url_flag(
                                        normalized_url,
                                        is_official=False,
                                        source="ai_conflict",
                                        reason=ai_judge.get("reason", "") or "ai_not_official_rule_conflict",
                                        confidence=ai_conf_f,
                                    )
                                    fallback_cands.append((record.get("url"), extracted))
                                    log.info(
                                        "[%s] AI negative conflict (review): %s (domain=%s evidence=%s conf=%.2f)",
                                        cid,
                                        record.get("url"),
                                        domain_score,
                                        evidence_score,
                                        ai_conf_f,
                                    )
                                    # 公式判定AIが誤って非公式に振れるケースがあるため、
                                    # conflict時は「AIだけで除外」せず、下段のルール評価にも回す。
                                elif ai_is_official is not None and ai_conf_f < AI_VERIFY_MIN_CONFIDENCE:
                                    # Low confidence: keep for rule evaluation
                                    record["ai_low_confidence"] = True

                            # directory_like は原則非公式（企業DB/まとめ）。強いシグナルはAIの誤爆でも採用しない。
                            # domain_scoreが極端に強い場合は例外を残すが、基本はハードに落とす。
                            if directory_like and directory_score >= DIRECTORY_HARD_REJECT_SCORE and domain_score < 4 and not (host_token_hit or strong_domain_host):
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="directory_like_hard",
                                    confidence=directory_score,
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] directory_like(hard) -> skip: %s", cid, record.get("url"))
                                continue

                            if directory_like and not ai_is_official_effective:
                                manager.upsert_url_flag(
                                    normalized_url,
                                    is_official=False,
                                    source="rule",
                                    reason="directory_like",
                                    confidence=directory_score,
                                )
                                fallback_cands.append((record.get("url"), extracted))
                                log.info("[%s] directory_like -> skip: %s", cid, record.get("url"))
                                continue

                            fast_phone_hit = bool(extracted.get("phone_numbers"))
                            fast_address_ok = address_ok or bool(rule_details.get("address_match"))
                            fast_domain_ok = (
                                domain_score >= 4
                                or host_token_hit
                                or strong_domain_host
                                or rule_details.get("strong_domain")
                            )
                            if ai_is_official_effective is True:
                                # AIの"official"は誤爆もあるため、弱い根拠だけで早期確定しない。
                                # （総処理時間の上限は変えず、候補の追加fetchも増やさずに、採用条件のみ厳格化する）
                                news_like = _is_news_like_url(normalized_url)
                                disallowed_official_host = CompanyScraper.is_disallowed_official_host(normalized_url or "")
                                ai_strong_accept = bool(
                                    (not directory_like)
                                    and (not disallowed_official_host)
                                    and fast_domain_ok
                                    and (
                                        name_hit
                                        or (address_ok and name_present and not news_like)
                                        or evidence_score >= 10
                                        or ("jsonld:org_name" in official_evidence)
                                    )
                                )
                                if not ai_strong_accept:
                                    force_review = True
                                    # 「AIは公式寄りだが根拠が弱い」ケースは、公式確定はせず review に落としつつ、
                                    # 後段の保存モデル(v2)で追跡できるよう provisional として退避しておく。
                                    if not forced_provisional_homepage and normalized_url:
                                        forced_provisional_homepage = normalized_url
                                        forced_provisional_reason = "ai_official_weak"
                                        if not (company.get("provisional_reason") or "").strip():
                                            company["provisional_reason"] = "ai_official_weak"
                                    log.info(
                                        "[%s] AI official (weak) -> review-only: %s (domain=%s host=%s name=%s addr=%s evidence=%s disallowed_host=%s conf=%.2f)",
                                        cid,
                                        normalized_url,
                                        domain_score,
                                        host_token_hit,
                                        name_hit,
                                        fast_address_ok,
                                        evidence_score,
                                        disallowed_official_host,
                                        ai_conf_f,
                                    )
                                    # ルール評価に回して、より強い候補があればそちらを優先する
                                else:
                                    candidate_url = normalized_url
                                    candidate_source = "ai_fast" if fast_domain_ok else "ai_review"
                                    candidate_score = float(rule_details.get("score") or 0.0)
                                    info = record.get("info")
                                    primary_cands = extracted
                                    homepage = candidate_url
                                    homepage_official_flag = 1
                                    homepage_official_source = candidate_source
                                    homepage_official_score = candidate_score
                                    ai_official_description = ai_judge.get("description") if isinstance(ai_judge, dict) else None
                                    chosen_domain_score = domain_score
                                    selected_candidate_record = record
                                    if not fast_domain_ok or (addr and not address_ok):
                                        force_review = True
                                    manager.upsert_url_flag(
                                        candidate_url,
                                        is_official=True,
                                        source=homepage_official_source,
                                        reason=ai_judge.get("reason", "") if isinstance(ai_judge, dict) else "",
                                        confidence=ai_judge.get("confidence") if isinstance(ai_judge, dict) else None,
                                    )
                                    log.info(
                                        "[%s] AI official selected: %s (source=%s domain=%s host=%s addr=%s phone=%s review=%s)",
                                        cid,
                                        candidate_url,
                                        homepage_official_source,
                                        domain_score,
                                        host_token_hit,
                                        fast_address_ok,
                                        fast_phone_hit,
                                        force_review,
                                    )
                                    break

                            brand_allowed = (allowed_tld or whitelist_host) and not host_token_hit
                            if (
                                not homepage
                                and brand_allowed
                                and content_strong
                                and not flag_info
                            ):
                                homepage = normalized_url
                                info = record.get("info")
                                primary_cands = extracted
                                # ブランド/親会社ドメイン（hostに社名が入らない）でも、内容が強い場合は公式寄りに扱う。
                                # ただし AIが高confidenceで否定している/AI conflict の場合は公式確定せず review に留める。
                                promote_to_official = bool(
                                    brand_allowed
                                    and content_strong
                                    and not _is_free_host(normalized_url)
                                    and not directory_like
                                    and not record.get("ai_conflict")
                                    and ai_is_official_effective is not False
                                    and not CompanyScraper.is_disallowed_official_host(normalized_url or "")
                                    and (
                                        evidence_score >= 10
                                        or (address_ok and not input_addr_pref_only)
                                        or weak_addr_signal_ok
                                        or ("jsonld:org_name" in official_evidence)
                                        or ("title" in official_evidence)
                                        or ("h1" in official_evidence)
                                    )
                                )
                                # AI公式判定が使える場合でも、強シグナルなら「公式寄り」に倒す（誤除外を減らす）
                                homepage_official_flag = 1 if promote_to_official else 0
                                homepage_official_source = "name_addr_strong" if promote_to_official else "name_addr"
                                homepage_official_score = float(rule_details.get("score") or 0.0)
                                chosen_domain_score = domain_score
                                selected_candidate_record = record
                                force_review = True
                                if record.get("ai_conflict"):
                                    homepage_official_source = "name_addr_ai_conflict"
                                if not ai_official_primary:
                                    manager.upsert_url_flag(
                                        normalized_url,
                                        is_official=True,
                                        source="name_addr",
                                        reason="name_address_match_brand_host",
                                        confidence=rule_details.get("score"),
                                    )
                                log.info(
                                    "[%s] 名前+住所一致（ブランドドメイン）でreview候補保存: %s host=%s",
                                    cid,
                                    normalized_url,
                                    host_name,
                                )
                                break

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
                            # キャッシュ公式は採用しない（参考のみ）
                            if rule_details.get("is_official"):
                                if input_addr_pref_only and not (host_token_hit or name_hit or strong_domain_host or domain_score >= 4 or weak_addr_signal_ok):
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
                                    # 住所根拠なしなら review 送りでURLは維持
                                    force_review = True
                                    log.info("[%s] 公式判定だが入力住所と一致せず -> review: %s", cid, record.get("url"))
                                if not host_token_hit and not (address_ok or weak_addr_signal_ok):
                                    manager.upsert_url_flag(
                                        normalized_url,
                                        is_official=False,
                                        source="rule",
                                        reason="host_no_name_no_address",
                                    )
                                    fallback_cands.append((record.get("url"), extracted))
                                    log.info("[%s] 公式判定でもホストに社名なし・住所根拠なしのため除外: %s", cid, record.get("url"))
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
                                if not (name_or_domain_ok or address_ok or weak_addr_signal_ok):
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
                                if domain_score < 3 and not strong_domain_host and not rule_details.get("strong_domain") and not (address_ok or weak_addr_signal_ok) and not name_hit and not host_token_hit:
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
                                selected_candidate_record = record
                                # AI公式判定が使える場合 rule だけで公式確定しない（AI公式の方で採用される）
                                homepage_official_flag = 0 if ai_official_primary else 1
                                homepage_official_source = "rule" if not ai_official_primary else "rule_review"
                                homepage_official_score = float(rule_details.get("score") or 0.0)
                                chosen_domain_score = domain_score
                                if ai_official_primary:
                                    force_review = True
                                # disallowed host は公式にしない（候補としては保持）
                                if homepage and int(homepage_official_flag or 0) == 1 and CompanyScraper.is_disallowed_official_host(homepage):
                                    homepage_official_flag = 0
                                    homepage_official_source = "rule_review_disallowed_host"
                                    force_review = True
                                if record.get("ai_conflict"):
                                    homepage_official_source = "rule_ai_conflict" if not ai_official_primary else "rule_review_ai_conflict"
                                    force_review = True
                                free_host = _is_free_host(homepage)
                                if (free_host and evidence_score < 10) or not _official_signal_ok(
                                    host_token_hit=host_token_hit,
                                    strong_domain_host=strong_domain_host,
                                    domain_score=domain_score,
                                    name_hit=name_hit,
                                    address_ok=address_ok,
                                    official_evidence_score=evidence_score,
                                ):
                                    homepage_official_flag = 0
                                    homepage_official_source = "provisional_freehost"
                                    force_review = True
                                    if normalized_url:
                                        forced_provisional_homepage = normalized_url
                                        forced_provisional_reason = "free_host_or_weak_signals"
                                    manager.upsert_url_flag(
                                        normalized_url,
                                        is_official=False,
                                        source="rule",
                                        reason="free_host_or_weak_signals",
                                    )
                                if not ai_official_primary:
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
                            log.info(
                                "[%s] 候補未採用: %s (domain=%s host_token=%s name_hit=%s addr_ok=%s evidence=%s dir=%s ai=%s conf=%.2f)",
                                cid,
                                record.get("url"),
                                domain_score,
                                host_token_hit,
                                name_hit,
                                address_ok,
                                evidence_score,
                                directory_like,
                                ai_is_official,
                                ai_conf_f,
                            )

                        if homepage:
                            break
                        if fetched_remaining or not remaining_pairs:
                            break
                        if prepare_timed_out or over_fetch_limit() or over_time_limit():
                            break
                        fetched_remaining = True
                        more_records, more_timed_out = await _prepare_batch(remaining_pairs, search_deadline)
                        remaining_pairs = []
                        if more_records:
                            _postprocess_candidates(more_records)
                            candidate_records.extend(more_records)
                            candidate_records.sort(
                                key=lambda rec: (
                                    not rec.get('strong_domain_host', False),
                                    -int(rec.get('domain_score') or 0),
                                    rec.get('order_idx', 0),
                                )
                            )
                            top3_ranked = sorted(candidate_records, key=lambda r: r.get('search_rank', 1e9))[:3]
                            for r in top3_ranked:
                                r['force_ai_official'] = True
                        prepare_timed_out = prepare_timed_out or more_timed_out
                        search_phase_end = elapsed()
                        continue
                    provisional_homepage = ""
                    provisional_info = None
                    provisional_cands: dict[str, list[str]] = {}
                    provisional_domain_score = 0
                    provisional_host_token = False
                    provisional_name_present = False
                    provisional_address_ok = False
                    provisional_ai_hint = False
                    provisional_profile_hit = False
                    provisional_evidence_score = 0
                    best_record: dict[str, Any] | None = None
                    if not homepage and candidate_records:
                        best_score = float("-inf")
                        for record in candidate_records:
                            normalized_url = record.get("normalized_url") or record.get("url")
                            rule_details = record.get("rule") or {}
                            if rule_details.get("directory_like"):
                                continue
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
                            pref_only_ok = bool(
                                input_addr_pref_only
                                and pref_hit
                                and (name_present or strong_domain_host or host_token_hit or domain_score >= 4)
                            )
                            address_ok = bool(addr) and (addr_hit or zip_hit or pref_only_ok)
                            evidence_score = int(rule_details.get("official_evidence_score") or 0)
                            ai_bonus = 0.0
                            aj = record.get("ai_judge")
                            ai_official_hint = False
                            if aj:
                                is_official_site = aj.get("is_official_site")
                                if is_official_site is None:
                                    is_official_site = aj.get("is_official")
                                conf = aj.get("official_confidence")
                                if conf is None:
                                    conf = aj.get("confidence")
                                try:
                                    conf_f = float(conf) if conf is not None else 0.0
                                except Exception:
                                    conf_f = 0.0
                                ai_official_hint = ai_official_hint_from_judge(aj, AI_VERIFY_MIN_CONFIDENCE)
                                if ai_official_hint:
                                    ai_bonus = 6.0
                            if ai_official_rejected_record is record:
                                ai_bonus = max(ai_bonus, 6.0)
                            allow_without_host = (
                                strong_domain_host
                                or name_present
                                or domain_score >= 4
                                or (address_ok and domain_score >= 3)
                                or evidence_score >= 10
                            )
                            # AI公式ヒントは「除外」ではなく暫定候補として保持する（誤爆回避のため directory_like は除外済み）
                            if not host_token_hit and not allow_without_host and not ai_official_hint:
                                continue
                            score = (
                                domain_score * 2
                                + (3 if address_ok else 0)
                                + float(rule_details.get("score") or 0.0)
                                + min(6.0, evidence_score / 2.0)
                                + (4 if strong_domain_host else 0)
                                + ai_bonus
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
                            aj = best_record.get("ai_judge") if isinstance(best_record.get("ai_judge"), dict) else None
                            provisional_ai_hint = ai_official_hint_from_judge(aj, AI_VERIFY_MIN_CONFIDENCE)

                            # 完全に根拠が無い暫定URLのみ破棄（ただしAI公式ヒントは暫定として保持）
                            if not provisional_host_token and not allow_without_host and not provisional_ai_hint:
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
                            provisional_evidence_score = int(rule_details.get("official_evidence_score") or 0)
                            if provisional_info:
                                try:
                                    pt = scraper.classify_page_type(
                                        normalized_url,
                                        text=provisional_info.get("text", "") or "",
                                        html=provisional_info.get("html", "") or "",
                                    ).get("page_type") or "OTHER"
                                    page_type_per_url[normalized_url] = str(pt)
                                    provisional_profile_hit = (pt == "COMPANY_PROFILE")
                                except Exception:
                                    pass
                            # ログだけ出して深掘りターゲットとする
                            if provisional_ai_hint:
                                company["provisional_reason"] = "ai_official_hint"
                            log.info(
                                "[%s] 公式未確定のため暫定URLで深掘り: %s (domain_score=%s name=%s addr=%s host_token=%s ai_hint=%s)",
                                cid,
                                provisional_homepage,
                                domain_score,
                                name_present,
                                address_ok,
                                strong_domain_host,
                                provisional_ai_hint,
                            )

                    # 暫定URLは深掘りにのみ使用し、保存は公式昇格条件を満たした場合に限定
                    if not homepage and provisional_homepage:
                        weak_provisional = (
                            provisional_domain_score < 3
                            and not provisional_host_token
                            and not provisional_name_present
                            and not provisional_address_ok
                            and not provisional_ai_hint
                            and provisional_evidence_score < 6
                            and not provisional_profile_hit
                        )
                        if weak_provisional:
                            # ルール上は弱いがAI公式（ただし除外）を見ている場合は、reviewで保持して深掘りは継続する
                            if ai_official_rejected_record:
                                rejected_url = ai_official_rejected_record.get("normalized_url") or ai_official_rejected_record.get("url") or ""
                                if rejected_url:
                                    ds = int(ai_official_rejected_record.get("domain_score") or 0)
                                    if ds == 0:
                                        ds = scraper._domain_score(company_tokens, rejected_url)  # type: ignore
                                        ai_official_rejected_record["domain_score"] = ds
                                    homepage = rejected_url
                                    info = ai_official_rejected_record.get("info")
                                    primary_cands = ai_official_rejected_record.get("extracted") or {}
                                    selected_candidate_record = ai_official_rejected_record
                                    homepage_official_flag = 0
                                    homepage_official_source = f"ai_rejected:{ai_official_rejected_reason or 'weak_provisional'}"
                                    homepage_official_score = float((ai_official_rejected_record.get("rule") or {}).get("score") or 0.0)
                                    chosen_domain_score = ds
                                    force_review = True
                                else:
                                    homepage = ""
                                    primary_cands = {}
                                    provisional_info = None
                                    provisional_cands = {}
                                    force_review = True
                                    provisional_homepage = ""
                            else:
                                # 弱い暫定でも「深掘り起点」としては保持する（保存可否は後段のポリシー/envで制御）
                                homepage = provisional_homepage
                                info = provisional_info
                                primary_cands = provisional_cands
                                selected_candidate_record = best_record
                                homepage_official_flag = 0
                                homepage_official_source = "provisional_weak"
                                homepage_official_score = float((best_record.get("rule") or {}).get("score") or 0.0) if best_record else 0.0
                                chosen_domain_score = int(provisional_domain_score or 0)
                                force_review = True
                                if not (company.get("provisional_reason") or "").strip():
                                    company["provisional_reason"] = "weak_provisional_target"
                        else:
                            homepage = provisional_homepage
                            info = provisional_info
                            primary_cands = provisional_cands
                            selected_candidate_record = best_record
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
                                selected_candidate_record = best_record
                                homepage_official_flag = 0
                                homepage_official_source = homepage_official_source or "provisional_timeout"
                                homepage_official_score = float(rule_details.get("score") or 0.0)
                                chosen_domain_score = domain_score
                                force_review = True

                    official_phase_end = elapsed()
                    deep_phase_deadline = time.monotonic() + DEEP_PHASE_TIMEOUT_SEC if DEEP_PHASE_TIMEOUT_SEC > 0 else None
                    priority_docs: dict[str, dict[str, Any]] = {}
                    if selected_candidate_record:
                        preload_docs = selected_candidate_record.get("profile_docs") or {}
                        for url, pdata in preload_docs.items():
                            priority_docs[url] = {
                                "text": (pdata.get("text", "") or ""),
                                "html": (pdata.get("html", "") or ""),
                            }
                        if priority_docs:
                            for url, pdata in priority_docs.items():
                                absorb_doc_data(url, pdata)
                    # 候補フェーズで取得した profile_docs があれば再利用し、不要な巡回を減らす
                    if homepage:
                        for rec in candidate_records:
                            normalized_url = rec.get("normalized_url") or rec.get("url") or ""
                            if normalized_url != homepage:
                                continue
                            preload_docs = rec.get("profile_docs") or {}
                            for url, pdata in preload_docs.items():
                                priority_docs[url] = {
                                    "text": (pdata.get("text", "") or ""),
                                    "html": (pdata.get("html", "") or ""),
                                }
                            break

                    phone = ""
                    found_address = ""
                    rep_name_val = scraper.clean_rep_name(company.get("rep_name")) or ""
                    if AI_DESCRIPTION_ALWAYS or REGENERATE_DESCRIPTION:
                        # 毎回AIで作る/再生成したい場合は既存を参照しない（後段でAI候補を優先採用）
                        description_val = ""
                    else:
                        description_val = clean_description_value(company.get("description") or "")
                    listing_val = clean_listing_value(company.get("listing") or "")
                    revenue_val = clean_amount_value(company.get("revenue") or "")
                    profit_val = clean_amount_value(company.get("profit") or "")
                    capital_val = clean_amount_value(company.get("capital") or "")
                    employees_val = clean_employee_value(company.get("employees") or "")
                    industry_major = (company.get("industry_major") or "").strip()
                    industry_middle = (company.get("industry_middle") or "").strip()
                    industry_minor = (company.get("industry_minor") or "").strip()
                    industry_major_code = ""
                    industry_middle_code = ""
                    industry_minor_code = ""
                    industry_class_source = ""
                    industry_class_confidence = 0.0
                    contact_url = (company.get("contact_url") or "").strip()
                    contact_url_source = (company.get("contact_url_source") or "").strip()
                    contact_url_score = float(company.get("contact_url_score") or 0.0)
                    contact_url_reason = (company.get("contact_url_reason") or "").strip()
                    contact_url_checked_at = (company.get("contact_url_checked_at") or "").strip()
                    contact_url_ai_verdict = (company.get("contact_url_ai_verdict") or "").strip()
                    contact_url_ai_confidence = float(company.get("contact_url_ai_confidence") or 0.0)
                    contact_url_ai_reason = (company.get("contact_url_ai_reason") or "").strip()
                    contact_url_status = (company.get("contact_url_status") or "").strip()
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
                    company.setdefault("employees", employees_val)
                    company.setdefault("industry_major", industry_major)
                    company.setdefault("industry_middle", industry_middle)
                    company.setdefault("industry_minor", industry_minor)
                    company.setdefault("industry_major_code", industry_major_code)
                    company.setdefault("industry_middle_code", industry_middle_code)
                    company.setdefault("industry_minor_code", industry_minor_code)
                    company.setdefault("industry_class_source", industry_class_source)
                    company.setdefault("industry_class_confidence", industry_class_confidence)
                    company.setdefault("industry_minor_item_code", "")
                    company.setdefault("industry_minor_item", "")
                    company.setdefault("contact_url", contact_url)
                    company.setdefault("contact_url_source", contact_url_source)
                    company.setdefault("contact_url_score", contact_url_score)
                    company.setdefault("contact_url_reason", contact_url_reason)
                    company.setdefault("contact_url_checked_at", contact_url_checked_at)
                    company.setdefault("contact_url_ai_verdict", contact_url_ai_verdict)
                    company.setdefault("contact_url_ai_confidence", contact_url_ai_confidence)
                    company.setdefault("contact_url_ai_reason", contact_url_ai_reason)
                    company.setdefault("contact_url_status", contact_url_status)
                    company.setdefault("fiscal_month", fiscal_val)
                    company.setdefault("founded_year", founded_val)
                    src_phone = ""
                    src_addr = ""
                    src_rep = ""
                    verify_result = {"phone_ok": False, "address_ok": False}
                    verify_result_source = "none"
                    confidence = 0.0
                    address_ai_confidence: float | None = None
                    address_ai_evidence: str | None = None
                    rule_phone = None
                    rule_address = None
                    rule_rep = None
                    best_rule_phone_score = float("-inf")
                    best_rule_phone_url = ""

                    def consider_rule_phone(raw_candidates: list[str] | None, url: str, pt: str) -> None:
                        nonlocal rule_phone, src_phone, best_rule_phone_score, best_rule_phone_url
                        for raw in raw_candidates or []:
                            norm, score = score_phone_candidate(str(raw), pt)
                            if not norm:
                                continue
                            if score > best_rule_phone_score:
                                best_rule_phone_score = score
                                best_rule_phone_url = url
                                rule_phone = norm
                                src_phone = url

                    need_listing = not bool(listing_val)
                    need_capital = not bool(capital_val)
                    need_revenue = not bool(revenue_val)
                    need_profit = not bool(profit_val)
                    need_fiscal = not bool(fiscal_val)
                    need_founded = not bool(founded_val)
                    need_description = not bool(description_val)

                    info_dict = info or {}
                    info_url = homepage
                    page_type_per_url: dict[str, str] = {}
                    page_score_per_url: dict[str, int] = {}
                    drop_reasons: dict[str, str] = {}
                    drop_details_by_url: dict[str, dict[str, str]] = {}
                    candidates_brief_by_url: dict[str, dict[str, Any]] = {}
                    ai_official_selected = bool(
                        homepage
                        and homepage_official_flag == 1
                        and isinstance(homepage_official_source, str)
                        and homepage_official_source.startswith("ai")
                    )
                    # 優先docs（会社概要/連絡先）取得は「公式とみなせるホームページ」なら許可する。
                    docs_allowed = bool(homepage and int(homepage_official_flag or 0) == 1)
                    # deep crawl は profile/rep を拾うのに有効だが、誤巡回を避けるため「公式判定が立っている場合」のみに限定する。
                    # 既定では rule 公式でも deep を許可する（会社概要ページ到達を取りこぼさないため）。
                    DEEP_ALLOW_RULE_OFFICIAL = (os.getenv("DEEP_ALLOW_RULE_OFFICIAL", "true") or "true").strip().lower() == "true"
                    deep_allowed = bool(ai_official_selected or (docs_allowed and DEEP_ALLOW_RULE_OFFICIAL))

                    def absorb_doc_data(url: str, pdata: dict[str, Any]) -> None:
                        nonlocal rule_phone, rule_address, rule_rep
                        nonlocal src_phone, src_addr, src_rep
                        nonlocal listing_val, need_listing
                        nonlocal capital_val, need_capital
                        nonlocal revenue_val, need_revenue
                        nonlocal profit_val, need_profit
                        nonlocal employees_val
                        nonlocal fiscal_val, need_fiscal
                        nonlocal founded_val, need_founded
                        nonlocal description_val, need_description

                        text_val = pdata.get("text", "") or ""
                        html_val = pdata.get("html", "") or ""
                        try:
                            pt_rec = scraper.classify_page_type(url, text=text_val, html=html_val) or {}
                            pt = pt_rec.get("page_type") or "OTHER"
                            try:
                                page_score_per_url[url] = int(pt_rec.get("score") or 0)
                            except Exception:
                                page_score_per_url[url] = 0
                        except Exception:
                            pt = "OTHER"
                            page_score_per_url[url] = 0
                        page_type_per_url[url] = str(pt)

                        cc = scraper.extract_candidates(text_val, html_val, page_type_hint=str(pt))
                        try:
                            candidates_brief_by_url[url] = {
                                "page_type": str(pt),
                                "phone_numbers": list(cc.get("phone_numbers") or [])[:3],
                                "addresses": list(cc.get("addresses") or [])[:3],
                                "rep_names": list(cc.get("rep_names") or [])[:3],
                            }
                        except Exception:
                            pass
                        if cc.get("phone_numbers"):
                            before = best_rule_phone_score
                            consider_rule_phone(list(cc.get("phone_numbers") or []), url, str(pt))
                            if best_rule_phone_score == before and not rule_phone:
                                # 候補はあるが、弱いページタイプ+弱いソースで採用できなかった
                                drop_reasons["phone"] = drop_reasons.get("phone") or f"not_profile:{pt}"
                        if cc.get("addresses"):
                            cand_addr = pick_best_address(None if ai_official_selected else addr, cc["addresses"])
                            if cand_addr and not rule_address:
                                cand_norm = normalize_address(cand_addr) or cand_addr
                                ok, reason = _address_candidate_ok(
                                    cand_norm,
                                    cc.get("addresses") or [],
                                    pt,
                                    addr,
                                    ai_official_selected,
                                )
                                if ok:
                                    rule_address = cand_norm
                                    src_addr = url
                                else:
                                    reason = reason or "no_hq_marker"
                                    drop_reasons["address"] = drop_reasons.get("address") or f"{reason}:{pt}"
                        if cc.get("rep_names"):
                            cand_rep = pick_best_rep(cc["rep_names"], url)
                            cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                            if cand_rep:
                                rep_ok, rep_reason = _rep_candidate_ok(
                                    cand_rep,
                                    cc.get("rep_names") or [],
                                    pt,
                                    url,
                                )
                                if rep_ok:
                                    if not rule_rep or len(cand_rep) > len(rule_rep):
                                        rule_rep = cand_rep
                                        src_rep = url
                                else:
                                    drop_reasons["rep"] = drop_reasons.get("rep") or rep_reason
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
                        if cc.get("employees"):
                            candidate = pick_best_employee(cc["employees"])
                            if candidate and not employees_val:
                                employees_val = candidate
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

                    def pick_primary_info_url(candidates: list[str], current: str) -> str:
                        pool = [u for u in candidates if isinstance(u, str) and u]
                        if not pool:
                            return current

                        def _key(u: str) -> tuple[int, int, int, int, int, str]:
                            pt = str(page_type_per_url.get(u) or "OTHER")
                            score = int(page_score_per_url.get(u) or 0)
                            is_profile_like = _is_profile_like_url(u)
                            is_contactish = pt == "ACCESS_CONTACT"
                            return (
                                0 if pt == "COMPANY_PROFILE" else 1,
                                1 if is_contactish else 0,
                                0 if is_profile_like else 1,
                                -score,
                                len(u or ""),
                                u,
                            )

                        best = sorted(pool, key=_key)[0]
                        if str(page_type_per_url.get(best) or "") == "ACCESS_CONTACT" and current:
                            return current
                        return best or current

                    def update_description_candidate(candidate: str | None) -> bool:
                        nonlocal description_val, need_description
                        if not candidate:
                            return False
                        cleaned = clean_description_value(candidate)
                        if not cleaned:
                            return False
                        if looks_mojibake(cleaned):
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
                        # AI最終選択の仕様（80〜160）も受けられるよう上限は160にしておく
                        if len(cleaned) > 160:
                            cleaned = cleaned[:160].rstrip()
                        if cleaned == description_val:
                            return False
                        description_val = cleaned
                        need_description = False
                        return True

                    def apply_ai_description(candidate: str | None) -> bool:
                        nonlocal description_val
                        if not (isinstance(candidate, str) and candidate.strip()):
                            return False
                        prev_desc = description_val
                        description_val = ""
                        ok = update_description_candidate(candidate)
                        if not ok:
                            description_val = prev_desc
                        return ok

                    # ---- AI description ----
                    # 1) AI公式判定で同時生成されたdescription
                    applied_ai_desc = False
                    # 既存descriptionがある場合は、AI_DESCRIPTION_ALWAYS / REGENERATE_DESCRIPTION のときだけ上書きする。
                    if (AI_DESCRIPTION_ALWAYS or REGENERATE_DESCRIPTION or (not description_val)) and isinstance(ai_official_description, str) and ai_official_description.strip():
                        applied_ai_desc = apply_ai_description(ai_official_description)

                    # 2) 公式選定がAI以外でも、selected_candidate_record がAI審査済みならそのdescriptionを使う
                    if not applied_ai_desc and isinstance(selected_candidate_record, dict):
                        aj = selected_candidate_record.get("ai_judge")
                        if isinstance(aj, dict):
                            cand = aj.get("description")
                            if (AI_DESCRIPTION_ALWAYS or REGENERATE_DESCRIPTION or (not description_val)) and isinstance(cand, str) and cand.strip():
                                applied_ai_desc = apply_ai_description(cand)

                    # 3) それでもAI由来が無い場合、追加AI呼び出しでdescriptionだけ生成（保証モード）
                    if (
                        AI_DESCRIPTION_ALWAYS
                        and (not AI_DESCRIPTION_ALWAYS_CALL)
                        and not description_val
                        and AI_DESCRIPTION_FALLBACK_CALL
                        and USE_AI
                        and verifier is not None
                        and homepage
                        and (not timed_out)
                        and has_time_for_ai()
                    ):
                        try:
                            blocks: list[str] = []
                            if info_dict and (info_dict.get("text") or info_dict.get("html")):
                                blocks.append(info_dict.get("text", "") or "")
                            # 既取得の優先docsがあれば少し足す（追加fetchはしない）
                            for pdata in list(priority_docs.values())[:2]:
                                blocks.append(pdata.get("text", "") or "")
                            desc_text = build_ai_text_payload(*blocks)
                            shot = (
                                info_dict.get("screenshot")
                                if isinstance(info_dict, dict) and isinstance(info_dict.get("screenshot"), (bytes, bytearray))
                                else b""
                            )
                            industry_hint = (company.get("industry") or "").strip()
                            if not industry_hint:
                                hint_payloads: list[dict[str, Any]] = []
                                if isinstance(info_dict, dict):
                                    hint_payloads.append(info_dict)
                                try:
                                    hint_payloads.extend(list(priority_docs.values()))
                                except Exception:
                                    pass
                                inferred, _ = infer_industry_and_business_tags(_collect_business_text_blocks(hint_payloads))
                                industry_hint = inferred or ""
                            generated = await asyncio.wait_for(
                                verifier.generate_description(desc_text, bytes(shot), name, addr_raw, industry_hint=industry_hint),
                                timeout=clamp_timeout(ai_call_timeout),
                            )
                            apply_ai_description(generated)
                        except Exception:
                            pass

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
                        if over_hard_deadline() or over_time_limit() or over_deep_phase_deadline():
                            timed_out = True
                            if over_hard_deadline():
                                raise_hard_timeout("priority_docs")
                            priority_docs = {}
                        else:
                            should_fetch_priority = (
                                docs_allowed
                                and (
                                    missing_contact > 0
                                    or need_description
                                    or need_founded
                                    or need_listing
                                    or need_revenue
                                    or need_profit
                                    or need_capital
                                    or need_fiscal
                                )
                            )
                            # verify=docs は、同一サイト内の「会社概要/連絡先」ページを優先取得できると精度と速度が上がる。
                            # 既に priority_docs がある場合は追加fetchしない。
                            verify_docs_required = (os.getenv("VERIFY_DOCS_REQUIRED", "true") or "true").strip().lower() == "true"
                            want_docs_for_verify = bool(deep_allowed and verify_docs_required and not should_fetch_priority and not priority_docs)
                            allow_slow_priority = bool(
                                missing_contact > 0
                                or need_rep
                                or need_description
                                or need_founded
                                or need_listing
                            )
                            try:
                                priority_max_links = 0
                                if should_fetch_priority:
                                    # 代表者/会社概要の取りこぼしが多いので、必要時は探索枠を少し広げる
                                    priority_max_links = 5 if (need_rep or need_description or need_founded or need_listing) else 3
                                elif want_docs_for_verify:
                                    priority_max_links = 2
                                if priority_max_links > 0:
                                    priority_max_links = min(priority_max_links, PRIORITY_DOCS_MAX_LINKS_CAP)
                                early_priority_docs = await asyncio.wait_for(
                                    scraper.fetch_priority_documents(
                                        homepage,
                                        info_dict.get("html", ""),
                                        max_links=priority_max_links,
                                        concurrency=(FETCH_CONCURRENCY if should_fetch_priority else 2),
                                        target_types=(["about", "finance"] if should_fetch_priority else (["about"] if want_docs_for_verify else None)),
                                        allow_slow=(allow_slow_priority if should_fetch_priority else False),
                                        exclude_urls=set(priority_docs.keys()) if priority_docs else None,
                                    ),
                                    timeout=bulk_timeout(
                                        PAGE_FETCH_TIMEOUT_SEC,
                                        priority_max_links if priority_max_links > 0 else (3 if should_fetch_priority else 2),
                                        slow=(allow_slow_priority if should_fetch_priority else False),
                                    ),
                                ) if (should_fetch_priority or want_docs_for_verify) else {}
                            except Exception:
                                early_priority_docs = {}
                            for url, pdata in early_priority_docs.items():
                                priority_docs[url] = pdata
                                absorb_doc_data(url, pdata)
                            # 会社概要/企業情報ページに到達できている場合は、以降の抽出の起点をそちらに寄せる
                            # （トップページだけでは代表者等が載っていないケースが多いため）
                            try:
                                candidates = [info_url, *list((priority_docs or {}).keys())]
                                adopted_url = pick_primary_info_url(candidates, info_url)
                                adopted = (priority_docs or {}).get(adopted_url) or {}
                                if adopted_url and adopted and adopted_url != info_url:
                                    log.info("[%s] prioritize profile doc as primary info_url: %s (was %s)", cid, adopted_url, info_url)
                                    info_url = adopted_url
                                    info_dict = {"url": adopted_url, "text": adopted.get("text", "") or "", "html": adopted.get("html", "") or ""}
                                    try:
                                        pt_hint = str((page_type_per_url or {}).get(adopted_url) or "OTHER")
                                        primary_cands = scraper.extract_candidates(
                                            info_dict.get("text", "") or "",
                                            info_dict.get("html", "") or "",
                                            page_type_hint=pt_hint,
                                        )
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            if timed_out:
                                missing_contact, missing_extra = refresh_need_flags()
                                fully_filled = False
                                related = {}

                        cands = primary_cands or {}
                        phones = cands.get("phone_numbers") or []
                        addrs = cands.get("addresses") or []
                        reps = cands.get("rep_names") or []
                        listings = cands.get("listings") or []
                        capitals = cands.get("capitals") or []
                        revenues = cands.get("revenues") or []
                        profits = cands.get("profits") or []
                        employees = cands.get("employees") or []
                        fiscals = cands.get("fiscal_months") or []
                        founded_years = cands.get("founded_years") or []

                        # primary_cands は page_type が不明なことがあるため、基本は absorb_doc_data の結果を優先する
                        pt_info = page_type_per_url.get(info_url) or "OTHER"
                        if phones:
                            consider_rule_phone(list(phones), info_url, str(pt_info))
                        if addrs and not rule_address:
                            cand_addr = pick_best_address(None if ai_official_selected else addr, addrs)
                            if cand_addr:
                                cand_norm = normalize_address(cand_addr) or cand_addr
                                ok, reason = _address_candidate_ok(
                                    cand_norm,
                                    addrs,
                                    pt_info,
                                    addr,
                                    ai_official_selected,
                                )
                                if ok:
                                    rule_address = cand_norm
                                else:
                                    reason = reason or "no_hq_marker"
                                    drop_reasons["address"] = drop_reasons.get("address") or f"{reason}:{pt_info}"
                        if reps and not rule_rep:
                            cand_rep = pick_best_rep(reps, info_url)
                            cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                            if cand_rep:
                                rep_ok, rep_reason = _rep_candidate_ok(
                                    cand_rep,
                                    reps,
                                    pt_info,
                                    info_url,
                                )
                                if rep_ok:
                                    rule_rep = cand_rep
                                else:
                                    drop_reasons["rep"] = drop_reasons.get("rep") or rep_reason
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
                        if employees and not employees_val:
                            candidate = pick_best_employee(employees)
                            if candidate:
                                employees_val = candidate
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
                            if over_hard_deadline():
                                raise_hard_timeout("after_priority_docs")

                        fully_filled = homepage and missing_contact == 0 and missing_extra == 0

                        try:
                            # 深掘りは不足がある場合のみ。揃っていれば追加巡回しない。
                            priority_limit = 0
                            # 不足項目に応じて深掘り対象リンクを絞り込む
                            target_types: list[str] = []
                            if not timed_out and not fully_filled and not over_deep_phase_deadline():
                                if missing_contact > 0:
                                    # 連絡先ページはノイズになりやすいので、会社概要系から拾う方針に寄せる
                                    priority_limit = 2
                                    base_pt = page_type_per_url.get(info_url) or page_type_per_url.get(homepage) or "OTHER"
                                    # provisional/非プロフィール起点でも会社概要へ寄せる
                                    if base_pt != "COMPANY_PROFILE" and "about" not in target_types:
                                        target_types.append("about")
                                if need_description or need_founded or need_listing:
                                    priority_limit = max(priority_limit, 2)
                                    target_types.append("about")
                                if need_revenue or need_profit or need_capital or need_fiscal:
                                    priority_limit = max(priority_limit, 2)
                                    target_types.append("finance")
                            site_docs = {}
                            # 既取得URLは除外しつつ不足がある場合のみ追加巡回する
                            if priority_limit > 0 and not over_deep_phase_deadline():
                                allow_slow_priority = bool("about" in target_types)
                                site_docs = await asyncio.wait_for(
                                    scraper.fetch_priority_documents(
                                        homepage,
                                        info_dict.get("html", ""),
                                        max_links=priority_limit,
                                        concurrency=FETCH_CONCURRENCY,
                                        target_types=target_types or None,
                                        allow_slow=allow_slow_priority,
                                        exclude_urls=set(priority_docs.keys()) if priority_docs else None,
                                    ),
                                    timeout=bulk_timeout(PAGE_FETCH_TIMEOUT_SEC, priority_limit, slow=allow_slow_priority),
                                )
                        except Exception:
                            site_docs = {}
                        for url, pdata in site_docs.items():
                            priority_docs[url] = pdata
                            absorb_doc_data(url, pdata)
                        if site_docs:
                            # provisional起点が会社概要でない場合、会社概要ページに到達できたら採用元URLを切り替える
                            try:
                                base_pt = page_type_per_url.get(info_url) or page_type_per_url.get(homepage) or "OTHER"
                            except Exception:
                                base_pt = page_type_per_url.get(info_url) or "OTHER"
                            if homepage and homepage_official_flag == 0 and base_pt != "COMPANY_PROFILE":
                                adopted_url = pick_primary_info_url([homepage, info_url, *list(site_docs.keys())], homepage)
                                adopted = site_docs.get(adopted_url) or priority_docs.get(adopted_url)
                                if adopted and adopted_url and adopted_url != homepage:
                                    log.info("[%s] provisional起点から会社概要へ誘導: adopt=%s from=%s", cid, adopted_url, homepage)
                                    info_url = adopted_url
                                    info_dict = adopted
                                    homepage = adopted_url
                                    try:
                                        provisional_profile_hit = True
                                        if adopted_url:
                                            ds = scraper._domain_score(company_tokens, adopted_url)  # type: ignore
                                            provisional_domain_score = max(int(provisional_domain_score or 0), int(ds or 0))
                                            provisional_host_token = provisional_host_token or scraper._host_token_hit(company_tokens, adopted_url)  # type: ignore
                                        text_val = adopted.get("text", "") or ""
                                        html_val = adopted.get("html", "") or ""
                                        extracted = scraper.extract_candidates(text_val, html_val, page_type_hint=str(page_type_per_url.get(adopted_url) or "OTHER"))
                                        adopted_rule = scraper.is_likely_official_site(
                                            name,
                                            adopted_url,
                                            {"url": adopted_url, "text": text_val, "html": html_val},
                                            addr,
                                            extracted,
                                            return_details=True,
                                        )
                                        if isinstance(adopted_rule, dict):
                                            provisional_name_present = provisional_name_present or bool(adopted_rule.get("name_present"))
                                            provisional_address_ok = provisional_address_ok or bool(
                                                adopted_rule.get("address_match")
                                                or adopted_rule.get("prefecture_match")
                                                or adopted_rule.get("postal_code_match")
                                            )
                                            provisional_evidence_score = max(
                                                int(provisional_evidence_score or 0),
                                                int(adopted_rule.get("official_evidence_score") or 0),
                                            )
                                    except Exception:
                                        pass
                                        # 保存用の暫定URL(起点)は保持しつつ、採用URLは切り替える
                                        force_review = True
                            missing_contact, missing_extra = refresh_need_flags()

                        # AIは最終手段（1社あたり最大1回）なので、この段階では呼び出さない。
                        # deep後に候補群（住所/電話/代表/会社情報/事業テキスト）をまとめて1回だけAIへ渡す。
                        ai_result = None
                        ai_attempted = False
                        ai_phone: str | None = None
                        ai_addr: str | None = None
                        ai_rep: str | None = None

                        missing_contact, missing_extra = refresh_need_flags()
                        if need_description:
                            payloads: list[dict[str, Any]] = []
                            if info_dict:
                                payloads.append(info_dict)
                            payloads.extend(priority_docs.values())
                            if not AI_DESCRIPTION_ALWAYS:
                                for pdata in payloads:
                                    desc = extract_description_from_payload(pdata)
                                    if desc:
                                        description_val = desc
                                        need_description = False
                                        break

                        if rule_phone:
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
                        else:
                            found_address = rule_address or ""
                            address_source = "none"

                        if rule_rep:
                            if not rep_name_val or len(rule_rep) > len(rep_name_val):
                                rep_name_val = rule_rep
                                if not src_rep:
                                    src_rep = info_url
                        # deep crawl は代表者の有無に関係なく実行する（不足がある場合のみ）
                        deep_pages_visited = 0
                        deep_fetch_count = 0
                        deep_fetch_failures = 0
                        deep_skip_reason = ""
                        deep_urls_visited = []
                        deep_phone_candidates = 0
                        deep_address_candidates = 0
                        deep_rep_candidates = 0

                        missing_contact, missing_extra = refresh_need_flags()
                        need_extra_fields = missing_extra > 0
                        related = {}
                        related_meta: dict[str, Any] = {}
                        if not deep_allowed:
                            deep_skip_reason = "ai_not_selected"
                        elif missing_contact == 0 and not need_extra_fields:
                            deep_skip_reason = "no_missing_fields"
                        else:
                            # deep は「会社概要/企業情報」等のプロフィールページに到達できた場合のみ実行する。
                            # （プロフィールに到達していない状態で広く巡回すると誤取得リスクが上がるため）
                            profile_urls: list[str] = [
                                u for u, pt in (page_type_per_url or {}).items() if str(pt) == "COMPANY_PROFILE"
                            ]
                            # AI公式判定で参照した pages の page_type も起点候補に含める（分類ミス/未吸収の補完）
                            try:
                                meta_pages = (selected_candidate_record or {}).get("_ai_pages_meta") or []
                                for p in meta_pages:
                                    if not isinstance(p, dict):
                                        continue
                                    u = p.get("url")
                                    if not u or u in profile_urls:
                                        continue
                                    # AI公式判定側の page_type は分類ミスもあり得るため、URL形状も併用して拾う
                                    if str(p.get("page_type") or "") == "COMPANY_PROFILE" or _is_profile_like_url(str(u)):
                                        profile_urls.append(u)
                            except Exception:
                                pass
                            # ルール分類が profile を落とした場合でも、URL形状が強い「会社概要」パスは起点候補に含める
                            try:
                                for u in list((page_type_per_url or {}).keys()):
                                    if u and (u not in profile_urls) and _is_profile_like_url(str(u)):
                                        profile_urls.append(u)
                            except Exception:
                                pass
                            # プロフィールページが未到達でも、トップ/公式ページ内に導線がある場合があるため、
                            # 「会社概要系リンク」を最小限だけ追加fetchして起点を見つける（時間/誤取得リスクを抑える）。
                            if not profile_urls and info_url and (not timed_out):
                                try:
                                    if over_time_limit() or over_deep_limit() or over_hard_deadline() or over_deep_phase_deadline():
                                        timed_out = True
                                    else:
                                        base_html = (info_dict or {}).get("html", "") if isinstance(info_dict, dict) else ""
                                        exclude_urls = set((page_type_per_url or {}).keys())
                                        discover_max_links = 4 if (need_rep or need_description or need_founded or need_listing) else 2
                                        discover_max_links = min(discover_max_links, PROFILE_DISCOVERY_MAX_LINKS_CAP)
                                        discovered = await asyncio.wait_for(
                                            scraper.fetch_priority_documents(
                                                info_url,
                                                base_html=base_html,
                                                max_links=discover_max_links,
                                                concurrency=2,
                                                target_types=["about"],
                                                allow_slow=False,
                                                exclude_urls=exclude_urls,
                                            ),
                                            timeout=bulk_timeout(6.0, count=discover_max_links),
                                        )
                                        for u, pdata in (discovered or {}).items():
                                            absorb_doc_data(u, pdata)
                                        profile_urls = [
                                            u for u, pt in (page_type_per_url or {}).items() if str(pt) == "COMPANY_PROFILE"
                                        ]
                                except asyncio.TimeoutError:
                                    pass
                                except Exception:
                                    log.debug("[%s] profile discovery skipped", cid, exc_info=True)

                            if not profile_urls:
                                deep_skip_reason = "no_profile_page"
                                related = {}
                                related_meta = {}
                            else:
                                profile_urls.sort(
                                    key=lambda u: (
                                        0 if any(seg in (u or "").lower() for seg in ("/company", "/about", "/corporate", "/profile", "/overview", "/summary", "/outline")) else 1,
                                        len(u or ""),
                                        u,
                                    )
                                )
                                deep_start_url = profile_urls[0]
                                deep_start_info = None
                                if deep_start_url == info_url:
                                    deep_start_info = info_dict
                                else:
                                    deep_start_info = (priority_docs or {}).get(deep_start_url) if isinstance(priority_docs, dict) else None

                            related_page_limit = RELATED_BASE_PAGES + (1 if missing_extra else 0)
                            if need_phone:
                                related_page_limit += RELATED_EXTRA_PHONE
                            if need_rep:
                                related_page_limit += 1
                            if need_description:
                                related_page_limit += 1
                            related_cap = 6 if ai_official_selected else 4
                            related_page_limit = max(0, min(related_cap, related_page_limit))

                            deep_on_weak = os.getenv("DEEP_ON_WEAK_PROVISIONAL", "true").lower() == "true"
                            weak_provisional_target = (
                                homepage_official_flag == 0
                                and homepage_official_source.startswith("provisional")
                                and chosen_domain_score < 3
                                and not provisional_host_token
                                and not provisional_name_present
                                and not provisional_address_ok
                                and not provisional_ai_hint
                                and provisional_evidence_score < 6
                                and not provisional_profile_hit
                            )
                            if weak_provisional_target and not deep_on_weak:
                                deep_skip_reason = "weak_provisional_target"

                            if not deep_skip_reason and (not timed_out) and ((missing_contact > 0) or need_extra_fields):
                                if over_time_limit() or over_deep_limit() or over_hard_deadline() or over_deep_phase_deadline():
                                    deep_skip_reason = "over_limit_before_deep"
                                    timed_out = True
                                    if over_hard_deadline():
                                        raise_hard_timeout("related_crawl")
                                else:
                                    # 弱い暫定URLでも、未取得があるなら「軽量deep」で救済する
                                    if weak_provisional_target and deep_on_weak:
                                        weak_cap = 2 if (need_rep or need_description) else 1
                                        related_page_limit = min(weak_cap, related_page_limit)
                                        max_hops = 2 if (need_rep or need_description) else 1
                                    else:
                                        max_hops = RELATED_MAX_HOPS_PHONE if need_phone else RELATED_MAX_HOPS_BASE
                                        max_hops_cap = 4 if ai_official_selected else 3
                                        max_hops = max(0, min(max_hops_cap, int(max_hops or 0)))
                                    try:
                                        allow_slow_deep = bool(need_addr or need_rep or need_description) and bool(
                                            (homepage_official_flag == 1)
                                            or (chosen_domain_score >= 4)
                                            or provisional_host_token
                                            or provisional_name_present
                                            or provisional_address_ok
                                        )
                                        related, related_meta = await asyncio.wait_for(
                                            scraper.crawl_related(
                                                deep_start_url,
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
                                                initial_info=deep_start_info,
                                                expected_address=addr,
                                                return_meta=True,
                                                allow_slow=allow_slow_deep,
                                            ),
                                            timeout=bulk_timeout(PAGE_FETCH_TIMEOUT_SEC, related_page_limit, slow=allow_slow_deep),
                                        )
                                    except Exception:
                                        related = {}
                                        related_meta = {}
                                        deep_skip_reason = deep_skip_reason or "deep_exception"

                            deep_pages_visited = int((related_meta or {}).get("pages_visited") or len(related))
                            deep_fetch_count = int((related_meta or {}).get("fetch_count") or 0)
                            deep_fetch_failures = int((related_meta or {}).get("fetch_failures") or 0)
                            deep_urls_visited = list((related_meta or {}).get("urls_visited") or list(related.keys()))
                            deep_stop_reason = str((related_meta or {}).get("stop_reason") or "")
                            if related:
                                try:
                                    log.info("[%s] deep_crawl visited=%s", cid, list(related.keys()))
                                except Exception:
                                    pass
                            try:
                                log.info(
                                    "[%s] deep_crawl summary pages=%s fetch=%s fail=%s reason=%s",
                                    cid,
                                    deep_pages_visited,
                                    deep_fetch_count,
                                    deep_fetch_failures,
                                    deep_skip_reason or (related_meta or {}).get("stop_reason") or "",
                                )
                            except Exception:
                                pass

                            deep_items: list[dict[str, Any]] = []
                            for url, data in related.items():
                                    text = data.get("text", "") or ""
                                    html_content = data.get("html", "") or ""
                                    try:
                                        pt_info = scraper.classify_page_type(url, text=text, html=html_content) or {}
                                        pt = pt_info.get("page_type") or "OTHER"
                                        pt_score = int(pt_info.get("score") or 0)
                                    except Exception:
                                        pt = "OTHER"
                                        pt_score = 0
                                    page_type_per_url[url] = str(pt)
                                    cc = scraper.extract_candidates(text, html_content, page_type_hint=str(pt))
                                    deep_phone_candidates += len(cc.get("phone_numbers") or [])
                                    deep_address_candidates += len(cc.get("addresses") or [])
                                    deep_rep_candidates += len(cc.get("rep_names") or [])
                                    present_fields = 0
                                    present_fields += int(bool(cc.get("addresses")))
                                    present_fields += int(bool(cc.get("phone_numbers")))
                                    present_fields += int(bool(cc.get("rep_names")))
                                    present_fields += int(bool(cc.get("capitals")))
                                    present_fields += int(bool(cc.get("revenues")))
                                    present_fields += int(bool(cc.get("profits")))
                                    present_fields += int(bool(cc.get("fiscal_months")))
                                    present_fields += int(bool(cc.get("founded_years")))
                                    deep_items.append(
                                        {
                                            "url": url,
                                            "pt": str(pt),
                                            "pt_score": pt_score,
                                            "cc": cc,
                                            "present_fields": present_fields,
                                        }
                                    )
                            page_type_rank = {
                                "COMPANY_PROFILE": 0,
                                "ACCESS_CONTACT": 1,
                                "BASES_LIST": 2,
                                "OTHER": 3,
                                "DIRECTORY_DB": 4,
                            }
                            deep_items.sort(
                                key=lambda item: (
                                    page_type_rank.get(str(item.get("pt") or "OTHER"), 9),
                                    -int(item.get("present_fields") or 0),
                                    -int(item.get("pt_score") or 0),
                                    0 if _is_profile_like_url(str(item.get("url") or "")) else 1,
                                )
                            )
                            for item in deep_items:
                                    url = str(item.get("url") or "")
                                    pt = str(item.get("pt") or "OTHER")
                                    cc = item.get("cc") or {}
                                    if need_phone and cc.get("phone_numbers"):
                                        before = best_rule_phone_score
                                        consider_rule_phone(list(cc.get("phone_numbers") or []), url, pt)
                                        if rule_phone and best_rule_phone_score > before:
                                            phone = rule_phone
                                            phone_source = "rule"
                                            src_phone = best_rule_phone_url or url
                                            need_phone = False
                                            log.info("[%s] deep_crawl picked phone=%s url=%s", cid, rule_phone, src_phone)
                                        elif not rule_phone:
                                            reason = f"not_profile:{pt}"
                                            drop_reasons["phone"] = drop_reasons.get("phone") or reason
                                            drop_details_by_url.setdefault(url, {})["phone"] = reason
                                            log.info(
                                                "[%s] deep_crawl rejected phones reason=%s url=%s candidates=%s",
                                                cid,
                                                reason,
                                                url,
                                                (cc.get("phone_numbers") or [])[:3],
                                            )
                                    if need_addr and cc.get("addresses"):
                                        cand_addr = pick_best_address(None if ai_official_selected else addr, cc["addresses"])
                                        if cand_addr:
                                            cand_norm = normalize_address(cand_addr) or cand_addr
                                            ok, reason = _address_candidate_ok(
                                                cand_norm,
                                                cc.get("addresses") or [],
                                                pt,
                                                addr,
                                                ai_official_selected,
                                            )
                                            if ok:
                                                found_address = cand_norm
                                                address_source = "rule"
                                                src_addr = url
                                                need_addr = False
                                                log.info("[%s] deep_crawl picked address=%s url=%s", cid, cand_norm, url)
                                            else:
                                                reason = (reason or "no_hq_marker")
                                                reason = f"{reason}:{pt}"
                                                drop_reasons["address"] = drop_reasons.get("address") or reason
                                                drop_details_by_url.setdefault(url, {})["address"] = reason
                                                log.info("[%s] deep_crawl rejected address reason=%s url=%s cand=%s", cid, reason, url, cand_norm)
                                        else:
                                            reason = "no_valid_address"
                                            drop_reasons["address"] = drop_reasons.get("address") or reason
                                            drop_details_by_url.setdefault(url, {})["address"] = reason
                                            log.info(
                                                "[%s] deep_crawl rejected addresses reason=%s url=%s candidates=%s",
                                                cid,
                                                reason,
                                                url,
                                                (cc.get("addresses") or [])[:3],
                                            )
                                    if need_rep and cc.get("rep_names"):
                                        cand_rep = pick_best_rep(cc["rep_names"], url)
                                        cand_rep = scraper.clean_rep_name(cand_rep) if cand_rep else None
                                        if cand_rep:
                                            rep_ok, rep_reason = _rep_candidate_ok(
                                                cand_rep,
                                                cc.get("rep_names") or [],
                                                pt,
                                                url,
                                            )
                                            if rep_ok:
                                                rep_name_val = cand_rep
                                                src_rep = url
                                                need_rep = False
                                                log.info("[%s] deep_crawl picked rep=%s url=%s", cid, cand_rep, url)
                                            else:
                                                reason = rep_reason or f"not_profile:{pt}"
                                                drop_reasons["rep"] = drop_reasons.get("rep") or reason
                                                drop_details_by_url.setdefault(url, {})["rep"] = reason
                                                log.info("[%s] deep_crawl rejected rep reason=%s url=%s cand=%s", cid, reason, url, cand_rep)
                                    if (not AI_DESCRIPTION_ALWAYS) and need_description and cc.get("description"):
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

                            if homepage and (need_addr or not found_address) and not timed_out and not over_deep_phase_deadline():
                                try:
                                    extra_docs = await asyncio.wait_for(
                                        scraper.fetch_priority_documents(
                                            homepage,
                                            info_dict.get("html", ""),
                                            max_links=3,
                                            concurrency=FETCH_CONCURRENCY,
                                            # 住所は会社概要系に載ることが多い。contactを優先しない。
                                            target_types=["about"],
                                            allow_slow=need_addr,
                                            exclude_urls=set(priority_docs.keys()) if priority_docs else None,
                                        ),
                                        timeout=bulk_timeout(PAGE_FETCH_TIMEOUT_SEC, 3, slow=need_addr),
                                    )
                                except Exception:
                                    extra_docs = {}
                                for url, pdata in extra_docs.items():
                                    priority_docs[url] = pdata
                                    absorb_doc_data(url, pdata)

                            deep_phase_end = elapsed()
                            # ---- AI (final, max 1 call/company) ----
                            missing_contact, missing_extra = refresh_need_flags()
                            ai_need_final = bool(
                                homepage
                                and USE_AI
                                and verifier is not None
                                and not timed_out
                                and has_time_for_ai()
                                and (not USE_AI_OFFICIAL or AI_FINAL_WITH_OFFICIAL or AI_FINAL_ALWAYS)
                                and (
                                    missing_contact > 0
                                    or missing_extra > 0
                                    or (not description_val)
                                )
                            )
                            if ai_need_final:
                                try:
                                    # 取得済みdocsから、page_type優先で最大3ページ分だけAIへ渡す（探索増なし）
                                    docs_by_url: dict[str, dict[str, Any]] = {}
                                    if info_url and info_dict:
                                        docs_by_url[info_url] = {
                                            "text": info_dict.get("text", "") or "",
                                            "html": info_dict.get("html", "") or "",
                                        }
                                    for u, d in (priority_docs or {}).items():
                                        if u not in docs_by_url:
                                            docs_by_url[u] = d
                                    for u, d in (related or {}).items():
                                        if u not in docs_by_url:
                                            docs_by_url[u] = {
                                            "text": d.get("text", "") or "",
                                            "html": d.get("html", "") or "",
                                        }

                                    def _pt_priority(pt: str) -> int:
                                        return {"COMPANY_PROFILE": 0, "ACCESS_CONTACT": 1, "OTHER": 2, "BASES_LIST": 3, "DIRECTORY_DB": 4}.get(pt, 9)

                                    scored_urls: list[tuple[int, str]] = []
                                    for u, d in docs_by_url.items():
                                        try:
                                            pt = page_type_per_url.get(u) or scraper.classify_page_type(
                                                u, text=d.get("text", ""), html=d.get("html", "")
                                            ).get("page_type") or "OTHER"
                                        except Exception:
                                            pt = page_type_per_url.get(u) or "OTHER"
                                        page_type_per_url[u] = str(pt)
                                        scored_urls.append((_pt_priority(str(pt)), u))
                                    scored_urls.sort(key=lambda x: (x[0], x[1]))
                                    top_urls_for_ai = [u for _, u in scored_urls if u][:3]

                                    def _pack_candidates(urls_for_ai: list[str]) -> dict[str, Any]:
                                        out: dict[str, Any] = {
                                            "company_name": name,
                                            "csv_address": addr_raw,
                                            "urls": [],
                                            "candidates": {
                                                "phone_numbers": [],
                                                "addresses": [],
                                                "representatives": [],
                                                "company_facts": {"capitals": [], "founded": [], "listing": []},
                                            },
                                            "business_snippets": [],
                                        }
                                        for u in urls_for_ai:
                                            d = docs_by_url.get(u) or {}
                                            t = d.get("text", "") or ""
                                            h = d.get("html", "") or ""
                                            pt = page_type_per_url.get(u) or "OTHER"
                                            out["urls"].append({"url": u, "page_type": pt})
                                            cc = scraper.extract_candidates(t, h, page_type_hint=str(pt))
                                            for p in (cc.get("phone_numbers") or [])[:10]:
                                                out["candidates"]["phone_numbers"].append({"value": p, "url": u, "page_type": pt})
                                            for a in (cc.get("addresses") or [])[:10]:
                                                out["candidates"]["addresses"].append({"value": a, "url": u, "page_type": pt})
                                            for r in (cc.get("rep_names") or [])[:10]:
                                                out["candidates"]["representatives"].append({"value": r, "url": u, "page_type": pt})
                                            for c in (cc.get("capitals") or [])[:8]:
                                                out["candidates"]["company_facts"]["capitals"].append({"value": c, "url": u, "page_type": pt})
                                            for fy in (cc.get("founded_years") or [])[:6]:
                                                out["candidates"]["company_facts"]["founded"].append({"value": fy, "url": u, "page_type": pt})
                                            for li in (cc.get("listings") or [])[:6]:
                                                out["candidates"]["company_facts"]["listing"].append({"value": li, "url": u, "page_type": pt})
                                            biz = select_relevant_paragraphs(t, limit=3)
                                            if biz:
                                                out["business_snippets"].append({"url": u, "snippet": biz[:800], "page_type": pt})
                                        return out

                                    ai_payload = _pack_candidates(top_urls_for_ai)
                                    screenshot_payload = None
                                    if info_url:
                                        info_dict = await ensure_info_has_screenshot(
                                            scraper,
                                            info_url,
                                            info_dict,
                                            need_screenshot=OFFICIAL_AI_USE_SCREENSHOT,
                                            policy=VERIFY_AI_SCREENSHOT_POLICY,
                                        )
                                        screenshot_payload = (info_dict or {}).get("screenshot")

                                    ai_started = time.monotonic()
                                    ai_attempted = True
                                    ai_result = await asyncio.wait_for(
                                        verifier.select_company_fields(ai_payload, screenshot_payload, name, addr_raw),
                                        timeout=clamp_timeout(max(ai_call_timeout, 5.0)),
                                    )
                                    ai_time_spent += time.monotonic() - ai_started
                                except Exception:
                                    ai_result = None

                                if isinstance(ai_result, dict):
                                    ai_used = 1
                                    ai_model = AI_MODEL_NAME
                                    company["ai_confidence"] = ai_result.get("confidence")
                                    stage = str(ai_result.get("selection_stage") or "single")
                                    company["ai_reason"] = f"final_selection:{stage}"[:120]
                                    def _strip_tags_for_ai(raw: str) -> str:
                                        out = raw or ""
                                        while True:
                                            m = re.match(r"^\[[A-Z_]+\]", out)
                                            if not m:
                                                break
                                            out = out[m.end():].lstrip()
                                        return out
                                    def _find_src_item(cands: list[dict[str, Any]], kind: str, chosen: str) -> tuple[str, str, str]:
                                        if not chosen:
                                            return ("", "", "")
                                        for it in cands or []:
                                            if not isinstance(it, dict):
                                                continue
                                            raw = it.get("value")
                                            url0 = it.get("url")
                                            pt0 = it.get("page_type")
                                            if not (isinstance(raw, str) and isinstance(url0, str) and url0):
                                                continue
                                            raw_val = _strip_tags_for_ai(raw)
                                            if kind == "phone" and (normalize_phone(raw_val) or "") == chosen:
                                                return (url0, raw, str(pt0 or ""))
                                            if kind == "address" and (normalize_address(raw_val) or "") == chosen:
                                                return (url0, raw, str(pt0 or ""))
                                            if kind == "rep":
                                                cleaned = scraper.clean_rep_name(raw_val) or ""
                                                if cleaned and cleaned == chosen:
                                                    return (url0, raw, str(pt0 or ""))
                                        return ("", "", "")
                                    def _ai_stage_to_source(stage: str) -> str:
                                        s = (stage or "").strip().lower()
                                        if s in {"relaxed", "mixed"}:
                                            return "ai_relaxed"
                                        if s in {"strict"}:
                                            return "ai"
                                        return "ai"
                                    def _evidence_contains_phone(ev: str, phone_norm: str) -> bool:
                                        if not ev or not phone_norm:
                                            return False
                                        ev_phone = normalize_phone(ev)
                                        return bool(ev_phone and ev_phone == phone_norm)
                                    def _evidence_contains_addr(ev: str, addr_norm: str) -> bool:
                                        if not ev or not addr_norm:
                                            return False
                                        # 雑に部分一致（正規化済みが前提）
                                        a = re.sub(r"\\s+", "", addr_norm)
                                        e = re.sub(r"\\s+", "", ev)
                                        return bool(a and e and (a in e or e in a))
                                    def _normalize_snippet_for_match(text: str) -> str:
                                        s = unicodedata.normalize("NFKC", str(text or ""))
                                        s = re.sub(r"\s+", "", s)
                                        s = re.sub(r"[\"'`“”‘’]", "", s)
                                        return s
                                    def _validate_description_evidence(
                                        evidence_list: Any,
                                        allowed_urls: set[str],
                                        docs: dict[str, dict[str, Any]],
                                    ) -> list[dict[str, str]]:
                                        if not isinstance(evidence_list, list):
                                            return []
                                        out: list[dict[str, str]] = []
                                        for it in evidence_list:
                                            if not isinstance(it, dict):
                                                continue
                                            u = it.get("url")
                                            sn = it.get("snippet")
                                            if not (isinstance(u, str) and isinstance(sn, str)):
                                                continue
                                            u = u.strip()
                                            sn = sn.strip()
                                            if not u or not sn:
                                                continue
                                            if u not in allowed_urls:
                                                continue
                                            doc = docs.get(u) or {}
                                            doc_text = _normalize_snippet_for_match(doc.get("text", "") or "")
                                            sn_norm = _normalize_snippet_for_match(sn)
                                            if not sn_norm or not doc_text:
                                                continue
                                            if sn_norm not in doc_text:
                                                continue
                                            out.append({"url": u, "snippet": sn[:200]})
                                        return out[:4]
                                    if not description_val and isinstance(ai_result.get("description"), str) and ai_result.get("description"):
                                        description_val = ai_result["description"]
                                        require_desc_ev = os.getenv("AI_REQUIRE_DESCRIPTION_EVIDENCE", "true").lower() == "true"
                                        if require_desc_ev:
                                            ev_valid = _validate_description_evidence(
                                                ai_result.get("description_evidence"),
                                                set(top_urls_for_ai or []),
                                                docs_by_url,
                                            )
                                            if len(ev_valid) < 2:
                                                # 根拠が取れない description は DB 汚染を避けて破棄
                                                description_val = ""
                                                company["description_evidence"] = ""
                                            else:
                                                try:
                                                    company["description_evidence"] = json.dumps(ev_valid[:2], ensure_ascii=False)
                                                except Exception:
                                                    description_val = ""
                                                    company["description_evidence"] = ""
                                        else:
                                            try:
                                                company["description_evidence"] = json.dumps(ai_result.get("description_evidence") or [], ensure_ascii=False)
                                            except Exception:
                                                company["description_evidence"] = ""
                                    if not phone and isinstance(ai_result.get("phone_number"), str) and ai_result.get("phone_number"):
                                        phone_candidate = normalize_phone(ai_result.get("phone_number")) or ""
                                        if phone_candidate:
                                            # docs に根拠があるものだけ採用（無根拠で online verify を増やさない）
                                            ok = quick_verify_from_docs(phone_candidate, None).get("phone_ok")
                                            ev = ai_result.get("evidence")
                                            ev_ok = isinstance(ev, str) and _evidence_contains_phone(ev, phone_candidate)
                                            if ok or ev_ok:
                                                phone = phone_candidate
                                                phone_source = _ai_stage_to_source(stage)
                                                if ev_ok and not ok:
                                                    phone_source = "ai_evidence"
                                                src_phone, _, _ = _find_src_item(ai_payload.get("candidates", {}).get("phone_numbers") or [], "phone", phone)
                                    if not found_address and isinstance(ai_result.get("address"), str) and ai_result.get("address"):
                                        addr_ai = normalize_address(ai_result.get("address"))
                                        if addr_ai:
                                            ok = quick_verify_from_docs(None, addr_ai).get("address_ok")
                                            ev = ai_result.get("evidence")
                                            ev_ok = isinstance(ev, str) and _evidence_contains_addr(ev, addr_ai)
                                            if ok or ev_ok:
                                                found_address = addr_ai
                                                address_source = _ai_stage_to_source(stage)
                                                if ev_ok and not ok:
                                                    address_source = "ai_evidence"
                                                src_addr, _, _ = _find_src_item(ai_payload.get("candidates", {}).get("addresses") or [], "address", found_address)
                                                if isinstance(ev, str) and ev.strip():
                                                    address_ai_evidence = ev.strip()[:200]
                                    if not rep_name_val and isinstance(ai_result.get("representative"), str) and ai_result.get("representative"):
                                        rep_candidate = scraper.clean_rep_name(ai_result.get("representative"))
                                        if rep_candidate:
                                            rep_valid = ai_result.get("representative_valid")
                                            if rep_valid is False:
                                                rep_candidate = None
                                        if rep_candidate:
                                            src0, raw0, pt0 = _find_src_item(ai_payload.get("candidates", {}).get("representatives") or [], "rep", rep_candidate)
                                            # 代表者は「役職語とのペア」由来（TABLE/LABEL/ROLE/JSONLD）からのみ採用する
                                            paired = bool(isinstance(raw0, str) and re.match(r"^(?:\\[[A-Z_]+\\])+", raw0) and any(tag in raw0 for tag in ("[TABLE]", "[LABEL]", "[ROLE]", "[JSONLD]")))
                                            url_ok = bool(src0 and (not _is_contact_like_url(src0)) and (_is_profile_like_url(src0) or _is_greeting_like_url(src0) or str(pt0) == "COMPANY_PROFILE"))
                                            if paired and url_ok:
                                                rep_name_val = rep_candidate
                                                src_rep = src0
                                    facts = ai_result.get("company_facts") if isinstance(ai_result.get("company_facts"), dict) else {}
                                    if not founded_val and isinstance(facts.get("founded"), str) and facts.get("founded"):
                                        founded_val = clean_founded_year(facts.get("founded"))
                                    if not capital_val and isinstance(facts.get("capital"), str) and facts.get("capital"):
                                        capital_val = clean_amount_value(facts.get("capital"))
                                    if isinstance(facts.get("employees"), str) and facts.get("employees"):
                                        company["employees"] = str(facts.get("employees"))[:60]
                                    if isinstance(facts.get("license"), str) and facts.get("license"):
                                        company["license"] = str(facts.get("license"))[:80]
                                    if isinstance(ai_result.get("industry"), str) and ai_result.get("industry"):
                                        company["industry"] = str(ai_result.get("industry"))[:60]
                                    if isinstance(ai_result.get("business_tags"), list):
                                        try:
                                            company["business_tags"] = json.dumps(ai_result.get("business_tags")[:5], ensure_ascii=False)
                                        except Exception:
                                            company["business_tags"] = ""

                            expected_phone = phone or rule_phone or None
                            expected_addr = found_address or addr or rule_address or ""
                            expected_addr = normalize_address(expected_addr) or ""
                            verifiable_addr = expected_addr if is_address_verifiable(expected_addr) else None
                            quick_verify_result = quick_verify_from_docs(expected_phone, verifiable_addr)
                            verify_result = dict(quick_verify_result)
                            verify_result_source = "docs" if any(verify_result.values()) else "skip"
                            require_phone = bool(expected_phone)
                            require_addr = bool(verifiable_addr)
                            # AI由来の値で docs 根拠が無い場合、online verify を強制しない（遅延と誤爆を抑える）
                            if phone_source.startswith("ai") and expected_phone and not bool(quick_verify_result.get("phone_ok")):
                                require_phone = False
                            if address_source.startswith("ai") and verifiable_addr and not bool(quick_verify_result.get("address_ok")):
                                require_addr = False
                            if verify_result_source == "skip":
                                log.info(
                                    "[%s] verify skip: expected_phone=%s expected_addr=%s verifiable_addr=%s",
                                    cid,
                                    bool(expected_phone),
                                    bool(expected_addr),
                                    bool(verifiable_addr),
                                )
                            need_online_verify = (
                                not timed_out
                                and homepage
                                and (require_phone or require_addr)
                                and ((require_phone and not verify_result.get("phone_ok")) or (require_addr and not verify_result.get("address_ok")))
                                and not over_after_official()
                                and not over_deep_phase_deadline()
                            )
                            if need_online_verify:
                                try:
                                    verify_result = await asyncio.wait_for(
                                        scraper.verify_on_site(
                                            homepage,
                                            expected_phone,
                                            verifiable_addr,
                                            fetch_limit=5,
                                        ),
                                        timeout=bulk_timeout(PAGE_FETCH_TIMEOUT_SEC, 5, slow=True),
                                    )
                                    verify_result_source = "online"
                                except Exception:
                                    log.warning("[%s] verify_on_site 失敗", cid, exc_info=True)
                                    verify_result = quick_verify_result
                                    verify_result_source = "docs"

                            phone_ok = bool(verify_result.get("phone_ok"))
                            addr_ok = bool(verify_result.get("address_ok"))
                            required = int(require_phone) + int(require_addr)
                            matches = int(phone_ok) + int(addr_ok)
                            evidence_ok = (not require_phone or phone_ok) and (not require_addr or addr_ok)
                            if required == 0:
                                confidence = max(confidence or 0.0, 0.6)
                            elif evidence_ok:
                                confidence = 1.0 if required == 2 else 0.85
                            elif matches >= 1:
                                confidence = 0.75
                            else:
                                confidence = 0.45

                            # 公式フラグは「公式らしさ」の判定を優先し、verifyは追加根拠として扱う。
                            # ただし、電話/住所ともに具体的な期待値があるのに両方拾えない場合は疑わしいので降格する。
                            # 逆に、AI未確定/ブランドドメイン等で公式フラグが立たない場合でも、
                            # verify で期待値の一致が取れたなら「公式の根拠」として昇格させる（誤除外を減らす）。
                            if homepage and homepage_official_flag == 0 and required > 0 and matches >= 1:
                                ai_negative_strong = False
                                try:
                                    rec = selected_candidate_record or {}
                                    aj = rec.get("ai_judge") if isinstance(rec.get("ai_judge"), dict) else None
                                    if aj:
                                        ai_is = aj.get("is_official_site")
                                        if ai_is is None:
                                            ai_is = aj.get("is_official")
                                        conf = aj.get("official_confidence")
                                        if conf is None:
                                            conf = aj.get("confidence")
                                        try:
                                            conf_f = float(conf) if conf is not None else 0.0
                                        except Exception:
                                            conf_f = 0.0
                                        ai_negative_strong = bool(ai_is is False and conf_f >= AI_VERIFY_MIN_CONFIDENCE)
                                except Exception:
                                    ai_negative_strong = False

                                allow_verify_promote = bool(
                                    not ai_negative_strong
                                    and homepage
                                    and not _is_free_host(homepage)
                                    and (chosen_domain_score or 0) >= 3
                                )
                                if allow_verify_promote:
                                    homepage_official_flag = 1
                                    homepage_official_source = "verify_promote"
                                    force_review = False
                                    log.info(
                                        "[%s] verify_on_site一致のため公式フラグを昇格 (required=%s matches=%s domain=%s): %s",
                                        cid,
                                        required,
                                        matches,
                                        chosen_domain_score,
                                        homepage,
                                    )
                            if homepage and homepage_official_flag == 1 and required == 2 and matches == 0:
                                log.info("[%s] verify_on_siteで根拠不足のため公式フラグを降格 (required=%s matches=%s): %s", cid, required, matches, homepage)
                                homepage_official_flag = 0
                                homepage_official_source = "verify_fail"
                                force_review = True
                            elif homepage and homepage_official_flag == 1 and required > 0 and not evidence_ok:
                                force_review = True
                    else:
                        if urls:
                            log.info("[%s] 公式サイト候補を判別できず -> 未保存", cid)
                        else:
                            log.info("[%s] 有効なホームページ候補なし。", cid)
                        company["rep_name"] = scraper.clean_rep_name(company.get("rep_name")) or ""
                        company["description"] = company.get("description", "") or ""
                        confidence = 0.4

                except SkipCompany:
                    raise
                except HardTimeout:
                    raise
                except Exception:
                    fatal_error = True
                    raise
                finally:
                    if fatal_error:
                        pass
                    else:
                        if skip_company_reason:
                            save_no_homepage(skip_company_reason)
                            # SkipCompany を上位へ伝播させて次の会社へ（以降の補完/深掘りは行わない）
                            raise SkipCompany(skip_company_reason)
                        # 公式サイトが無い場合のみ、検索結果の非公式ページから連絡先を補完
                        if not homepage and (not phone or not found_address or not rep_name_val):
                            for url, data in fallback_cands:
                                # 企業DB/ディレクトリ/まとめサイト由来の代表者は誤爆が多いので採用しない
                                try:
                                    dir_hint = scraper._detect_directory_like(url or "", text="", html="")  # type: ignore[attr-defined]
                                    if bool(dir_hint.get("is_directory_like")) and int(dir_hint.get("directory_score") or 0) >= DIRECTORY_HARD_REJECT_SCORE:
                                        # phone/address は補完対象として残すが rep_name は見ない
                                        if not phone and data.get("phone_numbers"):
                                            before = best_rule_phone_score
                                            consider_rule_phone(
                                                list(data.get("phone_numbers") or []),
                                                url,
                                                page_type_per_url.get(url) or "OTHER",
                                            )
                                            if rule_phone and best_rule_phone_score > before:
                                                phone = rule_phone
                                                phone_source = "rule"
                                                src_phone = best_rule_phone_url or url
                                        if not found_address and data.get("addresses"):
                                            cand_addr = pick_best_address(addr, data["addresses"])
                                            if cand_addr:
                                                found_address = cand_addr
                                                address_source = "rule"
                                                src_addr = url
                                        continue
                                except Exception:
                                    pass
                                if not phone and data.get("phone_numbers"):
                                    before = best_rule_phone_score
                                    consider_rule_phone(list(data.get("phone_numbers") or []), url, page_type_per_url.get(url) or "OTHER")
                                    if rule_phone and best_rule_phone_score > before:
                                        phone = rule_phone
                                        phone_source = "rule"
                                        src_phone = best_rule_phone_url or url
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
                                        # no_homepage（非公式補完）の代表者は「構造化+役職ペア」由来のみ許可し、LABEL単独は弾く
                                        try:
                                            meta = _rep_candidate_meta(data.get("rep_names") or [], cand_rep)
                                            strong_structured = bool(meta.get("table") or meta.get("role") or meta.get("jsonld"))
                                            if not strong_structured:
                                                drop_reasons["rep"] = drop_reasons.get("rep") or "fallback_rep_not_structured"
                                                continue
                                        except Exception:
                                            drop_reasons["rep"] = drop_reasons.get("rep") or "fallback_rep_meta_error"
                                            continue
                                        pt_fallback = page_type_per_url.get(url) or "OTHER"
                                        rep_ok, rep_reason = _rep_candidate_ok(
                                            cand_rep,
                                            data.get("rep_names") or [],
                                            pt_fallback,
                                            url,
                                        )
                                        if rep_ok:
                                            rep_name_val = cand_rep
                                            src_rep = url
                                        else:
                                            drop_reasons["rep"] = drop_reasons.get("rep") or rep_reason
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
                        if ai_official_selected and normalized_found_address and (address_source or "") != "none":
                            company["address"] = normalized_found_address
                        csv_pref = CompanyScraper._extract_prefecture(addr) if addr else ""
                        hp_pref = CompanyScraper._extract_prefecture(normalized_found_address) if normalized_found_address else ""
                        pref_match = int(bool(csv_pref and hp_pref and csv_pref == hp_pref)) if (csv_pref and hp_pref) else None
                        csv_city_m = CITY_RE.search(addr or "")
                        hp_city_m = CITY_RE.search(normalized_found_address or "")
                        city_match = int(bool(csv_city_m and hp_city_m and csv_city_m.group(1) == hp_city_m.group(1))) if (csv_city_m and hp_city_m) else None
                        rep_name_val = scraper.clean_rep_name(rep_name_val) or ""
                        description_val = clean_description_value(sanitize_text_block(description_val))
                        # AI_DESCRIPTION_ALWAYS でも生成できなかった場合、既存の説明があれば保持（空欄化による情報欠落を防ぐ）
                        if AI_DESCRIPTION_ALWAYS and not description_val:
                            description_val = clean_description_value(company.get("description") or "")

                        # ---- description品質統一（追加AI呼び出し無し） ----
                        payloads_for_desc = []
                        try:
                            if info_dict:
                                info_payload = dict(info_dict)
                                if info_url and not info_payload.get("url"):
                                    info_payload["url"] = info_url
                                payloads_for_desc.append(info_payload)
                        except Exception:
                            pass
                        try:
                            for u, d in list((priority_docs or {}).items())[:6]:
                                if isinstance(d, dict):
                                    payloads_for_desc.append(
                                        {
                                            "url": u,
                                            "text": d.get("text", "") or "",
                                            "html": d.get("html", "") or "",
                                        }
                                    )
                        except Exception:
                            pass
                        try:
                            for u, d in list((related or {}).items())[:6]:
                                if isinstance(d, dict):
                                    payloads_for_desc.append(
                                        {
                                            "url": u,
                                            "text": d.get("text", "") or "",
                                            "html": d.get("html", "") or "",
                                        }
                                    )
                        except Exception:
                            pass

                        profile_payloads_for_desc: list[dict[str, Any]] = []
                        for p in payloads_for_desc:
                            if not isinstance(p, dict):
                                continue
                            p_url = str(p.get("url") or "").strip()
                            pt = str((page_type_per_url or {}).get(p_url) or "")
                            if (pt == "COMPANY_PROFILE") or _is_profile_like_url(p_url):
                                profile_payloads_for_desc.append(p)
                        payloads_for_desc_priority = profile_payloads_for_desc if profile_payloads_for_desc else payloads_for_desc

                        # 80〜160字（既定）に寄せる。短すぎ/長すぎの場合は既取得テキストから再構成する。
                        if (not description_val) or (len(description_val) < FINAL_DESCRIPTION_MIN_LEN) or (len(description_val) > FINAL_DESCRIPTION_MAX_LEN):
                            rebuilt = build_final_description_from_payloads(payloads_for_desc_priority)
                            if (not rebuilt) and (payloads_for_desc_priority is not payloads_for_desc):
                                rebuilt = build_final_description_from_payloads(payloads_for_desc)
                            if rebuilt and (
                                (not description_val)
                                or (len(description_val) < FINAL_DESCRIPTION_MIN_LEN)
                                or (len(description_val) > FINAL_DESCRIPTION_MAX_LEN)
                            ):
                                description_val = rebuilt

                        ai_desc_attempted = False
                        # ---- AIで毎回 description を生成（読みやすい事業説明を優先） ----
                        if (
                            AI_DESCRIPTION_ALWAYS
                            and AI_DESCRIPTION_ALWAYS_CALL
                            and USE_AI
                            and verifier is not None
                            and remaining_time_budget() > 0.3
                        ):
                            try:
                                blocks = _collect_business_text_blocks(payloads_for_desc_priority)
                                if (not blocks) and (payloads_for_desc_priority is not payloads_for_desc):
                                    blocks = _collect_business_text_blocks(payloads_for_desc)
                                if not blocks and info_dict:
                                    blocks.append(info_dict.get("text", "") or "")
                                shot = b""
                                try:
                                    maybe_shot = (info_dict or {}).get("screenshot")
                                    if isinstance(maybe_shot, (bytes, bytearray)) and maybe_shot:
                                        shot = bytes(maybe_shot)
                                except Exception:
                                    shot = b""
                                # 入力が空（本文もスクショもなし）の場合はAI生成を行わず欠損のままにする
                                if (not blocks) and (not shot):
                                    pass
                                else:
                                    desc_text = build_ai_text_payload(*blocks)
                                    ai_desc_attempted = True
                                    ai_used = 1
                                    if not ai_model:
                                        ai_model = AI_MODEL_NAME or ""
                                    generated = await asyncio.wait_for(
                                        verifier.generate_description(desc_text, shot, name, addr_raw, industry_hint=""),
                                        timeout=clamp_timeout(ai_call_timeout),
                                    )
                                    apply_ai_description(generated)
                            except Exception:
                                pass

                        # それでも description が欠損/規格外なら、最後に AI で description だけ再生成して穴を塞ぐ
                        if (
                            AI_DESCRIPTION_ALWAYS
                            and AI_DESCRIPTION_FALLBACK_CALL
                            and USE_AI
                            and verifier is not None
                            and (not timed_out)
                            and has_time_for_ai()
                            and (
                                (not description_val)
                                or (len(description_val) < FINAL_DESCRIPTION_MIN_LEN)
                                or (len(description_val) > FINAL_DESCRIPTION_MAX_LEN)
                            )
                        ):
                            try:
                                blocks = _collect_business_text_blocks(payloads_for_desc_priority)
                                if (not blocks) and (payloads_for_desc_priority is not payloads_for_desc):
                                    blocks = _collect_business_text_blocks(payloads_for_desc)
                                if not blocks and info_dict:
                                    blocks.append(info_dict.get("text", "") or "")
                                shot = b""
                                try:
                                    maybe_shot = (info_dict or {}).get("screenshot")
                                    if isinstance(maybe_shot, (bytes, bytearray)) and maybe_shot:
                                        shot = bytes(maybe_shot)
                                except Exception:
                                    shot = b""
                                # 入力が空なら生成しない（ハルシネーション防止）
                                if (not blocks) and (not shot):
                                    pass
                                else:
                                    prev_desc = description_val
                                    desc_text = build_ai_text_payload(*blocks)
                                    generated = await asyncio.wait_for(
                                        verifier.generate_description(desc_text, shot, name, addr_raw, industry_hint=""),
                                        timeout=clamp_timeout(ai_call_timeout),
                                    )
                                    apply_ai_description(generated)
                                    # 生成に失敗/不適合なら直前の値に戻す（空欄化で欠損を作らない）
                                    if (not description_val) or (len(description_val) < 10):
                                        description_val = prev_desc
                            except Exception:
                                pass

                        if description_val and len(description_val) > FINAL_DESCRIPTION_MAX_LEN:
                            description_val = _truncate_final_description(description_val, max_len=FINAL_DESCRIPTION_MAX_LEN)

                        # ---- 業種/タグを推定（推定値はヒントとして使い、最終保存は分類結果に限定） ----
                        # 業種判定で既存値を自己参照すると誤分類が自己強化されるため、毎回空から判定する。
                        industry_val = ""
                        industry_hint_val = ""
                        business_tags_val = (company.get("business_tags") or "").strip()
                        if INFER_INDUSTRY_ALWAYS:
                            blocks = _collect_business_text_blocks(payloads_for_desc_priority)
                            if (not blocks) and (payloads_for_desc_priority is not payloads_for_desc):
                                blocks = _collect_business_text_blocks(payloads_for_desc)
                            if description_val:
                                blocks.append(description_val)
                            inferred_industry, inferred_tags = infer_industry_and_business_tags(blocks)
                            if inferred_industry:
                                industry_hint_val = inferred_industry[:60]
                            elif not industry_hint_val:
                                industry_hint_val = "不明"
                            if inferred_tags:
                                try:
                                    business_tags_val = json.dumps(inferred_tags[:5], ensure_ascii=False)
                                except Exception:
                                    business_tags_val = ""
                            elif not business_tags_val:
                                business_tags_val = ""
                        # description を整形（業種の付与は分類後に実施）
                        description_val = clean_description_value(sanitize_text_block(description_val))
                        if description_val and len(description_val) > FINAL_DESCRIPTION_MAX_LEN:
                            description_val = _truncate_final_description(description_val, max_len=FINAL_DESCRIPTION_MAX_LEN)

                        # ---- 業種（AI優先・ruleは厳格フォールバック） ----
                        if (not INDUSTRY_CLASSIFY_AFTER_HOMEPAGE) and INDUSTRY_CLASSIFY_ENABLED and INDUSTRY_CLASSIFIER.loaded:
                            blocks = _collect_business_text_blocks(payloads_for_desc)
                            if description_val:
                                blocks.append(description_val)
                            if name:
                                blocks.append(name)
                            if business_tags_val:
                                try:
                                    if business_tags_val.strip().startswith("["):
                                        tags = json.loads(business_tags_val)
                                        if isinstance(tags, list):
                                            blocks.extend([str(t) for t in tags if t])
                                    else:
                                        blocks.append(business_tags_val)
                                except Exception:
                                    blocks.append(business_tags_val)
                            license_val = (company.get("license") or "").strip()
                            if license_val:
                                blocks.append(f"許認可: {license_val}"[:400])
                            desc_ev = (company.get("description_evidence") or "").strip()
                            if desc_ev:
                                blocks.append(f"根拠: {desc_ev}"[:400])

                            scores = INDUSTRY_CLASSIFIER.score_levels(blocks)
                            industry_result_ai = None
                            ai_industry_val = ""
                            ai_attempted = False
                            ai_available = (
                                INDUSTRY_AI_ENABLED
                                and verifier is not None
                                and hasattr(verifier, "judge_industry")
                                and getattr(verifier, "industry_prompt", None)
                            )

                            def _top_and_margin(score_map: dict[str, Any]) -> tuple[int, int]:
                                if not isinstance(score_map, dict) or not score_map:
                                    return 0, 0
                                vals: list[int] = []
                                for v in score_map.values():
                                    try:
                                        vals.append(int(v))
                                    except Exception:
                                        continue
                                if not vals:
                                    return 0, 0
                                vals.sort(reverse=True)
                                top = vals[0]
                                second = vals[1] if len(vals) > 1 else 0
                                return top, max(0, top - second)

                            def _ai_accepts(
                                ai_res: dict[str, Any],
                                final_candidate: dict[str, str],
                            ) -> bool:
                                try:
                                    conf = float(ai_res.get("confidence") or 0.0)
                                except Exception:
                                    conf = 0.0
                                if conf < INDUSTRY_AI_MIN_CONFIDENCE:
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

                            def _build_classification_candidates(score_map: dict[str, Any]) -> list[dict[str, str]]:
                                use_detail = bool(score_map.get("use_detail"))
                                level = "detail" if use_detail else "minor"
                                candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                    level,
                                    score_map,
                                    top_n=INDUSTRY_AI_TOP_N,
                                )
                                if (not candidates) and use_detail:
                                    candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                        "minor",
                                        score_map,
                                        top_n=INDUSTRY_AI_TOP_N,
                                    )
                                return candidates

                            def _match_ai_candidate(
                                ai_res: dict[str, Any],
                                candidates: list[dict[str, str]],
                            ) -> dict[str, str] | None:
                                if not candidates:
                                    return None
                                maj = str(ai_res.get("major_code") or "").strip()
                                mid = str(ai_res.get("middle_code") or "").strip()
                                mino = str(ai_res.get("minor_code") or "").strip()
                                if maj or mid or mino:
                                    for c in candidates:
                                        if mino and c.get("minor_code") != mino:
                                            continue
                                        if mid and c.get("middle_code") != mid:
                                            continue
                                        if maj and c.get("major_code") != maj:
                                            continue
                                        return c
                                ai_minor_name = str(ai_res.get("minor_name") or "").strip()
                                if ai_minor_name:
                                    norm_ai_minor = INDUSTRY_CLASSIFIER.taxonomy._normalize(ai_minor_name)
                                    for c in candidates:
                                        if INDUSTRY_CLASSIFIER.taxonomy._normalize(c.get("minor_name", "")) == norm_ai_minor:
                                            return c
                                return None

                            ai_text_parts = []
                            if description_val:
                                ai_text_parts.append(description_val)
                            for b in blocks:
                                if not b:
                                    continue
                                if description_val and b == description_val:
                                    continue
                                ai_text_parts.append(b)
                            ai_text = "\n".join(ai_text_parts) if ai_text_parts else "\n".join(blocks)
                            class_candidates = _build_classification_candidates(scores)

                            if ai_available and class_candidates and has_time_for_ai():
                                ai_attempted = True
                                try:
                                    ai_res = await asyncio.wait_for(
                                        verifier.judge_industry(
                                            text=ai_text,
                                            company_name=name,
                                            candidates_text=INDUSTRY_CLASSIFIER.format_candidates_text(class_candidates),
                                        ),
                                        timeout=min(clamp_timeout(ai_call_timeout), 30.0),
                                    )
                                except Exception:
                                    ai_res = None
                                if isinstance(ai_res, dict):
                                    matched = _match_ai_candidate(ai_res, class_candidates)
                                    if matched and _ai_accepts(ai_res, matched):
                                        ai_industry_val = str(ai_res.get("industry") or "").strip()
                                        try:
                                            conf = float(ai_res.get("confidence") or 0.0)
                                        except Exception:
                                            conf = 0.0
                                        conf = max(0.0, min(1.0, conf))
                                        industry_result_ai = {
                                            "major_code": matched.get("major_code", ""),
                                            "major_name": matched.get("major_name", ""),
                                            "middle_code": matched.get("middle_code", ""),
                                            "middle_name": matched.get("middle_name", ""),
                                            "minor_code": matched.get("minor_code", ""),
                                            "minor_name": matched.get("minor_name", ""),
                                            "confidence": conf,
                                            "source": "ai",
                                        }

                            industry_result = industry_result_ai

                            lookup_name = ai_industry_val or industry_hint_val or industry_val
                            if (not industry_result) and INDUSTRY_NAME_LOOKUP_ENABLED and lookup_name:
                                try:
                                    exact_candidate = INDUSTRY_CLASSIFIER.resolve_exact_candidate_from_name(lookup_name)
                                except Exception:
                                    exact_candidate = None
                                if exact_candidate:
                                    industry_result = {
                                        "major_code": exact_candidate.get("major_code", ""),
                                        "major_name": exact_candidate.get("major_name", ""),
                                        "middle_code": exact_candidate.get("middle_code", ""),
                                        "middle_name": exact_candidate.get("middle_name", ""),
                                        "minor_code": exact_candidate.get("minor_code", ""),
                                        "minor_name": exact_candidate.get("minor_name", ""),
                                        "confidence": 0.7,
                                        "source": "name_exact",
                                    }

                            final_industry_name = ""
                            if industry_result:
                                final_industry_name = (
                                    industry_result.get("minor_name")
                                    or industry_result.get("middle_name")
                                    or industry_result.get("major_name")
                                    or ""
                                )
                            industry_val = (final_industry_name[:60] if final_industry_name else "不明")

                            if industry_result:
                                # If AI returns detail code, map to minor representative
                                minor_item_code = ""
                                minor_item_name = ""
                                minor_code_raw = str(industry_result.get("minor_code") or "").strip()
                                if minor_code_raw and minor_code_raw in INDUSTRY_CLASSIFIER.taxonomy.detail_names:
                                    (
                                        major_code,
                                        major_name,
                                        middle_code,
                                        middle_name,
                                        minor_code,
                                        minor_name,
                                        detail_code,
                                        detail_name,
                                    ) = INDUSTRY_CLASSIFIER.taxonomy.resolve_detail_hierarchy(minor_code_raw)
                                    if major_name:
                                        industry_major = major_name
                                    if middle_name:
                                        industry_middle = middle_name
                                    if minor_name:
                                        industry_minor = minor_name
                                    if major_code:
                                        industry_major_code = major_code
                                    if middle_code:
                                        industry_middle_code = middle_code
                                    if minor_code:
                                        industry_minor_code = minor_code
                                    minor_item_code = detail_code
                                    minor_item_name = detail_name
                                else:
                                    industry_major = industry_result.get("major_name", "") or industry_major
                                    industry_middle = industry_result.get("middle_name", "") or industry_middle
                                    industry_minor = industry_result.get("minor_name", "") or industry_minor
                                    industry_major_code = industry_result.get("major_code", "") or industry_major_code
                                    industry_middle_code = industry_result.get("middle_code", "") or industry_middle_code
                                    industry_minor_code = industry_result.get("minor_code", "") or industry_minor_code
                                if minor_item_code:
                                    company["industry_minor_item_code"] = minor_item_code
                                    company["industry_minor_item"] = minor_item_name
                                elif industry_minor_code and industry_minor:
                                    # Fallback: always populate minor_item with the resolved minor.
                                    company["industry_minor_item_code"] = industry_minor_code
                                    company["industry_minor_item"] = industry_minor
                                industry_class_source = industry_result.get("source", "") or industry_class_source
                                try:
                                    industry_class_confidence = float(industry_result.get("confidence") or industry_class_confidence)
                                except Exception:
                                    pass
                            else:
                                if not industry_class_source:
                                    industry_class_source = "unclassified"
                                industry_class_confidence = 0.0
                                if not company.get("industry_minor_item"):
                                    company["industry_minor_item_code"] = ""
                                    company["industry_minor_item"] = "不明"

                        # description は「何をしているどの会社か」が分かる形に整形（業種は含めない）
                        description_val = clean_description_value(sanitize_text_block(description_val))
                        if description_val and len(description_val) > FINAL_DESCRIPTION_MAX_LEN:
                            description_val = _truncate_final_description(description_val, max_len=FINAL_DESCRIPTION_MAX_LEN)

                        # ---- お問い合わせURL（同一ステップで判定→保存） ----
                        if CONTACT_URL_IN_MAIN and homepage and (CONTACT_URL_FORCE or not contact_url):
                            try:
                                url, score, source, reason = await _pick_contact_url(scraper, homepage)
                                ai_verdict = ""
                                ai_conf = 0.0
                                ai_reason = ""
                                if url and CONTACT_URL_AI_ENABLED and verifier is not None and verifier.model and verifier.contact_form_prompt:
                                    try:
                                        page = await scraper.get_page_info(url, allow_slow=False)
                                        text = page.get("text", "") or ""
                                        html = page.get("html", "") or ""
                                        signals = _build_ai_signals(homepage, url, html)
                                        ai_result = await verifier.judge_contact_form(
                                            text=text,
                                            company_name=name,
                                            homepage=homepage,
                                            url=url,
                                            signals=signals,
                                        )
                                    except Exception:
                                        ai_result = None
                                    if ai_result:
                                        verdict_val = ai_result.get("is_official_contact_form")
                                        ai_conf = float(ai_result.get("confidence") or 0.0)
                                        ai_reason = str(ai_result.get("reason") or "")
                                        if verdict_val is True and ai_conf >= CONTACT_URL_AI_MIN_CONFIDENCE:
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

                                if url:
                                    status = "success"
                                else:
                                    if ai_verdict == "not_official":
                                        status = "ai_not_official"
                                    elif ai_verdict == "unsure":
                                        status = "ai_unsure"
                                    else:
                                        status = "no_contact"

                                contact_url = url or ""
                                contact_url_source = source or ""
                                contact_url_score = float(score or 0.0)
                                contact_url_reason = reason or ""
                                contact_url_ai_verdict = ai_verdict or ""
                                contact_url_ai_confidence = float(ai_conf or 0.0)
                                contact_url_ai_reason = ai_reason or ""
                                contact_url_status = status
                                contact_url_checked_at = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                            except Exception:
                                pass
                        listing_val = clean_listing_value(listing_val)
                        capital_val = ai_normalize_amount(capital_val) or clean_amount_value(capital_val)
                        revenue_val = ai_normalize_amount(revenue_val) or clean_amount_value(revenue_val)
                        profit_val = ai_normalize_amount(profit_val) or clean_amount_value(profit_val)
                        employees_val = clean_employee_value(employees_val)
                        fiscal_val = clean_fiscal_month(fiscal_val)
                        founded_val = clean_founded_year(founded_val)
                        homepage = clean_homepage_url(homepage)
                        if not homepage:
                            homepage_official_flag = 0
                            homepage_official_source = ""
                            homepage_official_score = 0.0
                        normalized_phone = normalize_phone(phone)
                        if normalized_phone:
                            phone = normalized_phone
                        else:
                            phone = ""
                            phone_source = "none"
                            src_phone = ""
                        if drop_details_by_url:
                            try:
                                drop_reasons["_by_url"] = {k: drop_details_by_url[k] for k in list(drop_details_by_url.keys())[:3]}
                            except Exception:
                                pass
                        try:
                            log.info(
                                "[%s] final_decision homepage=%s official=%s(%s score=%.1f domain=%s) phone=%s(%s) address=%s(%s) rep=%s(%s)",
                                cid,
                                homepage or "",
                                homepage_official_flag,
                                homepage_official_source,
                                float(homepage_official_score or 0.0),
                                chosen_domain_score,
                                phone or "",
                                phone_source or "",
                                normalized_found_address or "",
                                address_source or "",
                                rep_name_val or "",
                                (src_rep or ""),
                            )
                        except Exception:
                            pass
                        company.update({
                            "homepage": homepage,
                            "phone": phone or "",
                            "found_address": normalized_found_address,
                            "rep_name": rep_name_val,
                            "description": description_val,
                            "industry": industry_val,
                            "industry_major_code": industry_major_code,
                            "industry_major": industry_major,
                            "industry_middle_code": industry_middle_code,
                            "industry_middle": industry_middle,
                            "industry_minor_code": industry_minor_code,
                            "industry_minor": industry_minor,
                            "industry_class_source": industry_class_source,
                            "industry_class_confidence": industry_class_confidence,
                            "contact_url": contact_url,
                            "contact_url_source": contact_url_source,
                            "contact_url_score": contact_url_score,
                            "contact_url_reason": contact_url_reason,
                            "contact_url_checked_at": contact_url_checked_at,
                            "contact_url_ai_verdict": contact_url_ai_verdict,
                            "contact_url_ai_confidence": contact_url_ai_confidence,
                            "contact_url_ai_reason": contact_url_ai_reason,
                            "contact_url_status": contact_url_status,
                            "business_tags": business_tags_val,
                            "listing": listing_val,
                            "revenue": revenue_val,
                            "profit": profit_val,
                            "capital": capital_val,
                            "employees": employees_val,
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
                            "address_confidence": address_ai_confidence,
                            "address_evidence": address_ai_evidence,
                            "deep_pages_visited": int(deep_pages_visited or 0),
                            "deep_fetch_count": int(deep_fetch_count or 0),
                            "deep_fetch_failures": int(deep_fetch_failures or 0),
                            "deep_skip_reason": deep_skip_reason or "",
                            "deep_urls_visited": json.dumps(list(deep_urls_visited or [])[:5], ensure_ascii=False),
                            "deep_phone_candidates": int(deep_phone_candidates or 0),
                            "deep_address_candidates": int(deep_address_candidates or 0),
                            "deep_rep_candidates": int(deep_rep_candidates or 0),
                            "top3_urls": json.dumps(list(urls or [])[:3], ensure_ascii=False),
                                "exclude_reasons": json.dumps(exclude_reasons or {}, ensure_ascii=False),
                                "skip_reason": (company.get("skip_reason") or "").strip(),
                                # 参照URL（どのページを“真”として扱ったか）。会社概要/会社案内/企業情報系を最優先で選ぶ。
                                "reference_homepage": _pick_reference_homepage(page_type_per_url),
                                "provisional_homepage": (provisional_homepage or forced_provisional_homepage or ""),
                                "provisional_reason": ((company.get("provisional_reason") or "").strip() or forced_provisional_reason or ""),
                                "final_homepage": (homepage or ""),
                            "deep_enabled": int(bool(deep_pages_visited or deep_fetch_count)),
                            "deep_stop_reason": (deep_stop_reason or deep_skip_reason or ""),
                            "timeout_stage": timeout_stage or "",
                            "page_type_per_url": json.dumps(page_type_per_url or {}, ensure_ascii=False),
                            "extracted_candidates_count": json.dumps(
                                {
                                    "phone": int(deep_phone_candidates or 0) + len((primary_cands or {}).get("phone_numbers") or []),
                                    "address": int(deep_address_candidates or 0) + len((primary_cands or {}).get("addresses") or []),
                                    "rep": int(deep_rep_candidates or 0) + len((primary_cands or {}).get("rep_names") or []),
                                },
                                ensure_ascii=False,
                            ),
                            "drop_reasons": json.dumps(drop_reasons or {}, ensure_ascii=False),
                            "pref_match": pref_match,
                            "city_match": city_match,
                        })

                        had_verify_target = bool(
                            (phone or rule_phone) or is_address_verifiable(found_address or rule_address or addr)
                        )

                        if REFERENCE_CHECKER:
                            accuracy_payload = REFERENCE_CHECKER.evaluate(company)
                            if accuracy_payload:
                                company.update(accuracy_payload)

                        # ---- homepage保存ポリシー（誤保存を防ぐ最終ゲート） ----
                        # final_decision 時点の候補は保持してDBに残す（後段のポリシーで落ちても痕跡を残す）
                        final_homepage_candidate = homepage or ""
                        # 1) provisional_* の弱いものを落とす（既定: 有効）
                        if APPLY_PROVISIONAL_HOMEPAGE_POLICY and homepage:
                            decision = apply_provisional_homepage_policy(
                                homepage=homepage,
                                homepage_official_flag=int(homepage_official_flag or 0),
                                homepage_official_source=str(homepage_official_source or ""),
                                homepage_official_score=float(homepage_official_score or 0.0),
                                chosen_domain_score=int(chosen_domain_score or 0),
                                provisional_host_token=bool(provisional_host_token),
                                provisional_name_present=bool(provisional_name_present),
                                provisional_address_ok=bool(provisional_address_ok),
                                provisional_ai_hint=bool(provisional_ai_hint),
                                provisional_profile_hit=bool(provisional_profile_hit),
                                provisional_evidence_score=int(provisional_evidence_score or 0),
                            )
                            if decision.dropped:
                                log.info(
                                    "[%s] provisional policy dropped (domain_score=%s host_token=%s name=%s addr=%s): %s",
                                    cid,
                                    chosen_domain_score,
                                    provisional_host_token,
                                    provisional_name_present,
                                    provisional_address_ok,
                                    homepage,
                                )
                            homepage = decision.homepage
                            homepage_official_flag = decision.homepage_official_flag
                            homepage_official_source = decision.homepage_official_source
                            homepage_official_score = decision.homepage_official_score
                            chosen_domain_score = decision.chosen_domain_score

                        # 2) final_homepage は「final_decision 時点の候補URL」を保持（homepage が空でも残す）

                        # 3) official と確定できない場合は homepage を空欄にし、候補は provisional_homepage に退避
                        provisional_store = (provisional_homepage or forced_provisional_homepage or "").strip()
                        if homepage and int(homepage_official_flag or 0) != 1:
                            source = str(homepage_official_source or "")
                            is_provisional_source = source.startswith("provisional") or source.startswith("ai_provisional")
                            if REQUIRE_OFFICIAL_HOMEPAGE or (is_provisional_source and not SAVE_PROVISIONAL_HOMEPAGE):
                                if not provisional_store:
                                    provisional_store = homepage
                                    if not (company.get("provisional_reason") or "").strip():
                                        company["provisional_reason"] = "non_official_candidate"
                                homepage = ""
                                homepage_official_flag = 0
                                homepage_official_source = ""
                                homepage_official_score = 0.0
                                chosen_domain_score = 0

                        # 4) disallowed host は official でも homepage に保存しない（DB汚染を防ぐ）
                        if homepage and int(homepage_official_flag or 0) == 1 and CompanyScraper.is_disallowed_official_host(homepage):
                            if not provisional_store:
                                provisional_store = homepage
                                if not (company.get("provisional_reason") or "").strip():
                                    company["provisional_reason"] = "disallowed_official_host"
                            homepage = ""
                            homepage_official_flag = 0
                            homepage_official_source = ""
                            homepage_official_score = 0.0
                            chosen_domain_score = 0

                        # 5) homepageモデルv2: official と候補を分離して保持
                        official_homepage = homepage if (homepage and int(homepage_official_flag or 0) == 1) else ""
                        alt_homepage = ""
                        alt_homepage_type = ""
                        if not official_homepage:
                            alt_homepage = (provisional_store or final_homepage_candidate or "").strip()
                            if alt_homepage:
                                if CompanyScraper.is_disallowed_official_host(alt_homepage):
                                    alt_homepage_type = "platform"
                                elif _is_free_host(alt_homepage):
                                    alt_homepage_type = "free_host"
                                else:
                                    alt_homepage_type = "candidate"

                        # company dict も同期してDB/CSVと状態が一致するようにする
                        company["homepage"] = homepage
                        company["homepage_official_flag"] = int(homepage_official_flag or 0)
                        company["homepage_official_source"] = str(homepage_official_source or "")
                        company["homepage_official_score"] = float(homepage_official_score or 0.0)
                        company["provisional_homepage"] = provisional_store
                        company["official_homepage"] = official_homepage
                        company["alt_homepage"] = alt_homepage
                        company["alt_homepage_type"] = alt_homepage_type
                        company["final_homepage"] = final_homepage_candidate

                        # ---- 業種（final_homepage確定後に最終判定） ----
                        if INDUSTRY_CLASSIFY_AFTER_HOMEPAGE and INDUSTRY_CLASSIFY_ENABLED and INDUSTRY_CLASSIFIER.loaded:
                            target_urls = [
                                official_homepage,
                                final_homepage_candidate,
                                alt_homepage,
                                company.get("reference_homepage") or "",
                            ]

                            def _normalized_host(url_val: str) -> str:
                                try:
                                    host_val = (urlparse(url_val).netloc or "").lower().split(":")[0]
                                except Exception:
                                    host_val = ""
                                if host_val.startswith("www."):
                                    host_val = host_val[4:]
                                return host_val

                            def _top_and_margin(score_map: dict[str, Any]) -> tuple[int, int]:
                                if not isinstance(score_map, dict) or not score_map:
                                    return 0, 0
                                vals: list[int] = []
                                for v in score_map.values():
                                    try:
                                        vals.append(int(v))
                                    except Exception:
                                        continue
                                if not vals:
                                    return 0, 0
                                vals.sort(reverse=True)
                                top = vals[0]
                                second = vals[1] if len(vals) > 1 else 0
                                return top, max(0, top - second)

                            target_hosts = {_normalized_host(u) for u in target_urls if isinstance(u, str) and u.strip()}
                            target_hosts = {h for h in target_hosts if h}

                            payloads_for_industry: list[dict[str, Any]] = []
                            for p in payloads_for_desc:
                                if not isinstance(p, dict):
                                    continue
                                p_url = str(p.get("url") or "").strip()
                                p_host = _normalized_host(p_url) if p_url else ""
                                if target_hosts:
                                    if p_host and p_host in target_hosts:
                                        payloads_for_industry.append(p)
                                else:
                                    payloads_for_industry.append(p)
                            if not payloads_for_industry:
                                payloads_for_industry = [p for p in payloads_for_desc if isinstance(p, dict)]

                            # 業種判定は「会社概要系ページ」を優先し、description / business_tags を主材料にする。
                            profile_payloads: list[dict[str, Any]] = []
                            non_profile_payloads: list[dict[str, Any]] = []
                            for p in payloads_for_industry:
                                p_url = str((p or {}).get("url") or "").strip()
                                pt = str((page_type_per_url or {}).get(p_url) or "")
                                if (pt == "COMPANY_PROFILE") or _is_profile_like_url(p_url):
                                    profile_payloads.append(p)
                                else:
                                    non_profile_payloads.append(p)

                            primary_payloads = profile_payloads if profile_payloads else payloads_for_industry
                            primary_blocks = _collect_business_text_blocks(primary_payloads)
                            secondary_blocks = _collect_business_text_blocks(non_profile_payloads) if profile_payloads else []
                            same_host_evidence_chars = sum(len(b) for b in primary_blocks if isinstance(b, str))

                            parsed_business_tags: list[str] = []
                            if business_tags_val:
                                try:
                                    if business_tags_val.strip().startswith("["):
                                        tags = json.loads(business_tags_val)
                                        if isinstance(tags, list):
                                            parsed_business_tags = [str(t).strip() for t in tags if str(t).strip()]
                                    else:
                                        parsed_business_tags = [business_tags_val.strip()]
                                except Exception:
                                    parsed_business_tags = [business_tags_val.strip()]

                            industry_blocks: list[str] = []
                            if description_val:
                                industry_blocks.append(description_val)
                            if parsed_business_tags:
                                industry_blocks.extend(parsed_business_tags)
                            industry_blocks.extend(primary_blocks)
                            # 会社概要系の材料が薄い場合のみ、他ページを少量補助利用する。
                            # 閾値を下げ、補助件数も2件に抑えて過剰なノイズを防ぎつつ本文量を確保。
                            if secondary_blocks and same_host_evidence_chars < 180:
                                industry_blocks.extend(secondary_blocks[:2])
                            if name:
                                industry_blocks.append(name)
                            if industry_hint_val:
                                industry_blocks.append(industry_hint_val)
                            if (not parsed_business_tags) and business_tags_val:
                                industry_blocks.append(business_tags_val)
                            license_val = (company.get("license") or "").strip()
                            if license_val:
                                industry_blocks.append(f"許認可: {license_val}"[:400])
                            desc_ev = (company.get("description_evidence") or "").strip()
                            if desc_ev:
                                industry_blocks.append(f"根拠: {desc_ev}"[:400])

                            dedup_industry_blocks: list[str] = []
                            seen_industry_blocks: set[str] = set()
                            for block in industry_blocks:
                                bb = (block or "").strip()
                                if not bb or bb in seen_industry_blocks:
                                    continue
                                seen_industry_blocks.add(bb)
                                dedup_industry_blocks.append(bb)
                            industry_blocks = dedup_industry_blocks[:20]

                            # description/tags は判定スコアにもう一段反映して優先度を高める。
                            score_blocks = list(industry_blocks)
                            if description_val:
                                score_blocks.append(description_val)
                            if parsed_business_tags:
                                score_blocks.extend(parsed_business_tags)

                            scores = INDUSTRY_CLASSIFIER.score_levels(score_blocks)
                            use_detail = bool(scores.get("use_detail"))
                            industry_result_post: dict[str, Any] | None = None
                            detail_result_post: dict[str, Any] | None = None
                            ai_industry_name = ""
                            ai_attempted = False
                            ai_available = (
                                INDUSTRY_AI_ENABLED
                                and verifier is not None
                                and hasattr(verifier, "judge_industry")
                                and getattr(verifier, "industry_prompt", None)
                            )
                            ai_text_blocks: list[str] = []
                            if description_val:
                                ai_text_blocks.append(f"[PRIORITY_DESCRIPTION]{description_val}")
                            for tag in parsed_business_tags:
                                ai_text_blocks.append(f"[PRIORITY_TAG]{tag}")
                            ai_text_blocks.extend([b for b in score_blocks if b])
                            # 重複を抑えて、優先入力を先頭に保つ
                            ai_text_dedup: list[str] = []
                            ai_text_seen: set[str] = set()
                            for block in ai_text_blocks:
                                bb = (block or "").strip()
                                if not bb or bb in ai_text_seen:
                                    continue
                                ai_text_seen.add(bb)
                                ai_text_dedup.append(bb)
                            ai_text = "\n".join(ai_text_dedup)

                            def _ai_confidence(ai_res: dict[str, Any]) -> float:
                                try:
                                    conf = float(ai_res.get("confidence") or 0.0)
                                except Exception:
                                    conf = 0.0
                                return max(0.0, min(1.0, conf))

                            def _match_candidate(
                                ai_res: dict[str, Any],
                                candidates: list[dict[str, str]],
                                level: str,
                            ) -> dict[str, str] | None:
                                if not candidates:
                                    return None

                                maj = str(ai_res.get("major_code") or "").strip()
                                mid = str(ai_res.get("middle_code") or "").strip()
                                mino = str(ai_res.get("minor_code") or "").strip()
                                has_codes = bool(maj or mid or mino)
                                if has_codes:
                                    for cand in candidates:
                                        if maj and cand.get("major_code") != maj:
                                            continue
                                        if level in {"middle", "minor", "detail"} and mid and cand.get("middle_code") != mid:
                                            continue
                                        if level in {"minor", "detail"} and mino and cand.get("minor_code") != mino:
                                            continue
                                        return cand

                                if level == "major":
                                    ai_name = str(ai_res.get("major_name") or ai_res.get("industry") or "").strip()
                                    if ai_name:
                                        norm_name = INDUSTRY_CLASSIFIER.taxonomy._normalize(ai_name)
                                        for cand in candidates:
                                            if INDUSTRY_CLASSIFIER.taxonomy._normalize(cand.get("major_name", "")) == norm_name:
                                                return cand
                                elif level == "middle":
                                    ai_name = str(ai_res.get("middle_name") or "").strip()
                                    if ai_name:
                                        norm_name = INDUSTRY_CLASSIFIER.taxonomy._normalize(ai_name)
                                        for cand in candidates:
                                            if INDUSTRY_CLASSIFIER.taxonomy._normalize(cand.get("middle_name", "")) == norm_name:
                                                return cand
                                else:
                                    ai_name = str(ai_res.get("minor_name") or "").strip()
                                    if ai_name:
                                        norm_name = INDUSTRY_CLASSIFIER.taxonomy._normalize(ai_name)
                                        for cand in candidates:
                                            if INDUSTRY_CLASSIFIER.taxonomy._normalize(cand.get("minor_name", "")) == norm_name:
                                                return cand
                                return None

                            def _ai_accepts(
                                ai_res: dict[str, Any],
                                matched: dict[str, str],
                                *,
                                min_confidence: float,
                            ) -> bool:
                                if _ai_confidence(ai_res) < min_confidence:
                                    return False
                                if str(ai_res.get("human_review") or "").strip().lower() in {"true", "1", "yes"}:
                                    return False
                                regulated_majors = {"医療，福祉", "建設業", "金融業，保険業"}
                                major_name = (matched.get("major_name") or "").strip()
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

                            async def _judge_stage(
                                level: str,
                                candidates: list[dict[str, str]],
                                *,
                                min_confidence: float,
                            ) -> tuple[dict[str, str] | None, dict[str, Any] | None]:
                                nonlocal ai_attempted
                                if not (ai_available and candidates and has_time_for_ai()):
                                    return None, None
                                ai_attempted = True
                                try:
                                    ai_res = await asyncio.wait_for(
                                        verifier.judge_industry(
                                            text=ai_text,
                                            company_name=name,
                                            candidates_text=INDUSTRY_CLASSIFIER.format_candidates_text(candidates),
                                        ),
                                        timeout=min(clamp_timeout(ai_call_timeout), 30.0),
                                    )
                                except Exception:
                                    return None, None
                                if not isinstance(ai_res, dict):
                                    return None, None
                                matched = _match_candidate(ai_res, candidates, level)
                                if not matched:
                                    return None, ai_res
                                if not _ai_accepts(ai_res, matched, min_confidence=min_confidence):
                                    return None, ai_res
                                return matched, ai_res

                            major_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                "major",
                                scores,
                                top_n=max(INDUSTRY_AI_TOP_N, 10),
                            )
                            if (not major_candidates) and industry_hint_val:
                                hint_candidates = INDUSTRY_CLASSIFIER.build_candidates_from_industry_name(
                                    industry_hint_val,
                                    top_n=max(INDUSTRY_AI_TOP_N, 24),
                                )
                                major_map: dict[str, dict[str, str]] = {}
                                for cand in hint_candidates:
                                    maj = str(cand.get("major_code") or "").strip()
                                    if not maj:
                                        continue
                                    if maj in major_map:
                                        continue
                                    major_map[maj] = {
                                        "major_code": maj,
                                        "major_name": str(cand.get("major_name") or ""),
                                        "middle_code": "",
                                        "middle_name": "",
                                        "minor_code": "",
                                        "minor_name": "",
                                    }
                                major_candidates = list(major_map.values())[: max(INDUSTRY_AI_TOP_N, 10)]

                            picked_major, _major_ai_res = await _judge_stage(
                                "major",
                                major_candidates,
                                min_confidence=INDUSTRY_AI_MIN_CONFIDENCE,
                            )
                            picked_minor: dict[str, str] | None = None
                            minor_ai_res: dict[str, Any] | None = None
                            picked_middle: dict[str, str] | None = None

                            if picked_major:
                                major_code_val = str(picked_major.get("major_code") or "")
                                full_minor_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                    "minor",
                                    scores,
                                    top_n=2000,
                                    major_code=major_code_val,
                                )
                                if (not full_minor_candidates) and industry_hint_val:
                                    hint_minor = INDUSTRY_CLASSIFIER.build_candidates_from_industry_name(
                                        industry_hint_val,
                                        top_n=300,
                                    )
                                    dedup_minor: dict[str, dict[str, str]] = {}
                                    for cand in hint_minor:
                                        if str(cand.get("major_code") or "") != major_code_val:
                                            continue
                                        min_code = str(cand.get("minor_code") or "")
                                        if not min_code or min_code in dedup_minor:
                                            continue
                                        dedup_minor[min_code] = cand
                                    full_minor_candidates = list(dedup_minor.values())

                                if full_minor_candidates:
                                    if len(full_minor_candidates) <= INDUSTRY_MINOR_DIRECT_MAX_CANDIDATES:
                                        direct_minor_candidates = full_minor_candidates[: max(INDUSTRY_AI_TOP_N, len(full_minor_candidates))]
                                        picked_minor, minor_ai_res = await _judge_stage(
                                            "minor",
                                            direct_minor_candidates,
                                            min_confidence=INDUSTRY_AI_MIN_CONFIDENCE,
                                        )
                                    else:
                                        middle_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                            "middle",
                                            scores,
                                            top_n=max(INDUSTRY_AI_TOP_N, 12),
                                            major_code=major_code_val,
                                        )
                                        picked_middle, _middle_ai_res = await _judge_stage(
                                            "middle",
                                            middle_candidates,
                                            min_confidence=INDUSTRY_AI_MIN_CONFIDENCE,
                                        )
                                        if picked_middle:
                                            middle_code_val = str(picked_middle.get("middle_code") or "")
                                            scoped_minor_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                                "minor",
                                                scores,
                                                top_n=max(INDUSTRY_AI_TOP_N, 30),
                                                major_code=major_code_val,
                                                middle_code=middle_code_val,
                                            )
                                            if not scoped_minor_candidates:
                                                scoped_minor_candidates = [
                                                    cand
                                                    for cand in full_minor_candidates
                                                    if str(cand.get("middle_code") or "") == middle_code_val
                                                ][: max(INDUSTRY_AI_TOP_N, 30)]
                                            picked_minor, minor_ai_res = await _judge_stage(
                                                "minor",
                                                scoped_minor_candidates,
                                                min_confidence=INDUSTRY_AI_MIN_CONFIDENCE,
                                            )
                                        if not picked_minor:
                                            fallback_minor_candidates = full_minor_candidates[: max(INDUSTRY_AI_TOP_N, 30)]
                                            picked_minor, minor_ai_res = await _judge_stage(
                                                "minor",
                                                fallback_minor_candidates,
                                                min_confidence=INDUSTRY_AI_MIN_CONFIDENCE,
                                            )

                            if picked_minor and isinstance(minor_ai_res, dict):
                                ai_industry_name = str(minor_ai_res.get("industry") or "").strip()
                                industry_result_post = {
                                    "major_code": str(picked_minor.get("major_code") or ""),
                                    "major_name": str(picked_minor.get("major_name") or ""),
                                    "middle_code": str(picked_minor.get("middle_code") or ""),
                                    "middle_name": str(picked_minor.get("middle_name") or ""),
                                    "minor_code": str(picked_minor.get("minor_code") or ""),
                                    "minor_name": str(picked_minor.get("minor_name") or ""),
                                    "confidence": _ai_confidence(minor_ai_res),
                                    "source": "ai_final_homepage",
                                }

                                if use_detail and same_host_evidence_chars >= INDUSTRY_DETAIL_MIN_EVIDENCE_CHARS:
                                    detail_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                        "detail",
                                        scores,
                                        top_n=max(INDUSTRY_AI_TOP_N, 20),
                                        major_code=str(picked_minor.get("major_code") or ""),
                                        middle_code=str(picked_minor.get("middle_code") or ""),
                                        minor_code=str(picked_minor.get("minor_code") or ""),
                                    )
                                    picked_detail, detail_ai_res = await _judge_stage(
                                        "detail",
                                        detail_candidates,
                                        min_confidence=INDUSTRY_DETAIL_MIN_CONFIDENCE,
                                    )
                                    if picked_detail and isinstance(detail_ai_res, dict):
                                        detail_result_post = {
                                            "major_code": str(picked_detail.get("major_code") or ""),
                                            "major_name": str(picked_detail.get("major_name") or ""),
                                            "middle_code": str(picked_detail.get("middle_code") or ""),
                                            "middle_name": str(picked_detail.get("middle_name") or ""),
                                            "minor_code": str(picked_detail.get("minor_code") or ""),
                                            "minor_name": str(picked_detail.get("minor_name") or ""),
                                            "confidence": _ai_confidence(detail_ai_res),
                                            "source": "ai_detail_optional",
                                        }

                            if (not industry_result_post) and INDUSTRY_NAME_LOOKUP_ENABLED:
                                lookup_name = ai_industry_name or industry_hint_val or industry_val
                                try:
                                    exact_candidate = INDUSTRY_CLASSIFIER.resolve_exact_candidate_from_name(lookup_name)
                                except Exception:
                                    exact_candidate = None
                                if exact_candidate:
                                    industry_result_post = {
                                        "major_code": exact_candidate.get("major_code", ""),
                                        "major_name": exact_candidate.get("major_name", ""),
                                        "middle_code": exact_candidate.get("middle_code", ""),
                                        "middle_name": exact_candidate.get("middle_name", ""),
                                        "minor_code": exact_candidate.get("minor_code", ""),
                                        "minor_name": exact_candidate.get("minor_name", ""),
                                        "confidence": 0.7,
                                        "source": "name_exact_post",
                                    }

                            # 低信頼なら tags を強いヒントにして再AI判定（精度補強）
                            if (
                                industry_result_post
                                and business_tags_val
                                and ai_available
                                and has_time_for_ai()
                                and float(industry_result_post.get("confidence") or 0.0) < 0.7
                            ):
                                tag_list: list[str] = []
                                try:
                                    if business_tags_val.strip().startswith("["):
                                        maybe = json.loads(business_tags_val)
                                        if isinstance(maybe, list):
                                            tag_list = [str(t).strip() for t in maybe if str(t).strip()]
                                    else:
                                        tag_list = [business_tags_val.strip()]
                                except Exception:
                                    tag_list = [business_tags_val.strip()]

                                if tag_list:
                                    cand_map: dict[str, dict[str, str]] = {}
                                    for tag in tag_list:
                                        for cand in INDUSTRY_CLASSIFIER.build_candidates_from_industry_name(
                                            tag, top_n=max(INDUSTRY_AI_TOP_N, 120)
                                        ):
                                            key = (
                                                str(cand.get("minor_code") or "")
                                                or str(cand.get("middle_code") or "")
                                                or str(cand.get("major_code") or "")
                                            )
                                            if not key or key in cand_map:
                                                continue
                                            cand_map[key] = cand
                                    tag_candidates = list(cand_map.values())[: max(INDUSTRY_AI_TOP_N, 120)]
                                    if tag_candidates:
                                        ai_attempted = True
                                        try:
                                            ai_res_tag = await asyncio.wait_for(
                                                verifier.judge_industry(
                                                    text=ai_text,
                                                    company_name=name,
                                                    candidates_text=INDUSTRY_CLASSIFIER.format_candidates_text(tag_candidates),
                                                ),
                                                timeout=min(clamp_timeout(ai_call_timeout), 20.0),
                                            )
                                        except Exception:
                                            ai_res_tag = None
                                        if isinstance(ai_res_tag, dict):
                                            matched_tag = _match_candidate(ai_res_tag, tag_candidates, "minor")
                                            new_conf = max(0.2, _ai_confidence(ai_res_tag)) if matched_tag else 0.0
                                            cur_conf = float(industry_result_post.get("confidence") or 0.0)
                                            if matched_tag and (new_conf >= cur_conf or cur_conf < INDUSTRY_AI_MIN_CONFIDENCE):
                                                industry_result_post = {
                                                    "major_code": str(matched_tag.get("major_code") or ""),
                                                    "major_name": str(matched_tag.get("major_name") or ""),
                                                    "middle_code": str(matched_tag.get("middle_code") or ""),
                                                    "middle_name": str(matched_tag.get("middle_name") or ""),
                                                    "minor_code": str(matched_tag.get("minor_code") or ""),
                                                    "minor_name": str(matched_tag.get("minor_name") or ""),
                                                    "confidence": new_conf,
                                                    "source": "ai_business_tags_retry",
                                                }
                                                ai_industry_name = str(ai_res_tag.get("industry") or ai_industry_name)

                            # business_tags を強いヒントにして追加AI判定（最小限の再呼び出し）
                            if (not industry_result_post) and ai_available and has_time_for_ai() and business_tags_val:
                                tag_list: list[str] = []
                                try:
                                    if business_tags_val.strip().startswith("["):
                                        maybe = json.loads(business_tags_val)
                                        if isinstance(maybe, list):
                                            tag_list = [str(t).strip() for t in maybe if str(t).strip()]
                                    else:
                                        tag_list = [business_tags_val.strip()]
                                except Exception:
                                    tag_list = [business_tags_val.strip()]

                                if tag_list:
                                    cand_map: dict[str, dict[str, str]] = {}
                                    for tag in tag_list:
                                        for cand in INDUSTRY_CLASSIFIER.build_candidates_from_industry_name(
                                            tag, top_n=max(INDUSTRY_AI_TOP_N, 120)
                                        ):
                                            key = (
                                                str(cand.get("minor_code") or "")
                                                or str(cand.get("middle_code") or "")
                                                or str(cand.get("major_code") or "")
                                            )
                                            if not key or key in cand_map:
                                                continue
                                            cand_map[key] = cand
                                    tag_candidates = list(cand_map.values())[: max(INDUSTRY_AI_TOP_N, 120)]
                                    if tag_candidates:
                                        ai_attempted = True
                                        try:
                                            ai_res_tag = await asyncio.wait_for(
                                                verifier.judge_industry(
                                                    text=ai_text,
                                                    company_name=name,
                                                    candidates_text=INDUSTRY_CLASSIFIER.format_candidates_text(tag_candidates),
                                                ),
                                                timeout=min(clamp_timeout(ai_call_timeout), 20.0),
                                            )
                                        except Exception:
                                            ai_res_tag = None
                                        if isinstance(ai_res_tag, dict):
                                            matched_tag = _match_candidate(ai_res_tag, tag_candidates, "minor")
                                            if matched_tag:
                                                industry_result_post = {
                                                    "major_code": str(matched_tag.get("major_code") or ""),
                                                    "major_name": str(matched_tag.get("major_name") or ""),
                                                    "middle_code": str(matched_tag.get("middle_code") or ""),
                                                    "middle_name": str(matched_tag.get("middle_name") or ""),
                                                    "minor_code": str(matched_tag.get("minor_code") or ""),
                                                    "minor_name": str(matched_tag.get("minor_name") or ""),
                                                    "confidence": max(0.2, _ai_confidence(ai_res_tag)),
                                                    "source": "ai_business_tags",
                                                }
                                                ai_industry_name = str(ai_res_tag.get("industry") or ai_industry_name)

                            if (not industry_result_post) and INDUSTRY_FORCE_CLASSIFY:
                                taxonomy = INDUSTRY_CLASSIFIER.taxonomy

                                def _candidate_from_code(code_val: str) -> dict[str, Any] | None:
                                    code = str(code_val or "").strip()
                                    if not code:
                                        return None
                                    if code in taxonomy.minor_names:
                                        (
                                            major_code,
                                            major_name,
                                            middle_code,
                                            middle_name,
                                            minor_code,
                                            minor_name,
                                        ) = taxonomy.resolve_hierarchy(code)
                                        if not (major_code and middle_code and minor_code):
                                            return None
                                        return {
                                            "major_code": major_code,
                                            "major_name": major_name,
                                            "middle_code": middle_code,
                                            "middle_name": middle_name,
                                            "minor_code": minor_code,
                                            "minor_name": minor_name,
                                        }
                                    if code in taxonomy.detail_names:
                                        (
                                            major_code,
                                            major_name,
                                            middle_code,
                                            middle_name,
                                            _minor_code,
                                            _minor_name,
                                            detail_code,
                                            detail_name,
                                        ) = taxonomy.resolve_detail_hierarchy(code)
                                        if not (major_code and middle_code and detail_code):
                                            return None
                                        return {
                                            "major_code": major_code,
                                            "major_name": major_name,
                                            "middle_code": middle_code,
                                            "middle_name": middle_name,
                                            "minor_code": detail_code,
                                            "minor_name": detail_name,
                                        }
                                    return None

                                forced_candidate: dict[str, Any] | None = None
                                forced_confidence = 0.35
                                minor_scores_map = scores.get("minor_scores") or {}
                                if isinstance(minor_scores_map, dict) and minor_scores_map:
                                    best_minor_code = ""
                                    best_minor_score = -1
                                    for code_key, score_val in minor_scores_map.items():
                                        try:
                                            score_num = int(score_val)
                                        except Exception:
                                            continue
                                        if score_num > best_minor_score:
                                            best_minor_code = str(code_key or "")
                                            best_minor_score = score_num
                                    if best_minor_code:
                                        forced_candidate = _candidate_from_code(best_minor_code)
                                        if forced_candidate:
                                            forced_confidence = 0.45

                                if (not forced_candidate) and ai_available and has_time_for_ai():
                                    forced_candidates = INDUSTRY_CLASSIFIER.build_level_candidates(
                                        "minor",
                                        scores,
                                        top_n=max(INDUSTRY_AI_TOP_N, 120),
                                    )
                                    if (not forced_candidates) and industry_hint_val:
                                        forced_candidates = INDUSTRY_CLASSIFIER.build_candidates_from_industry_name(
                                            industry_hint_val,
                                            top_n=max(INDUSTRY_AI_TOP_N, 120),
                                        )
                                    if forced_candidates:
                                        ai_attempted = True
                                        try:
                                            ai_res_force = await asyncio.wait_for(
                                                verifier.judge_industry(
                                                    text=ai_text,
                                                    company_name=name,
                                                    candidates_text=INDUSTRY_CLASSIFIER.format_candidates_text(forced_candidates),
                                                ),
                                                timeout=min(clamp_timeout(ai_call_timeout), 30.0),
                                            )
                                        except Exception:
                                            ai_res_force = None
                                        if isinstance(ai_res_force, dict):
                                            matched_force = _match_candidate(ai_res_force, forced_candidates, "minor")
                                            if matched_force:
                                                forced_candidate = matched_force
                                                forced_confidence = max(0.2, _ai_confidence(ai_res_force))
                                                ai_industry_name = str(ai_res_force.get("industry") or ai_industry_name)
                                    if (not forced_candidate) and forced_candidates:
                                        forced_candidate = forced_candidates[0]
                                        forced_confidence = 0.25

                                if not forced_candidate:
                                    for code in INDUSTRY_FORCE_DEFAULT_MINOR_CODES:
                                        forced_candidate = _candidate_from_code(code)
                                        if forced_candidate:
                                            forced_confidence = 0.2
                                            break

                                    if not forced_candidate:
                                        for code in sorted(taxonomy.minor_names.keys()):
                                            forced_candidate = _candidate_from_code(code)
                                            if forced_candidate:
                                                forced_confidence = 0.1
                                                break

                                if forced_candidate:
                                    industry_result_post = {
                                        "major_code": str(forced_candidate.get("major_code") or ""),
                                        "major_name": str(forced_candidate.get("major_name") or ""),
                                        "middle_code": str(forced_candidate.get("middle_code") or ""),
                                        "middle_name": str(forced_candidate.get("middle_name") or ""),
                                        "minor_code": str(forced_candidate.get("minor_code") or ""),
                                        "minor_name": str(forced_candidate.get("minor_name") or ""),
                                        "confidence": forced_confidence,
                                        "source": "forced_post",
                                    }

                            if industry_result_post:
                                minor_item_code = ""
                                minor_item_name = ""
                                minor_code_raw = str(industry_result_post.get("minor_code") or "").strip()
                                if minor_code_raw and minor_code_raw in INDUSTRY_CLASSIFIER.taxonomy.detail_names:
                                    (
                                        major_code,
                                        major_name,
                                        middle_code,
                                        middle_name,
                                        minor_code,
                                        minor_name,
                                        detail_code,
                                        detail_name,
                                    ) = INDUSTRY_CLASSIFIER.taxonomy.resolve_detail_hierarchy(minor_code_raw)
                                    if major_name:
                                        industry_major = major_name
                                    if middle_name:
                                        industry_middle = middle_name
                                    if minor_name:
                                        industry_minor = minor_name
                                    if major_code:
                                        industry_major_code = major_code
                                    if middle_code:
                                        industry_middle_code = middle_code
                                    if minor_code:
                                        industry_minor_code = minor_code
                                    minor_item_code = detail_code
                                    minor_item_name = detail_name
                                else:
                                    industry_major = industry_result_post.get("major_name", "") or industry_major
                                    industry_middle = industry_result_post.get("middle_name", "") or industry_middle
                                    industry_minor = industry_result_post.get("minor_name", "") or industry_minor
                                    industry_major_code = industry_result_post.get("major_code", "") or industry_major_code
                                    industry_middle_code = industry_result_post.get("middle_code", "") or industry_middle_code
                                    industry_minor_code = industry_result_post.get("minor_code", "") or industry_minor_code

                                if detail_result_post and not minor_item_code:
                                    detail_code_raw = str(detail_result_post.get("minor_code") or "").strip()
                                    if detail_code_raw and detail_code_raw in INDUSTRY_CLASSIFIER.taxonomy.detail_names:
                                        (
                                            _d_major_code,
                                            _d_major_name,
                                            _d_middle_code,
                                            _d_middle_name,
                                            d_minor_code,
                                            _d_minor_name,
                                            d_detail_code,
                                            d_detail_name,
                                        ) = INDUSTRY_CLASSIFIER.taxonomy.resolve_detail_hierarchy(detail_code_raw)
                                        if d_minor_code and d_minor_code == industry_minor_code:
                                            minor_item_code = d_detail_code
                                            minor_item_name = d_detail_name

                                if minor_item_code:
                                    company["industry_minor_item_code"] = minor_item_code
                                    company["industry_minor_item"] = minor_item_name
                                elif industry_minor_code and industry_minor:
                                    company["industry_minor_item_code"] = industry_minor_code
                                    company["industry_minor_item"] = industry_minor

                                industry_class_source = industry_result_post.get("source", "") or industry_class_source
                                try:
                                    industry_class_confidence = float(
                                        industry_result_post.get("confidence") or industry_class_confidence
                                    )
                                except Exception:
                                    pass
                                industry_val = (
                                    industry_minor
                                    or industry_result_post.get("middle_name")
                                    or industry_result_post.get("major_name")
                                    or industry_val
                                )
                            else:
                                # ここまでで決まらなければ無理に埋めず、レビュー対象に回す
                                industry_major_code = industry_major_code or ""
                                industry_major = industry_major or ""
                                industry_middle_code = industry_middle_code or ""
                                industry_middle = industry_middle or ""
                                industry_minor_code = ""
                                industry_minor = ""
                                company["industry_minor_item_code"] = ""
                                company["industry_minor_item"] = ""
                                industry_val = industry_val or "不明"
                                industry_class_source = industry_class_source or "unclassified"
                                industry_class_confidence = 0.0
                                if company.get("status") in (None, "", "pending", "done"):
                                    company["status"] = "review"

                            company["industry"] = (industry_val or "不明")[:60]
                            company["industry_major_code"] = industry_major_code or ""
                            company["industry_major"] = industry_major or "不明"
                            company["industry_middle_code"] = industry_middle_code or ""
                            company["industry_middle"] = industry_middle or "不明"
                            company["industry_minor_code"] = industry_minor_code or ""
                            company["industry_minor"] = industry_minor or "不明"
                            # タグが未生成なら、最終ブロックからローカル推定で埋める（2パス目での判定精度向上用）
                            if (not business_tags_val) and INFER_INDUSTRY_ALWAYS:
                                try:
                                    _inf_ind, inferred_tags2 = infer_industry_and_business_tags(industry_blocks)
                                    if inferred_tags2:
                                        business_tags_val = json.dumps(inferred_tags2[:5], ensure_ascii=False)
                                except Exception:
                                    pass
                            minor_item_now = str(company.get("industry_minor_item") or "").strip()
                            if (not minor_item_now) or minor_item_now == "不明":
                                if industry_minor:
                                    company["industry_minor_item_code"] = industry_minor_code or ""
                                    company["industry_minor_item"] = industry_minor
                                else:
                                    company["industry_minor_item_code"] = company.get("industry_minor_item_code") or ""
                                    company["industry_minor_item"] = (industry_val or "分類不能の産業")[:60]
                            company["industry_class_source"] = industry_class_source or "unclassified"
                            company["industry_class_confidence"] = float(industry_class_confidence or 0.0)
                            company["business_tags"] = business_tags_val

                        if not homepage:
                            top3_urls = list(urls or [])[:3]
                            top3_records = sorted(candidate_records or [], key=lambda r: r.get("search_rank", 1e9))[:3]
                            all_directory_like = bool(top3_records) and all(bool((r.get("rule") or {}).get("directory_like")) for r in top3_records)
                            if not top3_urls:
                                skip_reason = "no_search_results_or_prefiltered"
                            elif all_directory_like:
                                skip_reason = "top3_all_directory_like"
                            else:
                                skip_reason = "no_official_in_top3"
                            if not (company.get("error_code") or "").strip():
                                company["error_code"] = skip_reason
                            append_jsonl(
                                NO_OFFICIAL_LOG_PATH,
                                {
                                    "id": cid,
                                    "company_name": name,
                                    "csv_address": addr,
                                    "skip_reason": skip_reason,
                                    "top3_urls": top3_urls,
                                    "top3_candidates": [
                                        {
                                            "url": (r.get("normalized_url") or r.get("url") or ""),
                                            "search_rank": int(r.get("search_rank") or 0),
                                            "domain_score": int(r.get("domain_score") or 0),
                                            "rule_score": float(((r.get("rule") or {}).get("score")) or 0.0),
                                            "directory_like": bool((r.get("rule") or {}).get("directory_like")),
                                            "directory_score": int((r.get("rule") or {}).get("directory_score") or 0),
                                            "directory_reasons": list((r.get("rule") or {}).get("directory_reasons") or [])[:8],
                                            "blocked_host": bool((r.get("rule") or {}).get("blocked_host")),
                                            "prefecture_mismatch": bool((r.get("rule") or {}).get("prefecture_mismatch")),
                                        }
                                        for r in top3_records
                                    ],
                                },
                            )

                        status = "done" if homepage else "no_homepage"
                        if timed_out:
                            company["error_code"] = "timeout"
                            status = "review"
                        if not homepage and candidate_records:
                            status = "review"
                            if status == "done" and found_address and (not ai_official_selected) and not addr_compatible(addr, found_address):
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

                        strong_official = False
                        if status == "done":
                            strong_official = bool(
                                homepage
                                and homepage_official_flag == 1
                                and (
                                    (chosen_domain_score or 0) >= 4
                                    or (homepage_official_source == "verify_promote" and (chosen_domain_score or 0) >= 3)
                                )
                                and (
                                    ai_official_selected
                                    or (not addr or not found_address or addr_compatible(addr, found_address))
                                )
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

                        if EXTRACT_DEBUG_JSONL_PATH:
                            try:
                                os.makedirs(os.path.dirname(EXTRACT_DEBUG_JSONL_PATH) or ".", exist_ok=True)
                                debug_payload = {
                                    "id": cid,
                                    "name": name,
                                    "status": status,
                                    "homepage": homepage,
                                    "homepage_official_flag": int(homepage_official_flag or 0),
                                    "homepage_official_source": homepage_official_source,
                                    "homepage_official_score": float(homepage_official_score or 0.0),
                                    "chosen_domain_score": int(chosen_domain_score or 0),
                                    "phone": phone,
                                    "phone_source": phone_source,
                                    "source_url_phone": src_phone,
                                    "address": found_address,
                                    "address_source": address_source,
                                    "source_url_address": src_addr,
                                    "rep_name": rep_name_val,
                                    "source_url_rep": src_rep,
                                    "page_type_per_url": page_type_per_url,
                                    "candidates_brief_by_url": {k: candidates_brief_by_url[k] for k in list(candidates_brief_by_url.keys())[:8]},
                                    "deep": {
                                        "pages_visited": int(deep_pages_visited or 0),
                                        "fetch_count": int(deep_fetch_count or 0),
                                        "fetch_failures": int(deep_fetch_failures or 0),
                                        "skip_reason": deep_skip_reason or "",
                                        "stop_reason": deep_stop_reason or "",
                                        "urls_visited": list(deep_urls_visited or [])[:8],
                                    },
                                    "drop_reasons": drop_reasons,
                                }
                                with open(EXTRACT_DEBUG_JSONL_PATH, "a", encoding="utf-8") as f:
                                    f.write(json.dumps(debug_payload, ensure_ascii=False) + "\n")
                            except Exception:
                                log.debug("extract debug log skipped", exc_info=True)

                        manager.save_company_data(company, status=status)
                        log.info("[%s] 保存完了: status=%s elapsed=%.1fs (worker=%s)", cid, status, elapsed(), WORKER_ID)

                        if csv_writer:
                            csv_writer.writerow(_csv_safe_row({k: company.get(k, "") for k in CSV_FIELDNAMES}))
                            csv_file.flush()

                        processed += 1

            except SkipCompany:
                # save_no_homepage() が保存済み（エラー扱いにしない）
                try:
                    log.info("[%s] 候補が全滅のため次へ (worker=%s)", cid, WORKER_ID)
                    if csv_writer:
                        csv_writer.writerow(_csv_safe_row({k: company.get(k, "") for k in CSV_FIELDNAMES}))
                        csv_file.flush()
                    processed += 1
                except Exception:
                    pass
            except HardTimeout:
                # 60秒超え等で打ち切り：ここまでに分かっている情報を保存して次へ
                try:
                    save_partial("timeout")
                except Exception:
                    pass
            except Exception as e:
                log.error("[%s] エラー: %s (worker=%s)", cid, e, WORKER_ID, exc_info=True)
                manager.update_status(cid, "error")

            # 1社ごとのスリープ（±JITTERでレート制限/ドメイン集中回避）
            if SLEEP_BETWEEN_SEC > 0:
                await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))

        if timeouts_extended:
            TIME_LIMIT_FETCH_ONLY = DEFAULT_TIME_LIMIT_FETCH_ONLY
            TIME_LIMIT_WITH_OFFICIAL = DEFAULT_TIME_LIMIT_WITH_OFFICIAL
            TIME_LIMIT_DEEP = DEFAULT_TIME_LIMIT_DEEP
            try:
                scraper.page_timeout_ms = normal_page_timeout_ms
                scraper.slow_page_threshold_ms = normal_slow_page_threshold_ms
            except Exception:
                pass

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
