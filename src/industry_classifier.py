import csv
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from src.text_normalizer import norm_text, norm_text_compact

try:
    import google.generativeai as _genai
except Exception:
    _genai = None  # type: ignore

log = logging.getLogger(__name__)

DEFAULT_JSIC_CSV_PATH = os.getenv("JSIC_CSV_PATH", "docs/industry_select.csv")
DEFAULT_JSIC_JSON_PATH = os.getenv("JSIC_JSON_PATH", "docs/industry_select.json")
DEFAULT_INDUSTRY_ALIASES_CSV_PATH = os.getenv("INDUSTRY_ALIASES_CSV_PATH", "industry_aliases.csv")
DEFAULT_GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
DEFAULT_ALIAS_EMBED_MODEL = (os.getenv("INDUSTRY_ALIAS_EMBED_MODEL") or "models/text-embedding-004").strip()
ALIAS_SEMANTIC_ENABLED = os.getenv("INDUSTRY_ALIAS_SEMANTIC_ENABLED", "true").lower() == "true"
ALIAS_SEMANTIC_PROVIDER = (os.getenv("INDUSTRY_ALIAS_SEMANTIC_PROVIDER", "auto") or "auto").strip().lower()
ALIAS_SEMANTIC_MIN_SIM = float(os.getenv("INDUSTRY_ALIAS_SEMANTIC_MIN_SIM", "0.90"))
ALIAS_SEMANTIC_MIN_MARGIN = float(os.getenv("INDUSTRY_ALIAS_SEMANTIC_MIN_MARGIN", "0.05"))
ALIAS_SEMANTIC_STRONG_SIM = float(os.getenv("INDUSTRY_ALIAS_SEMANTIC_STRONG_SIM", "0.95"))
ALIAS_SEMANTIC_STRONG_MARGIN = float(os.getenv("INDUSTRY_ALIAS_SEMANTIC_STRONG_MARGIN", "0.08"))
ALIAS_SEMANTIC_MIN_PRIORITY = max(1, int(os.getenv("INDUSTRY_ALIAS_SEMANTIC_MIN_PRIORITY", "6")))
ALIAS_SEMANTIC_MAX_PHRASES = max(1, int(os.getenv("INDUSTRY_ALIAS_SEMANTIC_MAX_PHRASES", "10")))
ALIAS_SEMANTIC_MAX_HITS = max(1, int(os.getenv("INDUSTRY_ALIAS_SEMANTIC_MAX_HITS", "4")))
ALIAS_SEMANTIC_ALLOW_STANDALONE = os.getenv("INDUSTRY_ALIAS_SEMANTIC_ALLOW_STANDALONE", "false").lower() == "true"
SEMANTIC_TAXONOMY_ENABLED = os.getenv("INDUSTRY_SEMANTIC_TAXONOMY_ENABLED", "true").lower() == "true"
SEMANTIC_TAXONOMY_MIN_SIM = float(os.getenv("INDUSTRY_SEMANTIC_TAXONOMY_MIN_SIM", "0.92"))
SEMANTIC_TAXONOMY_MIN_MARGIN = float(os.getenv("INDUSTRY_SEMANTIC_TAXONOMY_MIN_MARGIN", "0.05"))
SEMANTIC_TAXONOMY_MAX_HITS = max(1, int(os.getenv("INDUSTRY_SEMANTIC_TAXONOMY_MAX_HITS", "4")))
SEMANTIC_TAXONOMY_REQUIRE_BOTH = os.getenv("INDUSTRY_SEMANTIC_TAXONOMY_REQUIRE_BOTH", "true").lower() == "true"
DEFAULT_INDUSTRY_MEMORY_CSV_PATH = os.getenv("INDUSTRY_MEMORY_CSV_PATH", "data/industry_memory.csv")
AUTO_LEARN_ENABLED = os.getenv("INDUSTRY_AUTO_LEARN_ENABLED", "false").lower() == "true"
AUTO_LEARN_MIN_CONFIDENCE = float(os.getenv("INDUSTRY_AUTO_LEARN_MIN_CONFIDENCE", "0.78"))
AUTO_LEARN_MIN_COUNT = max(1, int(os.getenv("INDUSTRY_AUTO_LEARN_MIN_COUNT", "2")))
AUTO_LEARN_MAX_TERMS = max(1, int(os.getenv("INDUSTRY_AUTO_LEARN_MAX_TERMS", "6")))
AUTO_LEARN_MIN_TERM_LEN = max(2, int(os.getenv("INDUSTRY_AUTO_LEARN_MIN_TERM_LEN", "2")))
AUTO_LEARN_MAX_TERM_LEN = max(AUTO_LEARN_MIN_TERM_LEN, int(os.getenv("INDUSTRY_AUTO_LEARN_MAX_TERM_LEN", "20")))

_GENERIC_TOKENS = {
    "事業", "業", "サービス", "製品", "商品", "販売", "製造", "提供", "運営", "管理",
    "開発", "加工", "設計", "施工", "保守", "メンテナンス", "関連", "その他", "附随",
}
_AUTO_LEARN_STOPWORDS = _GENERIC_TOKENS | {
    "当社", "弊社", "会社", "企業", "公式", "情報", "サイト", "ホームページ", "お問い合わせ",
    "コンサル", "コンサルティング", "ソリューション", "プラットフォーム", "システム", "サービス提供",
    "提供しています", "行っています", "行う", "行います", "実施しています", "実施", "対応しています",
}
_AUTO_TERM_RE = re.compile(r"[一-龥ぁ-んァ-ンa-z0-9]{2,32}")
_AUTO_LEARN_VERB_RE = re.compile(
    r"(して(い)?ます|します|しました|した|する|いたします|行っています|行います|行った|行う|しております|提供しています)$"
)

_TOKEN_PART_RE = re.compile(r"[a-z0-9]+|[一-龥ぁ-んァ-ン]{2,}")
_COMPOUND_SUFFIXES = (
    "販売", "製造", "加工", "企画", "開発", "運営", "運送", "輸送", "配送",
    "賃貸", "管理", "仲介", "代理", "保守", "メンテナンス", "工事", "施工",
    "卸売", "小売", "小売業", "卸売業", "サービス", "コンサル", "コンサルティング",
    "支援", "設計", "修理", "再生", "投資", "買取",
)

# CSV が読めない場合の最小限fallback
_ALIAS_TO_MINOR_FALLBACK: dict[str, tuple[str, int, int, str]] = {
    "ai": ("392", 6, 0, "it"),
    "生成ai": ("392", 7, 0, "it"),
    "llm": ("392", 7, 0, "it"),
    "dx": ("392", 6, 0, "it"),
    "it": ("392", 5, 0, "it"),
    "ict": ("392", 5, 0, "it"),
    "iot": ("392", 5, 0, "it"),
    "saas": ("392", 6, 0, "it"),
    "paas": ("392", 5, 0, "it"),
    "iaas": ("392", 5, 0, "it"),
    "クラウド": ("392", 5, 0, "it"),
    "システム開発": ("391", 6, 0, "it"),
    "受託開発": ("391", 6, 0, "it"),
    "ソフトウェア開発": ("391", 6, 0, "it"),
    "webサービス": ("401", 6, 0, "it"),
    "ec": ("401", 6, 0, "ec"),
    "eコマース": ("611", 6, 0, "ec"),
    "ネット通販": ("611", 7, 0, "ec"),
    "オンラインショップ": ("611", 7, 0, "ec"),
    "ec運営": ("611", 6, 0, "ec"),
    "d2c": ("611", 6, 0, "ec"),
    "seo": ("731", 5, 0, "ad"),
    "広告運用": ("731", 6, 0, "ad"),
    "デジタルマーケティング": ("731", 6, 0, "ad"),
    "人材派遣": ("912", 7, 0, "hr"),
    "人材紹介": ("911", 7, 0, "hr"),
    "bpo": ("929", 5, 1, "review"),
    "物流": ("441", 6, 0, "logistics"),
    "運送": ("441", 6, 0, "logistics"),
    "配送": ("441", 6, 0, "logistics"),
    "倉庫": ("471", 6, 0, "logistics"),
    "建設": ("062", 6, 0, "construction"),
    "土木工事": ("062", 7, 0, "construction"),
    "電気工事": ("081", 7, 0, "construction"),
    "管工事": ("083", 7, 0, "construction"),
    "不動産売買": ("681", 7, 0, "real_estate"),
    "賃貸仲介": ("682", 7, 0, "real_estate"),
    "不動産管理": ("694", 7, 0, "real_estate"),
    "病院": ("831", 7, 0, "medical"),
    "クリニック": ("832", 7, 0, "medical"),
    "介護": ("854", 7, 0, "medical"),
    "訪問介護": ("854", 7, 0, "medical"),
    "訪問看護": ("854", 7, 0, "medical"),
    "学習塾": ("823", 7, 0, "education"),
    "eラーニング": ("822", 6, 0, "education"),
    "職業訓練": ("822", 7, 0, "education"),
    "保育": ("853", 7, 0, "education"),
    "保険代理店": ("674", 7, 0, "finance"),
    "決済": ("661", 6, 1, "review"),
    "リース": ("701", 6, 0, "finance"),
    "ファクタリング": ("649", 6, 1, "review"),
    "貸金": ("641", 7, 0, "finance"),
    "試作": ("329", 5, 1, "review"),
    "量産": ("329", 5, 1, "review"),
    "金型": ("269", 6, 0, "manufacturing"),
    "精密加工": ("266", 6, 0, "manufacturing"),
    "切削": ("266", 6, 0, "manufacturing"),
    "板金": ("244", 6, 0, "manufacturing"),
    "表面処理": ("246", 6, 0, "manufacturing"),
    "oem": ("329", 5, 1, "review"),
    "odm": ("329", 5, 1, "review"),
}


def _normalize_vector(vec: list[float] | tuple[float, ...] | None) -> list[float] | None:
    if not vec:
        return None
    s = 0.0
    out: list[float] = []
    for v in vec:
        try:
            f = float(v)
        except Exception:
            continue
        out.append(f)
        s += f * f
    if not out or s <= 0:
        return None
    norm = math.sqrt(s)
    return [v / norm for v in out]


def _cosine_similarity(v1: list[float] | None, v2: list[float] | None) -> float:
    if not v1 or not v2:
        return -1.0
    if len(v1) != len(v2):
        return -1.0
    s = 0.0
    for a, b in zip(v1, v2):
        s += a * b
    return float(s)


class _SemanticEmbedder:
    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        raise NotImplementedError


class _GeminiSemanticEmbedder(_SemanticEmbedder):
    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = (api_key or "").strip()
        self.model = (model or DEFAULT_ALIAS_EMBED_MODEL).strip()
        self.available = bool(self.api_key and self.model and _genai is not None)
        if not self.available:
            return
        try:
            _genai.configure(api_key=self.api_key)  # type: ignore[union-attr]
        except Exception:
            self.available = False

    @staticmethod
    def _extract_vector(resp: Any) -> list[float] | None:
        if resp is None:
            return None
        if isinstance(resp, dict):
            emb = resp.get("embedding")
            if isinstance(emb, list) and emb and isinstance(emb[0], (int, float)):
                return _normalize_vector(emb)
            if isinstance(emb, dict):
                values = emb.get("values")
                if isinstance(values, list):
                    return _normalize_vector(values)
        emb_attr = getattr(resp, "embedding", None)
        if isinstance(emb_attr, list):
            return _normalize_vector(emb_attr)
        vals_attr = getattr(emb_attr, "values", None)
        if isinstance(vals_attr, list):
            return _normalize_vector(vals_attr)
        return None

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        if not self.available:
            return [None for _ in texts]
        out: list[list[float] | None] = []
        for text in texts:
            t = str(text or "").strip()
            if not t:
                out.append(None)
                continue
            try:
                resp = _genai.embed_content(  # type: ignore[union-attr]
                    model=self.model,
                    content=t,
                    task_type="SEMANTIC_SIMILARITY",
                )
            except Exception:
                out.append(None)
                continue
            out.append(self._extract_vector(resp))
        return out


class _NgramSemanticEmbedder(_SemanticEmbedder):
    def __init__(self, dim: int = 384) -> None:
        self.dim = max(64, int(dim))

    def _embed_one(self, text: str) -> list[float] | None:
        compact = norm_text_compact(text)
        if len(compact) < 2:
            return None
        vec = [0.0] * self.dim
        grams: list[str] = []
        for n in (2, 3):
            if len(compact) < n:
                continue
            for i in range(0, len(compact) - n + 1):
                grams.append(compact[i : i + n])
        if not grams:
            return None
        for g in grams:
            idx = (hash(g) & 0x7FFFFFFF) % self.dim
            vec[idx] += 1.0
        return _normalize_vector(vec)

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        return [self._embed_one(str(t or "")) for t in texts]


@dataclass
class IndustryEntry:
    level: str
    code: str
    name: str
    major_code: str
    middle_code: str
    minor_code: str


@dataclass
class IndustryAliasEntry:
    alias: str
    target_minor_code: str
    priority: int
    requires_review: bool
    domain_tag: str
    allowed_major_codes: tuple[str, ...]
    notes: str
    alias_norm: str


@dataclass
class IndustryMemoryEntry:
    term: str
    target_minor_code: str
    count: int
    confidence: float
    source: str
    updated_at: str
    term_norm: str


@dataclass
class SemanticTaxonomyCandidate:
    minor_code: str
    sim: float
    margin: float
    phrase: str
    source: str


class JSICTaxonomy:
    def __init__(self, csv_path: str, json_path: Optional[str] = None) -> None:
        self.csv_path = csv_path
        self.json_path = json_path
        self.entries: list[IndustryEntry] = []
        self.major_names: dict[str, str] = {}
        self.middle_names: dict[str, str] = {}
        self.minor_names: dict[str, str] = {}
        self._minor_names_norm: dict[str, str] = {}
        self.detail_names: dict[str, str] = {}
        self._detail_names_norm: dict[str, str] = {}
        self.minor_to_middle: dict[str, str] = {}
        self.middle_to_major: dict[str, str] = {}
        self.detail_to_minor: dict[str, str] = {}
        self.detail_to_middle: dict[str, str] = {}
        self.detail_to_major: dict[str, str] = {}
        self.normalized_name_index: dict[str, list[dict[str, str]]] = {}
        self._token_index: dict[str, set[str]] = {}

    def load(self) -> bool:
        if self._load_from_csv():
            self._finalize_loaded_data()
            return True

        allow_json_fallback = os.getenv("JSIC_ALLOW_JSON_FALLBACK", "false").lower() == "true"
        if allow_json_fallback and self.json_path and os.path.exists(self.json_path):
            if self._load_from_json(self.json_path):
                self._finalize_loaded_data()
                return True

        log.warning("JSIC taxonomy not loaded. CSV=%s JSON=%s", self.csv_path, self.json_path)
        return False

    def _finalize_loaded_data(self) -> None:
        self._rebuild_reverse_indexes()
        self._build_name_index_from_maps()
        self._build_token_index()

    def _load_from_csv(self) -> bool:
        if not self.csv_path or not os.path.exists(self.csv_path):
            return False
        rows = self._read_rows(self.csv_path)
        if not rows:
            log.warning("JSIC taxonomy CSV empty: %s", self.csv_path)
            return False

        if self._looks_like_industry_select(rows):
            self._load_from_industry_select(rows)
            return True

        current_major = ""
        current_middle = ""
        current_minor = ""
        for row in rows:
            code, name = self._extract_code_name(row)
            if not code or not name:
                continue
            level = self._infer_level(code)
            if level == "major":
                current_major = code
                current_middle = ""
                current_minor = ""
                self.major_names[code] = name
            elif level == "middle":
                current_middle = code
                current_minor = ""
                if current_major:
                    self.middle_to_major[current_middle] = current_major
                self.middle_names[code] = name
            elif level == "minor":
                current_minor = code
                if current_middle:
                    self.minor_to_middle[current_minor] = current_middle
                self.minor_names[code] = name
                self._minor_names_norm[code] = self._normalize(name)
            elif level == "detail":
                self.detail_names[code] = name
                self._detail_names_norm[code] = self._normalize(name)
                if current_minor:
                    self.detail_to_minor[code] = current_minor
            else:
                continue

            entry = IndustryEntry(
                level=level,
                code=code,
                name=name,
                major_code=current_major,
                middle_code=current_middle,
                minor_code=current_minor,
            )
            self.entries.append(entry)

            self._append_normalized_name_index(
                name=name,
                major_code=current_major,
                middle_code=current_middle,
                minor_code=current_minor if level in {"minor", "detail"} else "",
                detail_code=code if level == "detail" else "",
            )

        return True

    def _load_from_json(self, path: str) -> bool:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        major_names = data.get("major_names")
        middle_names = data.get("middle_names")
        minor_names = data.get("minor_names")
        detail_names = data.get("detail_names")
        minor_to_middle = data.get("minor_to_middle")
        middle_to_major = data.get("middle_to_major")
        detail_to_minor = data.get("detail_to_minor")
        entries = data.get("entries")
        if not isinstance(major_names, dict) or not isinstance(middle_names, dict) or not isinstance(minor_names, dict):
            return False

        self.major_names = {str(k): str(v) for k, v in major_names.items()}
        self.middle_names = {str(k): str(v) for k, v in (middle_names or {}).items()}
        self.minor_names = {str(k): str(v) for k, v in (minor_names or {}).items()}
        self.detail_names = {str(k): str(v) for k, v in (detail_names or {}).items()}
        self.minor_to_middle = {str(k): str(v) for k, v in (minor_to_middle or {}).items()}
        self.middle_to_major = {str(k): str(v) for k, v in (middle_to_major or {}).items()}
        self.detail_to_minor = {str(k): str(v) for k, v in (detail_to_minor or {}).items()}
        self._minor_names_norm = {k: self._normalize(v) for k, v in self.minor_names.items()}
        self._detail_names_norm = {k: self._normalize(v) for k, v in self.detail_names.items()}

        self.entries = []
        if isinstance(entries, list) and entries:
            for e in entries:
                if not isinstance(e, dict):
                    continue
                level = str(e.get("level") or "")
                code = str(e.get("code") or "")
                name = str(e.get("name") or "")
                major_code = str(e.get("major_code") or "")
                middle_code = str(e.get("middle_code") or "")
                minor_code = str(e.get("minor_code") or "")
                if not (level and code and name):
                    continue
                self.entries.append(
                    IndustryEntry(
                        level=level,
                        code=code,
                        name=name,
                        major_code=major_code,
                        middle_code=middle_code,
                        minor_code=minor_code,
                    )
                )
        else:
            for code, name in self.major_names.items():
                self.entries.append(IndustryEntry("major", code, name, code, "", ""))
            for code, name in self.middle_names.items():
                major_code = self.middle_to_major.get(code, "")
                self.entries.append(IndustryEntry("middle", code, name, major_code, code, ""))
            for code, name in self.minor_names.items():
                middle_code = self.minor_to_middle.get(code, "")
                major_code = self.middle_to_major.get(middle_code, "") if middle_code else ""
                self.entries.append(IndustryEntry("minor", code, name, major_code, middle_code, code))
            for code, name in self.detail_names.items():
                minor_code = self.detail_to_minor.get(code, "")
                middle_code = self.minor_to_middle.get(minor_code, "") if minor_code else ""
                major_code = self.middle_to_major.get(middle_code, "") if middle_code else ""
                self.entries.append(IndustryEntry("detail", code, name, major_code, middle_code, minor_code))
        return True

    @staticmethod
    def _looks_like_industry_select(rows: list[dict[str, str]]) -> bool:
        if not rows:
            return False
        keys = set(rows[0].keys())
        return {"大分類コード", "中分類コード", "小分類コード", "細分類コード", "項目名"}.issubset(keys)

    def _load_from_industry_select(self, rows: list[dict[str, str]]) -> None:
        for row in rows:
            major = (row.get("大分類コード") or "").strip()
            middle = (row.get("中分類コード") or "").strip()
            minor = (row.get("小分類コード") or "").strip()
            detail = (row.get("細分類コード") or "").strip()
            name = (row.get("項目名") or "").strip()
            if not name or not major:
                continue

            level = ""
            code = ""
            if middle == "00" and minor == "000" and detail == "0000":
                level = "major"
                code = major
                self.major_names[major] = name
            elif minor == "000" and detail == "0000":
                level = "middle"
                code = middle
                self.middle_names[middle] = name
                if major:
                    self.middle_to_major[middle] = major
            elif detail == "0000" and minor != "000":
                level = "minor"
                code = minor
                self.minor_names[minor] = name
                self._minor_names_norm[minor] = self._normalize(name)
                if middle:
                    self.minor_to_middle[minor] = middle
                if major and middle:
                    self.middle_to_major[middle] = major
            elif detail and detail != "0000":
                level = "detail"
                code = detail
                self.detail_names[detail] = name
                self._detail_names_norm[detail] = self._normalize(name)
                if minor:
                    self.detail_to_minor[detail] = minor
                if middle:
                    self.detail_to_middle[detail] = middle
                if major:
                    self.detail_to_major[detail] = major
            else:
                continue

            if level and code:
                self.entries.append(
                    IndustryEntry(
                        level=level,
                        code=code,
                        name=name,
                        major_code=major,
                        middle_code=middle,
                        minor_code=minor,
                    )
                )

                self._append_normalized_name_index(
                    name=name,
                    major_code=major,
                    middle_code="" if middle == "00" else middle,
                    minor_code="" if minor == "000" else minor,
                    detail_code="" if detail == "0000" else detail,
                )

    def _read_rows(self, path: str) -> list[dict[str, str]]:
        for enc in ("utf-8-sig", "cp932", "shift_jis"):
            try:
                with open(path, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    return [dict(row) for row in reader]
            except Exception:
                continue

        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                rows = []
                for row in reader:
                    if not row:
                        continue
                    rows.append({f"col{i}": v for i, v in enumerate(row)})
                return rows
        except Exception:
            return []

    def _extract_code_name(self, row: dict[str, str]) -> tuple[str, str]:
        keys = [k for k in row.keys() if k is not None]
        values = [str(row.get(k) or "").strip() for k in keys]

        if {"大分類コード", "中分類コード", "小分類コード", "細分類コード", "項目名"}.issubset(keys):
            name = (row.get("項目名") or "").strip()
            major = (row.get("大分類コード") or "").strip()
            middle = (row.get("中分類コード") or "").strip()
            minor = (row.get("小分類コード") or "").strip()
            detail = (row.get("細分類コード") or "").strip()

            code = ""
            if detail and detail not in {"0000", "0"}:
                code = detail
            elif minor and minor not in {"000", "0"}:
                code = minor
            elif middle and middle not in {"00", "0"}:
                code = middle
            elif major:
                code = major

            if code and name:
                return code, name

        def pick_key(candidates: Iterable[str]) -> Optional[str]:
            for k in keys:
                if k in candidates:
                    return k
            return None

        code_key = pick_key({"分類コード", "分類番号", "コード", "項目番号", "分類項目番号", "番号"})
        name_key = pick_key({"分類項目名", "分類名", "項目名", "名称"})

        code = (row.get(code_key) or "").strip() if code_key else ""
        name = (row.get(name_key) or "").strip() if name_key else ""

        if code and name:
            return code, name

        for v in values:
            if not v:
                continue
            m = re.match(r"^([A-Z]|[0-9]{2,4})\s*(.+)$", v)
            if m:
                return m.group(1), m.group(2).strip()
        return "", ""

    @staticmethod
    def _infer_level(code: str) -> str:
        if re.fullmatch(r"[A-Z]", code):
            return "major"
        if re.fullmatch(r"[0-9]{2}", code):
            return "middle"
        if re.fullmatch(r"[0-9]{3}", code):
            return "minor"
        if re.fullmatch(r"[0-9]{4}", code):
            return "detail"
        return "unknown"

    @staticmethod
    def _normalize(text: str) -> str:
        return norm_text_compact(text)

    def _split_compound_token(self, token: str) -> set[str]:
        out: set[str] = set()
        raw = str(token or "").strip()
        if len(raw) < 2:
            return out
        out.add(raw)

        for part in _TOKEN_PART_RE.findall(raw):
            p = part.strip()
            if len(p) >= 2:
                out.add(p)

        for suf in _COMPOUND_SUFFIXES:
            if raw.endswith(suf) and len(raw) > len(suf) + 1:
                root = raw[: -len(suf)].strip()
                if len(root) >= 2:
                    out.add(root)
                if len(suf) >= 2:
                    out.add(suf)
        return out

    def _extract_text_tokens(self, text: str) -> set[str]:
        spaced = norm_text(text)
        if not spaced:
            return set()
        tokens: set[str] = set()
        for chunk in spaced.split(" "):
            chunk = chunk.strip()
            if len(chunk) < 2:
                continue
            for tok in self._split_compound_token(chunk):
                if tok in _GENERIC_TOKENS:
                    continue
                tokens.add(tok)
        return tokens

    def _build_token_index(self) -> None:
        index: dict[str, set[str]] = {}
        for code, name in self.minor_names.items():
            tokens = self._tokenize_name(name)
            for t in tokens:
                index.setdefault(t, set()).add(code)
        self._token_index = index

    def _tokenize_name(self, name: str) -> set[str]:
        return self._extract_text_tokens(name)

    def _rebuild_reverse_indexes(self) -> None:
        for detail_code, minor_code in list(self.detail_to_minor.items()):
            if not detail_code or not minor_code:
                continue
            if not self.detail_to_middle.get(detail_code):
                middle_code = self.minor_to_middle.get(minor_code, "")
                if middle_code:
                    self.detail_to_middle[detail_code] = middle_code
            if not self.detail_to_major.get(detail_code):
                middle_code = self.detail_to_middle.get(detail_code, "")
                major_code = self.middle_to_major.get(middle_code, "") if middle_code else ""
                if major_code:
                    self.detail_to_major[detail_code] = major_code

    def _append_normalized_name_index(
        self,
        *,
        name: str,
        major_code: str,
        middle_code: str,
        minor_code: str,
        detail_code: str,
    ) -> None:
        norm = self._normalize(name)
        if not norm:
            return
        record = {
            "major_code": major_code,
            "middle_code": middle_code,
            "minor_code": minor_code,
            "detail_code": detail_code,
            "raw_name": name,
        }
        arr = self.normalized_name_index.setdefault(norm, [])
        if record not in arr:
            arr.append(record)

    def _build_name_index_from_maps(self) -> None:
        self.normalized_name_index = {}
        for code, name in self.major_names.items():
            self._append_normalized_name_index(
                name=name,
                major_code=code,
                middle_code="",
                minor_code="",
                detail_code="",
            )
        for code, name in self.middle_names.items():
            self._append_normalized_name_index(
                name=name,
                major_code=self.middle_to_major.get(code, ""),
                middle_code=code,
                minor_code="",
                detail_code="",
            )
        for code, name in self.minor_names.items():
            middle_code = self.minor_to_middle.get(code, "")
            self._append_normalized_name_index(
                name=name,
                major_code=self.middle_to_major.get(middle_code, "") if middle_code else "",
                middle_code=middle_code,
                minor_code=code,
                detail_code="",
            )
        for code, name in self.detail_names.items():
            minor_code = self.detail_to_minor.get(code, "")
            middle_code = self.detail_to_middle.get(code, "") or self.minor_to_middle.get(minor_code, "")
            major_code = self.detail_to_major.get(code, "") or (self.middle_to_major.get(middle_code, "") if middle_code else "")
            self._append_normalized_name_index(
                name=name,
                major_code=major_code,
                middle_code=middle_code,
                minor_code=minor_code,
                detail_code=code,
            )

    def score_minors(self, text: str) -> dict[str, int]:
        if not text:
            return {}
        norm = self._normalize(text)
        if not norm:
            return {}

        text_tokens = self._extract_text_tokens(text)

        scores: dict[str, int] = {}
        for tok in text_tokens:
            for code in self._token_index.get(tok, set()):
                scores[code] = scores.get(code, 0) + 1
            for code, name_norm in self._minor_names_norm.items():
                if tok in name_norm:
                    scores[code] = scores.get(code, 0) + 1
        return scores

    def score_details(self, text: str) -> dict[str, int]:
        if not text or not self.detail_names:
            return {}
        norm = self._normalize(text)
        if not norm:
            return {}
        text_tokens = self._extract_text_tokens(text)
        scores: dict[str, int] = {}
        for tok in text_tokens:
            for code, name_norm in self._detail_names_norm.items():
                if tok in name_norm:
                    scores[code] = scores.get(code, 0) + 1
        return scores

    def resolve_hierarchy(self, minor_code: str) -> tuple[str, str, str, str, str, str]:
        minor_name = self.minor_names.get(minor_code, "")
        middle_code = self.minor_to_middle.get(minor_code, "")
        major_code = self.middle_to_major.get(middle_code, "") if middle_code else ""
        middle_name = self.middle_names.get(middle_code, "")
        major_name = self.major_names.get(major_code, "")
        return major_code, major_name, middle_code, middle_name, minor_code, minor_name

    def resolve_detail_hierarchy(self, detail_code: str) -> tuple[str, str, str, str, str, str, str, str]:
        detail_name = self.detail_names.get(detail_code, "")
        minor_code = self.detail_to_minor.get(detail_code, "")
        middle_code = self.detail_to_middle.get(detail_code, "")
        major_code = self.detail_to_major.get(detail_code, "") if middle_code else ""
        if not middle_code and minor_code:
            middle_code = self.minor_to_middle.get(minor_code, "")
        if not major_code and middle_code:
            major_code = self.middle_to_major.get(middle_code, "")
        minor_name = self.minor_names.get(minor_code, "")
        middle_name = self.middle_names.get(middle_code, "")
        major_name = self.major_names.get(major_code, "")
        return major_code, major_name, middle_code, middle_name, minor_code, minor_name, detail_code, detail_name


class IndustryClassifier:
    def __init__(
        self,
        csv_path: str | None = None,
        aliases_csv_path: str | None = None,
        semantic_embedder: Optional[_SemanticEmbedder] = None,
    ) -> None:
        self.csv_path = csv_path or DEFAULT_JSIC_CSV_PATH
        self.taxonomy = JSICTaxonomy(self.csv_path, DEFAULT_JSIC_JSON_PATH)
        self.loaded = self.taxonomy.load()

        self.aliases_csv_path = aliases_csv_path or DEFAULT_INDUSTRY_ALIASES_CSV_PATH
        self.alias_entries: list[IndustryAliasEntry] = []
        self.learned_alias_entries: list[IndustryAliasEntry] = []
        self.alias_source = "fallback"
        self.memory_csv_path = DEFAULT_INDUSTRY_MEMORY_CSV_PATH
        self.auto_learn_enabled = bool(AUTO_LEARN_ENABLED)
        self.auto_learn_min_confidence = float(max(0.0, min(1.0, AUTO_LEARN_MIN_CONFIDENCE)))
        self.auto_learn_min_count = max(1, int(AUTO_LEARN_MIN_COUNT))
        self.auto_learn_max_terms = max(1, int(AUTO_LEARN_MAX_TERMS))
        self.auto_learn_min_term_len = max(2, int(AUTO_LEARN_MIN_TERM_LEN))
        self.auto_learn_max_term_len = max(self.auto_learn_min_term_len, int(AUTO_LEARN_MAX_TERM_LEN))
        self.memory_entries: dict[tuple[str, str], IndustryMemoryEntry] = {}
        self.semantic_enabled = bool(ALIAS_SEMANTIC_ENABLED)
        self.semantic_provider = ALIAS_SEMANTIC_PROVIDER
        self.semantic_embedder = semantic_embedder
        self.semantic_min_sim = float(max(0.0, min(1.0, ALIAS_SEMANTIC_MIN_SIM)))
        self.semantic_min_margin = float(max(0.0, min(1.0, ALIAS_SEMANTIC_MIN_MARGIN)))
        self.semantic_strong_sim = float(max(self.semantic_min_sim, min(1.0, ALIAS_SEMANTIC_STRONG_SIM)))
        self.semantic_strong_margin = float(max(self.semantic_min_margin, min(1.0, ALIAS_SEMANTIC_STRONG_MARGIN)))
        self.semantic_min_priority = max(1, int(ALIAS_SEMANTIC_MIN_PRIORITY))
        self.semantic_max_phrases = max(1, int(ALIAS_SEMANTIC_MAX_PHRASES))
        self.semantic_max_hits = max(1, int(ALIAS_SEMANTIC_MAX_HITS))
        self._semantic_query_cache: dict[str, list[float] | None] = {}
        self._semantic_alias_entries: list[IndustryAliasEntry] = []
        self._semantic_alias_vectors: list[list[float]] = []
        self.semantic_taxonomy_enabled = bool(SEMANTIC_TAXONOMY_ENABLED)
        self.semantic_taxonomy_min_sim = float(max(0.0, min(1.0, SEMANTIC_TAXONOMY_MIN_SIM)))
        self.semantic_taxonomy_min_margin = float(max(0.0, min(1.0, SEMANTIC_TAXONOMY_MIN_MARGIN)))
        self.semantic_taxonomy_max_hits = max(1, int(SEMANTIC_TAXONOMY_MAX_HITS))
        self._semantic_taxonomy_codes: list[str] = []
        self._semantic_taxonomy_vectors: list[list[float]] = []
        self._semantic_taxonomy_proto: dict[str, str] = {}
        if self.loaded:
            self._load_alias_entries()
            self._load_memory_entries()
            self._init_semantic_alias_matcher()

    def _init_semantic_alias_matcher(self) -> None:
        self._semantic_alias_entries = []
        self._semantic_alias_vectors = []
        self._semantic_query_cache = {}
        self._semantic_taxonomy_codes = []
        self._semantic_taxonomy_vectors = []
        self._semantic_taxonomy_proto = {}

        need_embedder = bool(self.semantic_enabled or self.semantic_taxonomy_enabled)
        if not need_embedder:
            return

        if self.semantic_embedder is None:
            provider = self.semantic_provider
            if provider == "auto":
                provider = "gemini" if (DEFAULT_GEMINI_API_KEY and _genai is not None) else "off"
            if provider == "gemini":
                emb = _GeminiSemanticEmbedder(DEFAULT_GEMINI_API_KEY, DEFAULT_ALIAS_EMBED_MODEL)
                if emb.available:
                    self.semantic_embedder = emb
                else:
                    log.warning("Industry semantic alias disabled: gemini embedder unavailable.")
                    self.semantic_embedder = None
            elif provider == "ngram":
                self.semantic_embedder = _NgramSemanticEmbedder()
            else:
                self.semantic_embedder = None

        if self.semantic_embedder is None:
            return
        if self.semantic_enabled:
            self._build_semantic_alias_index()
        if self.semantic_taxonomy_enabled:
            self._build_semantic_taxonomy_index()

    def _semantic_prototype_text(self, entry: IndustryAliasEntry) -> str:
        target_name = (
            self.taxonomy.detail_names.get(entry.target_minor_code)
            or self.taxonomy.minor_names.get(entry.target_minor_code)
            or ""
        )
        major_code = self._resolve_target_major_code(entry.target_minor_code)
        major_name = self.taxonomy.major_names.get(major_code, "") if major_code else ""
        parts = [entry.alias, target_name, major_name]
        uniq: list[str] = []
        seen: set[str] = set()
        for p in parts:
            t = str(p or "").strip()
            if not t:
                continue
            n = norm_text_compact(t)
            if not n or n in seen:
                continue
            seen.add(n)
            uniq.append(t)
        return " ".join(uniq)

    def _build_semantic_alias_index(self) -> None:
        self._semantic_alias_entries = []
        self._semantic_alias_vectors = []
        if self.semantic_embedder is None:
            return

        seed_entries: list[IndustryAliasEntry] = []
        texts: list[str] = []
        seen: set[tuple[str, str]] = set()
        for entry in self._alias_corpus():
            if not entry.target_minor_code:
                continue
            if int(entry.priority) < int(self.semantic_min_priority):
                continue
            key = (entry.alias_norm, entry.target_minor_code)
            if key in seen:
                continue
            seen.add(key)
            proto = self._semantic_prototype_text(entry)
            if not proto:
                continue
            seed_entries.append(entry)
            texts.append(proto)

        if not seed_entries:
            return

        vectors = self.semantic_embedder.embed_texts(texts)
        for entry, vec in zip(seed_entries, vectors):
            if not vec:
                continue
            norm_vec = _normalize_vector(vec)
            if not norm_vec:
                continue
            self._semantic_alias_entries.append(entry)
            self._semantic_alias_vectors.append(norm_vec)

        if self._semantic_alias_entries:
            log.info(
                "Industry semantic alias index ready: %s prototypes (provider=%s).",
                len(self._semantic_alias_entries),
                self.semantic_provider,
            )

    @staticmethod
    def _dedupe_alias_hits(hits: list[IndustryAliasEntry]) -> list[IndustryAliasEntry]:
        out: list[IndustryAliasEntry] = []
        seen: set[tuple[str, str]] = set()
        for h in hits:
            key = (h.alias_norm, h.target_minor_code)
            if key in seen:
                continue
            seen.add(key)
            out.append(h)
        return out

    def _extract_semantic_phrases(self, text: str) -> list[str]:
        spaced = norm_text(text)
        if not spaced:
            return []
        tokens = [t.strip() for t in spaced.split(" ") if 2 <= len(t.strip()) <= 24]
        tokens = [t for t in tokens if t and t not in _GENERIC_TOKENS]
        if not tokens:
            return []
        out: list[str] = []
        seen: set[str] = set()

        def push(phrase: str) -> None:
            p = str(phrase or "").strip()
            if len(p) < 2 or len(p) > 36:
                return
            n = norm_text_compact(p)
            if not n or n in seen:
                return
            seen.add(n)
            out.append(p)

        compact = "".join(tokens)[:36]
        push(compact)
        for tok in tokens:
            push(tok)
        for i in range(0, len(tokens) - 1):
            push(tokens[i] + tokens[i + 1])
            if i + 2 < len(tokens):
                push(tokens[i] + tokens[i + 1] + tokens[i + 2])
            if len(out) >= self.semantic_max_phrases:
                break

        return out[: self.semantic_max_phrases]

    def _embed_query_phrase(self, phrase: str) -> list[float] | None:
        key = norm_text_compact(phrase)
        if not key:
            return None
        if key in self._semantic_query_cache:
            return self._semantic_query_cache[key]
        if self.semantic_embedder is None:
            self._semantic_query_cache[key] = None
            return None
        vecs = self.semantic_embedder.embed_texts([phrase])
        vec = _normalize_vector(vecs[0] if vecs else None)
        self._semantic_query_cache[key] = vec
        return vec

    def _semantic_alias_hits_for_text(self, text: str) -> list[IndustryAliasEntry]:
        if (
            not text
            or not self.semantic_enabled
            or self.semantic_embedder is None
            or not self._semantic_alias_entries
            or not self._semantic_alias_vectors
        ):
            return []

        phrases = self._extract_semantic_phrases(text)
        if not phrases:
            return []

        hits: list[IndustryAliasEntry] = []
        for phrase in phrases:
            qvec = self._embed_query_phrase(phrase)
            if not qvec:
                continue
            best_idx = -1
            best_sim = -1.0
            second_sim = -1.0
            for idx, avec in enumerate(self._semantic_alias_vectors):
                sim = _cosine_similarity(qvec, avec)
                if sim > best_sim:
                    second_sim = best_sim
                    best_sim = sim
                    best_idx = idx
                elif sim > second_sim:
                    second_sim = sim

            if best_idx < 0 or best_sim < self.semantic_min_sim:
                continue
            margin = best_sim - max(second_sim, -1.0)
            if margin < self.semantic_min_margin:
                continue

            base = self._semantic_alias_entries[best_idx]
            is_strong = (
                best_sim >= self.semantic_strong_sim
                and margin >= self.semantic_strong_margin
                and int(base.priority) >= 8
                and (not base.requires_review)
            )
            requires_review = (not is_strong) or bool(base.requires_review)
            priority = int(base.priority) if is_strong else max(1, int(base.priority) - 2)
            note_marker = f"semantic_match:{best_sim:.3f}"
            notes = self._append_alias_note(base.notes, note_marker)
            hits.append(
                IndustryAliasEntry(
                    alias=f"[semantic]{phrase}->{base.alias}",
                    target_minor_code=base.target_minor_code,
                    priority=priority,
                    requires_review=requires_review,
                    domain_tag=base.domain_tag,
                    allowed_major_codes=base.allowed_major_codes,
                    notes=notes,
                    alias_norm=norm_text_compact(f"semantic:{phrase}:{base.alias_norm}:{base.target_minor_code}"),
                )
            )
            if len(hits) >= self.semantic_max_hits:
                break

        hits.sort(
            key=lambda x: (
                int(x.requires_review),
                -int(x.priority),
                x.target_minor_code,
                x.alias_norm,
            )
        )
        return self._dedupe_alias_hits(hits)

    def _semantic_taxonomy_prototype_text(self, minor_code: str) -> str:
        major_code, major_name, _mid, middle_name, _minor_code, minor_name = self.taxonomy.resolve_hierarchy(minor_code)
        parts = [minor_name, middle_name, major_name]
        alias_candidates: list[IndustryAliasEntry] = []
        for entry in self._alias_corpus():
            target = str(entry.target_minor_code or "").strip()
            if not target or entry.requires_review:
                continue
            if target in self.taxonomy.detail_names:
                _m1, _m1n, _m2, _m2n, resolved_minor, _m3n, _dc, _dn = self.taxonomy.resolve_detail_hierarchy(target)
                target = resolved_minor
            if target != minor_code:
                continue
            alias_candidates.append(entry)
        alias_candidates.sort(key=lambda x: (-int(x.priority), -len(x.alias_norm), x.alias_norm))
        for entry in alias_candidates[:4]:
            parts.append(entry.alias)

        uniq: list[str] = []
        seen: set[str] = set()
        for part in parts:
            t = str(part or "").strip()
            if not t:
                continue
            n = norm_text_compact(t)
            if not n or n in seen:
                continue
            seen.add(n)
            uniq.append(t)
        return " ".join(uniq)

    def _build_semantic_taxonomy_index(self) -> None:
        self._semantic_taxonomy_codes = []
        self._semantic_taxonomy_vectors = []
        self._semantic_taxonomy_proto = {}
        if (
            not self.semantic_taxonomy_enabled
            or self.semantic_embedder is None
            or not self.taxonomy.minor_names
        ):
            return

        codes: list[str] = []
        texts: list[str] = []
        for code in sorted(self.taxonomy.minor_names.keys()):
            proto = self._semantic_taxonomy_prototype_text(code)
            if not proto:
                continue
            codes.append(code)
            texts.append(proto)
            self._semantic_taxonomy_proto[code] = proto
        if not texts:
            return

        vecs = self.semantic_embedder.embed_texts(texts)
        for code, vec in zip(codes, vecs):
            norm_vec = _normalize_vector(vec)
            if not norm_vec:
                continue
            self._semantic_taxonomy_codes.append(code)
            self._semantic_taxonomy_vectors.append(norm_vec)
        if self._semantic_taxonomy_codes:
            log.info(
                "Industry semantic taxonomy index ready: %s minors (provider=%s).",
                len(self._semantic_taxonomy_codes),
                self.semantic_provider,
            )

    def _semantic_taxonomy_hits_for_text(self, text: str, *, source: str) -> list[SemanticTaxonomyCandidate]:
        if (
            not text
            or not self.semantic_taxonomy_enabled
            or self.semantic_embedder is None
            or not self._semantic_taxonomy_codes
            or not self._semantic_taxonomy_vectors
        ):
            return []
        phrases = self._extract_semantic_phrases(text)
        if not phrases:
            return []
        best_by_minor: dict[str, SemanticTaxonomyCandidate] = {}
        for phrase in phrases:
            qvec = self._embed_query_phrase(phrase)
            if not qvec:
                continue
            best_idx = -1
            best_sim = -1.0
            second_sim = -1.0
            for idx, avec in enumerate(self._semantic_taxonomy_vectors):
                sim = _cosine_similarity(qvec, avec)
                if sim > best_sim:
                    second_sim = best_sim
                    best_sim = sim
                    best_idx = idx
                elif sim > second_sim:
                    second_sim = sim
            if best_idx < 0 or best_sim < self.semantic_taxonomy_min_sim:
                continue
            margin = best_sim - max(second_sim, -1.0)
            if margin < self.semantic_taxonomy_min_margin:
                continue
            code = self._semantic_taxonomy_codes[best_idx]
            cand = SemanticTaxonomyCandidate(
                minor_code=code,
                sim=float(best_sim),
                margin=float(margin),
                phrase=phrase,
                source=source,
            )
            prev = best_by_minor.get(code)
            if prev is None or cand.sim > prev.sim:
                best_by_minor[code] = cand
        if not best_by_minor:
            return []
        ranked = sorted(
            best_by_minor.values(),
            key=lambda x: (-float(x.sim), -float(x.margin), x.minor_code),
        )
        return ranked[: self.semantic_taxonomy_max_hits]

    def classify_from_semantic_taxonomy(self, description: str, business_tags: Any) -> Optional[dict[str, Any]]:
        if (
            not self.loaded
            or not self.semantic_taxonomy_enabled
            or self.semantic_embedder is None
            or not self._semantic_taxonomy_codes
        ):
            return None
        desc_hits = self._semantic_taxonomy_hits_for_text(str(description or ""), source="desc")
        tag_values = self._iter_tag_values(business_tags)
        tag_hits: list[SemanticTaxonomyCandidate] = []
        for tag in tag_values:
            tag_hits.extend(self._semantic_taxonomy_hits_for_text(tag, source="tag"))
        if not desc_hits and not tag_hits:
            return None

        context_text = "\n".join([str(description or "")] + tag_values).strip()
        best_major, best_major_score, best_major_margin = self._infer_major_context_from_text(context_text)
        if best_major and best_major_score >= 3 and best_major_margin >= 2:
            desc_hits = [
                h
                for h in desc_hits
                if self._resolve_target_major_code(h.minor_code) in {"", best_major}
            ]
            tag_hits = [
                h
                for h in tag_hits
                if self._resolve_target_major_code(h.minor_code) in {"", best_major}
            ]
            if not desc_hits and not tag_hits:
                return None

        scores: dict[str, dict[str, Any]] = {}

        def apply_hit(hit: SemanticTaxonomyCandidate) -> None:
            slot = scores.setdefault(
                hit.minor_code,
                {
                    "score": 0.0,
                    "desc_hits": 0,
                    "tag_hits": 0,
                    "best_sim": 0.0,
                    "best_margin": 0.0,
                    "phrases": set(),
                },
            )
            boost = float(hit.sim) + (float(hit.margin) * 0.45)
            if hit.source == "tag":
                boost += 0.06
                slot["tag_hits"] = int(slot.get("tag_hits") or 0) + 1
            else:
                slot["desc_hits"] = int(slot.get("desc_hits") or 0) + 1
            slot["score"] = float(slot.get("score") or 0.0) + boost
            slot["best_sim"] = max(float(slot.get("best_sim") or 0.0), float(hit.sim))
            slot["best_margin"] = max(float(slot.get("best_margin") or 0.0), float(hit.margin))
            phrase_set = slot.get("phrases")
            if isinstance(phrase_set, set):
                phrase_set.add(str(hit.phrase))

        for hit in desc_hits:
            apply_hit(hit)
        for hit in tag_hits:
            apply_hit(hit)
        if not scores:
            return None

        ranked = sorted(
            scores.items(),
            key=lambda x: (
                -float(x[1].get("score") or 0.0),
                -float(x[1].get("best_sim") or 0.0),
                -int(x[1].get("tag_hits") or 0),
                x[0],
            ),
        )
        best_code, best_meta = ranked[0]
        candidate = self._resolve_candidate_from_code(best_code)
        if not candidate:
            return None

        second_score = float(ranked[1][1].get("score") or 0.0) if len(ranked) >= 2 else 0.0
        best_score = float(best_meta.get("score") or 0.0)
        score_margin = max(0.0, best_score - second_score)
        desc_count = int(best_meta.get("desc_hits") or 0)
        tag_count = int(best_meta.get("tag_hits") or 0)
        both_sources = desc_count > 0 and tag_count > 0
        best_sim = float(best_meta.get("best_sim") or 0.0)
        best_margin = float(best_meta.get("best_margin") or 0.0)

        require_both = bool(SEMANTIC_TAXONOMY_REQUIRE_BOTH)
        confidence = 0.38 + min(0.30, best_score * 0.20) + min(0.12, score_margin * 0.30)
        if both_sources:
            confidence += 0.06
        if best_sim >= 0.97:
            confidence += 0.04
        confidence = float(max(0.0, min(0.85, confidence)))

        def _normalize_minor(code_val: str) -> str:
            code_val = str(code_val or "").strip()
            if not code_val:
                return ""
            if code_val in self.taxonomy.detail_names:
                return self.taxonomy.detail_to_minor.get(code_val, "")
            return code_val

        target_minor = _normalize_minor(best_code)
        context_norm = self.taxonomy._normalize(context_text)

        def _has_alias_support(text: str) -> bool:
            if not text or not target_minor:
                return False
            for hit in self._alias_hits_for_text(text):
                hit_code = _normalize_minor(hit.target_minor_code)
                if hit_code and hit_code == target_minor:
                    return True
            return False

        lexical_support = False
        if target_minor:
            if _has_alias_support(str(description or "")):
                lexical_support = True
            if not lexical_support:
                for tag in tag_values:
                    if _has_alias_support(tag):
                        lexical_support = True
                        break
            if not lexical_support:
                minor_name = self.taxonomy.minor_names.get(target_minor, "")
                minor_norm = self.taxonomy._normalize(minor_name) if minor_name else ""
                if minor_norm and context_norm and (minor_norm in context_norm):
                    lexical_support = True

        ambiguous = score_margin < 0.08 or best_margin < (self.semantic_taxonomy_min_margin + 0.02)
        review_required = bool((not both_sources) or ambiguous or confidence <= 0.56)
        if require_both and not both_sources and not lexical_support:
            review_required = True
        if lexical_support and (not ambiguous) and confidence >= 0.56:
            review_required = False
        if review_required:
            confidence = min(confidence, 0.56 if both_sources else 0.50)
        if require_both and not both_sources and not lexical_support:
            confidence = min(confidence, 0.50)

        source_name = "semantic_taxonomy_desc_tags" if both_sources else "semantic_taxonomy_single"
        return {
            **candidate,
            "confidence": confidence,
            "source": source_name,
            "review_required": review_required,
            "semantic_taxonomy_only": True,
            "semantic_taxonomy_score": best_score,
            "semantic_taxonomy_score_margin": score_margin,
            "semantic_taxonomy_best_sim": best_sim,
            "semantic_taxonomy_best_margin": best_margin,
            "semantic_taxonomy_phrases": sorted(list(best_meta.get("phrases") or [])),
            "semantic_taxonomy_desc_hits": desc_count,
            "semantic_taxonomy_tag_hits": tag_count,
        }

    def _read_alias_rows(self, path: str) -> list[dict[str, str]]:
        for enc in ("utf-8-sig", "cp932", "shift_jis"):
            try:
                with open(path, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    return [dict(row) for row in reader]
            except Exception:
                continue
        return []

    def _fallback_alias_rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for alias, (target, priority, requires_review, notes) in _ALIAS_TO_MINOR_FALLBACK.items():
            rows.append(
                {
                    "alias": alias,
                    "target_minor_code": target,
                    "priority": str(priority),
                    "requires_review": str(requires_review),
                    "domain_tag": notes,
                    "allowed_major_codes": "",
                    "notes": notes,
                }
            )
        return rows

    @staticmethod
    def _append_alias_note(notes: str, marker: str) -> str:
        marker = str(marker or "").strip()
        if not marker:
            return notes
        current = str(notes or "").strip()
        if not current:
            return marker
        parts = [p.strip() for p in current.split("|") if p.strip()]
        if marker in parts:
            return current
        return f"{current}|{marker}"

    @staticmethod
    def _parse_allowed_major_codes(raw: str) -> tuple[str, ...]:
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

    def _resolve_target_major_code(self, target_code: str) -> str:
        target = str(target_code or "").strip()
        if not target:
            return ""
        if target in self.taxonomy.minor_names:
            major_code, *_ = self.taxonomy.resolve_hierarchy(target)
            return major_code
        if target in self.taxonomy.detail_names:
            major_code, *_ = self.taxonomy.resolve_detail_hierarchy(target)
            return major_code
        return ""

    def _normalize_alias_row(self, row: dict[str, str]) -> IndustryAliasEntry | None:
        alias = str(row.get("alias") or "").strip()
        alias_norm = norm_text_compact(alias)
        if not alias_norm:
            return None

        target_minor_code = str(row.get("target_minor_code") or "").strip()
        try:
            priority = int(str(row.get("priority") or "1").strip())
        except Exception:
            priority = 1
        priority = max(1, priority)

        requires_review_raw = str(row.get("requires_review") or "0").strip().lower()
        requires_review = requires_review_raw in {"1", "true", "yes"}
        domain_tag = str(row.get("domain_tag") or row.get("notes") or "").strip()
        allowed_major_codes = self._parse_allowed_major_codes(str(row.get("allowed_major_codes") or ""))
        notes = str(row.get("notes") or "").strip()

        valid_target = bool(
            target_minor_code
            and (
                target_minor_code in self.taxonomy.minor_names
                or target_minor_code in self.taxonomy.detail_names
            )
        )
        if target_minor_code and not valid_target:
            requires_review = True
            notes = self._append_alias_note(notes, "unknown_minor_code")
            target_minor_code = ""
        elif target_minor_code:
            target_major_code = self._resolve_target_major_code(target_minor_code)
            if target_major_code and not allowed_major_codes:
                allowed_major_codes = (target_major_code,)
            if target_major_code and allowed_major_codes and target_major_code not in allowed_major_codes:
                requires_review = True
                notes = self._append_alias_note(notes, "allowed_major_mismatch")
                # target major mismatch rows are disabled to suppress false positives.
                target_minor_code = ""

        return IndustryAliasEntry(
            alias=alias,
            target_minor_code=target_minor_code,
            priority=priority,
            requires_review=requires_review,
            domain_tag=domain_tag,
            allowed_major_codes=allowed_major_codes,
            notes=notes,
            alias_norm=alias_norm,
        )

    def _load_alias_entries(self) -> None:
        rows: list[dict[str, str]] = []
        if self.aliases_csv_path and os.path.exists(self.aliases_csv_path):
            rows = self._read_alias_rows(self.aliases_csv_path)
            required = {"alias", "target_minor_code", "priority", "requires_review"}
            if rows and not required.issubset(set(rows[0].keys())):
                rows = []
            if rows:
                self.alias_source = "csv"

        if not rows:
            rows = self._fallback_alias_rows()
            self.alias_source = "fallback"

        entries: list[IndustryAliasEntry] = []
        for row in rows:
            entry = self._normalize_alias_row(row)
            if entry is None:
                continue
            entries.append(entry)

        entries.sort(key=lambda x: (-x.priority, -len(x.alias_norm), x.alias_norm, x.target_minor_code))
        self.alias_entries = entries
        log.info("Industry aliases loaded: %s entries (source=%s)", len(entries), self.alias_source)

    def _alias_corpus(self) -> list[IndustryAliasEntry]:
        if not self.learned_alias_entries:
            return self.alias_entries
        return self.alias_entries + self.learned_alias_entries

    def _known_alias_norms(self) -> set[str]:
        norms = {e.alias_norm for e in self._alias_corpus() if e.alias_norm}
        norms.update(self.taxonomy.normalized_name_index.keys())
        return norms

    def _load_memory_entries(self) -> None:
        self.memory_entries = {}
        self.learned_alias_entries = []
        path = str(self.memory_csv_path or "").strip()
        if not path or (not os.path.exists(path)):
            return

        rows: list[dict[str, str]] = []
        for enc in ("utf-8-sig", "cp932", "shift_jis"):
            try:
                with open(path, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    rows = [dict(row) for row in reader]
                    break
            except Exception:
                continue
        if not rows:
            return

        for row in rows:
            term = str(row.get("term") or row.get("alias") or "").strip()
            term_norm = norm_text_compact(term)
            target = str(row.get("target_minor_code") or "").strip()
            if not term_norm or not target:
                continue
            if target in self.taxonomy.detail_names:
                _maj, _maj_name, _mid, _mid_name, resolved_minor, _minor_name, _d, _dn = self.taxonomy.resolve_detail_hierarchy(target)
                if resolved_minor:
                    target = resolved_minor
            if target not in self.taxonomy.minor_names:
                continue
            try:
                count = int(str(row.get("count") or "1").strip())
            except Exception:
                count = 1
            try:
                confidence = float(str(row.get("confidence") or row.get("last_confidence") or "0.0").strip())
            except Exception:
                confidence = 0.0
            source = str(row.get("source") or row.get("last_source") or "auto").strip()
            updated_at = str(row.get("updated_at") or "").strip() or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            key = (term_norm, target)
            existing = self.memory_entries.get(key)
            if existing:
                existing.count = max(existing.count, max(1, count))
                existing.confidence = max(existing.confidence, confidence)
                if term and (len(term) > len(existing.term)):
                    existing.term = term
                if source and source != "auto":
                    existing.source = source
                existing.updated_at = updated_at
            else:
                self.memory_entries[key] = IndustryMemoryEntry(
                    term=term,
                    target_minor_code=target,
                    count=max(1, count),
                    confidence=float(max(0.0, min(1.0, confidence))),
                    source=source or "auto",
                    updated_at=updated_at,
                    term_norm=term_norm,
                )
        self._rebuild_learned_alias_entries()
        if self.learned_alias_entries:
            log.info("Industry auto-learn memory loaded: %s entries", len(self.learned_alias_entries))

    def _rebuild_learned_alias_entries(self) -> None:
        entries: list[IndustryAliasEntry] = []
        for mem in self.memory_entries.values():
            if not mem.target_minor_code:
                continue
            target_major = self._resolve_target_major_code(mem.target_minor_code)
            requires_review = bool(
                mem.count < self.auto_learn_min_count
                or mem.confidence < max(0.65, self.auto_learn_min_confidence - 0.08)
            )
            priority = 2 + min(4, int(mem.count))
            if mem.confidence >= 0.90:
                priority += 1
            if not requires_review:
                priority = max(priority, 5)
            priority = int(max(1, min(8, priority)))
            notes = f"auto_learn|count:{int(mem.count)}|conf:{float(mem.confidence):.3f}|src:{mem.source}"
            entries.append(
                IndustryAliasEntry(
                    alias=mem.term,
                    target_minor_code=mem.target_minor_code,
                    priority=priority,
                    requires_review=requires_review,
                    domain_tag="auto",
                    allowed_major_codes=(target_major,) if target_major else (),
                    notes=notes,
                    alias_norm=mem.term_norm,
                )
            )
        entries.sort(key=lambda x: (-x.priority, -len(x.alias_norm), x.alias_norm, x.target_minor_code))
        self.learned_alias_entries = entries

    def _flush_memory_entries(self) -> None:
        path = str(self.memory_csv_path or "").strip()
        if not path:
            return
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        rows = sorted(
            self.memory_entries.values(),
            key=lambda x: (-int(x.count), -float(x.confidence), x.target_minor_code, x.term_norm),
        )
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["term", "target_minor_code", "count", "confidence", "source", "updated_at"])
            for mem in rows:
                w.writerow(
                    [
                        mem.term,
                        mem.target_minor_code,
                        int(mem.count),
                        f"{float(mem.confidence):.4f}",
                        mem.source,
                        mem.updated_at,
                    ]
                )

    @staticmethod
    def _is_auto_learn_source_allowed(source: str) -> bool:
        s = str(source or "").strip().lower()
        if not s:
            return False
        if "unclassified" in s:
            return False
        if s.startswith("alias"):
            return False
        if s.startswith("forced"):
            return False
        if "semantic" in s:
            return False
        return True

    def _extract_auto_learn_terms(self, text_blocks: list[str]) -> list[str]:
        if not text_blocks:
            return []
        known_aliases = self._known_alias_norms()
        token_counts: dict[str, int] = {}
        token_surface: dict[str, str] = {}
        for block in text_blocks:
            text = norm_text(block)
            if not text:
                continue
            for m in _AUTO_TERM_RE.finditer(text):
                term = str(m.group(0) or "").strip()
                if not term:
                    continue
                if term.isdigit():
                    continue
                if term in _AUTO_LEARN_STOPWORDS:
                    continue
                term_norm = norm_text_compact(term)
                if not term_norm:
                    continue
                if len(term_norm) < self.auto_learn_min_term_len or len(term_norm) > self.auto_learn_max_term_len:
                    continue
                if term_norm in _AUTO_LEARN_STOPWORDS:
                    continue
                if _AUTO_LEARN_VERB_RE.search(term):
                    continue
                if re.fullmatch(r"[ぁ-ん]+", term):
                    continue
                if not re.search(r"[一-龥ァ-ンA-Za-z]", term):
                    continue
                if term_norm in known_aliases:
                    continue
                token_counts[term_norm] = token_counts.get(term_norm, 0) + 1
                prev = token_surface.get(term_norm, "")
                if not prev or len(term) > len(prev):
                    token_surface[term_norm] = term
        if not token_counts:
            return []
        ranked = sorted(
            token_counts.items(),
            key=lambda x: (-int(x[1]), -len(x[0]), x[0]),
        )
        out: list[str] = []
        for term_norm, _count in ranked:
            term = token_surface.get(term_norm) or term_norm
            out.append(term)
            if len(out) >= self.auto_learn_max_terms:
                break
        return out

    def auto_learn_from_result(
        self,
        *,
        description: str,
        business_tags: Any,
        result: dict[str, Any] | None,
        company_name: str = "",
    ) -> int:
        if not self.loaded or not self.auto_learn_enabled:
            return 0
        if not isinstance(result, dict):
            return 0
        if bool(result.get("review_required")):
            return 0
        source = str(result.get("source") or "").strip()
        if not self._is_auto_learn_source_allowed(source):
            return 0

        target_minor_code = str(result.get("minor_code") or "").strip()
        if not target_minor_code:
            return 0
        if target_minor_code in self.taxonomy.detail_names:
            _maj, _maj_name, _mid, _mid_name, resolved_minor, _minor_name, _d, _dn = self.taxonomy.resolve_detail_hierarchy(target_minor_code)
            if resolved_minor:
                target_minor_code = resolved_minor
        if target_minor_code not in self.taxonomy.minor_names:
            return 0

        try:
            confidence = float(result.get("confidence") or 0.0)
        except Exception:
            confidence = 0.0
        confidence = float(max(0.0, min(1.0, confidence)))
        if confidence < self.auto_learn_min_confidence:
            return 0

        blocks: list[str] = []
        if description:
            blocks.append(str(description))
        tag_values = self._iter_tag_values(business_tags)
        if tag_values:
            blocks.extend(tag_values)
        terms = self._extract_auto_learn_terms(blocks)
        if not terms:
            return 0

        changed = 0
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for term in terms:
            term_norm = norm_text_compact(term)
            if not term_norm:
                continue
            key = (term_norm, target_minor_code)
            existing = self.memory_entries.get(key)
            if existing is None:
                self.memory_entries[key] = IndustryMemoryEntry(
                    term=term,
                    target_minor_code=target_minor_code,
                    count=1,
                    confidence=confidence,
                    source=source,
                    updated_at=now,
                    term_norm=term_norm,
                )
                changed += 1
                continue

            existing.count = max(1, int(existing.count) + 1)
            if confidence >= existing.confidence:
                existing.source = source
            existing.confidence = max(existing.confidence, confidence)
            if len(term) > len(existing.term):
                existing.term = term
            existing.updated_at = now
            changed += 1

        if changed > 0:
            self._rebuild_learned_alias_entries()
            self._flush_memory_entries()
            self._build_semantic_taxonomy_index()
        return changed

    def auto_learn_from_company(self, company: dict[str, Any], *, status: str = "") -> int:
        if not isinstance(company, dict):
            return 0
        if status and status != "done":
            return 0
        try:
            confidence = float(company.get("industry_class_confidence") or 0.0)
        except Exception:
            confidence = 0.0
        result = {
            "minor_code": str(company.get("industry_minor_code") or "").strip(),
            "confidence": confidence,
            "source": str(company.get("industry_class_source") or "").strip(),
            "review_required": bool(status == "review"),
        }
        return self.auto_learn_from_result(
            description=str(company.get("description") or ""),
            business_tags=company.get("business_tags"),
            result=result,
            company_name=str(company.get("company_name") or ""),
        )

    def _alias_hits_for_text(self, text: str) -> list[IndustryAliasEntry]:
        entries = self._alias_corpus()
        if not text or not entries:
            return []
        spaced = norm_text(text)
        compact = spaced.replace(" ", "")
        if not compact:
            return []

        hits: list[IndustryAliasEntry] = []
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            if not entry.alias_norm:
                continue
            if re.fullmatch(r"[a-z0-9]+", entry.alias_norm):
                pat = re.compile(rf"(?<![a-z0-9]){re.escape(entry.alias_norm)}(?![a-z0-9])")
                if not pat.search(compact):
                    continue
            else:
                if entry.alias_norm not in compact:
                    continue
            key = (entry.alias_norm, entry.target_minor_code)
            if key in seen:
                continue
            seen.add(key)
            hits.append(entry)
        return hits

    def _infer_major_context_from_text(self, text: str) -> tuple[str, int, int]:
        if not text:
            return "", 0, 0
        major_scores: dict[str, int] = {}

        minor_scores = self.taxonomy.score_minors(text)
        for minor_code, score in minor_scores.items():
            middle_code = self.taxonomy.minor_to_middle.get(minor_code, "")
            major_code = self.taxonomy.middle_to_major.get(middle_code, "") if middle_code else ""
            if not major_code:
                continue
            major_scores[major_code] = major_scores.get(major_code, 0) + int(score)

        norm_compact = self.taxonomy._normalize(text)
        if norm_compact:
            for major_code, major_name in self.taxonomy.major_names.items():
                major_name_norm = self.taxonomy._normalize(major_name)
                if major_name_norm and major_name_norm in norm_compact:
                    major_scores[major_code] = major_scores.get(major_code, 0) + 3

        if not major_scores:
            return "", 0, 0
        ranked = sorted(major_scores.items(), key=lambda x: (-x[1], x[0]))
        best_major, best_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) >= 2 else 0
        margin = max(0, int(best_score) - int(second_score))
        return best_major, int(best_score), margin

    def _filter_alias_hits_by_major_context(self, hits: list[IndustryAliasEntry], text: str) -> list[IndustryAliasEntry]:
        if not hits:
            return hits
        best_major, best_score, margin = self._infer_major_context_from_text(text)
        # Apply guard only when text has a clear major-level signal.
        if not best_major or best_score < 3 or margin < 2:
            return hits

        filtered: list[IndustryAliasEntry] = []
        for hit in hits:
            if hit.allowed_major_codes and best_major not in hit.allowed_major_codes:
                continue
            filtered.append(hit)
        return filtered

    def _iter_tag_values(self, business_tags: Any) -> list[str]:
        if business_tags is None:
            return []
        if isinstance(business_tags, (list, tuple, set)):
            return [str(v).strip() for v in business_tags if str(v).strip()]

        text = str(business_tags).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                pass
        parts = re.split(r"[\n,、/|]+", text)
        return [p.strip() for p in parts if p.strip()]

    def _resolve_candidate_from_code(self, code: str) -> Optional[dict[str, str]]:
        target = str(code or "").strip()
        if not target:
            return None
        if target in self.taxonomy.detail_names:
            (
                major_code,
                major_name,
                middle_code,
                middle_name,
                _minor_code,
                _minor_name,
                detail_code,
                detail_name,
            ) = self.taxonomy.resolve_detail_hierarchy(target)
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

        if target in self.taxonomy.minor_names:
            major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(target)
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
        return None

    def classify_from_aliases(self, description: str, business_tags: Any) -> Optional[dict[str, Any]]:
        if not self.loaded or not self.alias_entries:
            return None

        desc_hits = self._alias_hits_for_text(description or "")
        desc_hits.extend(self._semantic_alias_hits_for_text(description or ""))
        tag_values = self._iter_tag_values(business_tags)
        tag_hits: list[IndustryAliasEntry] = []
        for tag in tag_values:
            tag_hits.extend(self._alias_hits_for_text(tag))
            tag_hits.extend(self._semantic_alias_hits_for_text(tag))
        desc_hits = self._dedupe_alias_hits(desc_hits)
        tag_hits = self._dedupe_alias_hits(tag_hits)
        context_text = "\n".join([description or ""] + tag_values).strip()
        desc_hits = self._filter_alias_hits_by_major_context(desc_hits, context_text)
        tag_hits = self._filter_alias_hits_by_major_context(tag_hits, context_text)

        scores: dict[str, dict[str, Any]] = {}

        def apply_hit(entry: IndustryAliasEntry, source: str) -> None:
            if not entry.target_minor_code:
                return
            is_semantic = str(entry.alias or "").startswith("[semantic]")
            slot = scores.setdefault(
                entry.target_minor_code,
                {
                    "score": 0,
                    "desc_hits": 0,
                    "tag_hits": 0,
                    "semantic_hits": 0,
                    "max_priority": 0,
                    "requires_review": False,
                    "aliases": set(),
                    "domain_tags": set(),
                },
            )
            boost = max(1, int(entry.priority))
            if source == "tag":
                boost += 1
            if is_semantic:
                boost = min(boost, 3)
            if entry.requires_review:
                # Review-required aliases are intentionally weak to reduce false positives.
                boost = min(boost, 2)
            if source == "tag":
                slot["tag_hits"] = int(slot.get("tag_hits") or 0) + 1
            else:
                slot["desc_hits"] = int(slot.get("desc_hits") or 0) + 1
            if is_semantic:
                slot["semantic_hits"] = int(slot.get("semantic_hits") or 0) + 1
            slot["score"] = int(slot.get("score") or 0) + boost
            slot["max_priority"] = max(int(slot.get("max_priority") or 0), int(entry.priority))
            if entry.requires_review:
                slot["requires_review"] = True
            alias_set = slot.get("aliases")
            if isinstance(alias_set, set):
                alias_set.add(entry.alias)
            domain_tag_set = slot.get("domain_tags")
            if isinstance(domain_tag_set, set) and entry.domain_tag:
                domain_tag_set.add(entry.domain_tag)

        for hit in desc_hits:
            apply_hit(hit, "desc")
        for hit in tag_hits:
            apply_hit(hit, "tag")

        if not scores:
            return None

        ranked = sorted(
            scores.items(),
            key=lambda item: (
                -int(item[1].get("score") or 0),
                -int(item[1].get("tag_hits") or 0),
                -int(item[1].get("desc_hits") or 0),
                -int(item[1].get("max_priority") or 0),
                item[0],
            ),
        )
        best_code, best_meta = ranked[0]
        candidate = self._resolve_candidate_from_code(best_code)
        if not candidate:
            return None

        desc_count = int(best_meta.get("desc_hits") or 0)
        tag_count = int(best_meta.get("tag_hits") or 0)
        semantic_hits = int(best_meta.get("semantic_hits") or 0)
        lexical_hits = max(0, int(desc_count + tag_count - semantic_hits))
        semantic_only = semantic_hits > 0 and lexical_hits == 0
        both_sources = desc_count > 0 and tag_count > 0
        best_score = int(best_meta.get("score") or 0)
        best_domain_tags = {str(v) for v in (best_meta.get("domain_tags") or set()) if str(v)}
        # セマンティック単独は採用せず review 寄せ

        # aliasヒットが複数ドメインをまたぐ/僅差で競合する場合は誤分類リスクが高い。
        domain_conflict = len(best_domain_tags) >= 2
        if len(ranked) >= 2:
            second_code, second_meta = ranked[1]
            second_score = int(second_meta.get("score") or 0)
            score_close = second_score > 0 and (best_score - second_score) <= 1
            if score_close:
                second_candidate = self._resolve_candidate_from_code(second_code)
                second_domain_tags = {str(v) for v in (second_meta.get("domain_tags") or set()) if str(v)}
                if (
                    second_candidate
                    and str(second_candidate.get("major_code") or "")
                    and str(candidate.get("major_code") or "")
                    and str(second_candidate.get("major_code") or "") != str(candidate.get("major_code") or "")
                ):
                    domain_conflict = True
                elif second_domain_tags and best_domain_tags and second_domain_tags != best_domain_tags:
                    domain_conflict = True

        base_conf = 0.45 + 0.04 * min(5, best_score)
        if both_sources:
            confidence = min(0.72, max(0.55, base_conf))
        else:
            confidence = min(0.5, base_conf)
        if semantic_only:
            # semantic-only は誤爆防止を優先し、単独確信を抑える。
            confidence = min(confidence, 0.54 if both_sources else 0.46)
        if domain_conflict:
            confidence = min(confidence, 0.58)

        review_required = bool(best_meta.get("requires_review") or confidence <= 0.5 or domain_conflict or semantic_only)
        source_name = "alias_desc_tags" if both_sources else "alias_single"
        if semantic_only:
            source_name = "alias_semantic"

        return {
            **candidate,
            "confidence": float(max(0.0, min(1.0, confidence))),
            "source": source_name,
            "review_required": review_required,
            "alias_only": True,
            "alias_domain_conflict": domain_conflict,
            "alias_match_count": int(desc_count + tag_count),
            "alias_desc_hits": desc_count,
            "alias_tag_hits": tag_count,
            "alias_semantic_hits": semantic_hits,
            "alias_matches": sorted(list(best_meta.get("aliases") or [])),
            "alias_domain_tags": sorted(list(best_meta.get("domain_tags") or [])),
        }

    def match_tags_to_taxonomy(
        self,
        business_tags: Any,
        *,
        min_sim: float = 0.95,
        min_margin: float = 0.08,
        max_per_tag: int = 1,
    ) -> list[dict[str, Any]]:
        if (
            not self.loaded
            or not self.semantic_taxonomy_enabled
            or self.semantic_embedder is None
            or not self._semantic_taxonomy_codes
        ):
            return []
        tag_values = self._iter_tag_values(business_tags)
        if not tag_values:
            return []
        out: list[dict[str, Any]] = []
        for tag in tag_values:
            hits = self._semantic_taxonomy_hits_for_text(tag, source="tag")
            if not hits:
                continue
            picked = 0
            for hit in hits:
                if hit.sim < min_sim or hit.margin < min_margin:
                    continue
                candidate = self._resolve_candidate_from_code(hit.minor_code)
                if not candidate:
                    continue
                out.append(
                    {
                        "tag": str(tag),
                        **candidate,
                        "sim": float(hit.sim),
                        "margin": float(hit.margin),
                    }
                )
                picked += 1
                if picked >= max_per_tag:
                    break
        return out

    def score_levels(self, text_blocks: list[str]) -> dict[str, Any]:
        if not self.loaded:
            return {
                "use_detail": False,
                "detail_scores": {},
                "minor_scores": {},
                "middle_scores": {},
                "major_scores": {},
                "alias_matches": [],
                "alias_requires_review": False,
            }

        text = "\n".join([t for t in text_blocks if t])
        if not text:
            return {
                "use_detail": False,
                "detail_scores": {},
                "minor_scores": {},
                "middle_scores": {},
                "major_scores": {},
                "alias_matches": [],
                "alias_requires_review": False,
            }

        use_detail = bool(self.taxonomy.detail_names)
        detail_scores: dict[str, int] = {}
        if use_detail:
            detail_scores = self.taxonomy.score_details(text)

        if use_detail:
            minor_scores: dict[str, int] = {}
            for detail_code, score in detail_scores.items():
                minor_code = self.taxonomy.detail_to_minor.get(detail_code, "")
                if not minor_code:
                    continue
                minor_scores[minor_code] = minor_scores.get(minor_code, 0) + score
            if not minor_scores:
                minor_scores = self.taxonomy.score_minors(text)
        else:
            minor_scores = self.taxonomy.score_minors(text)

        norm_compact = self.taxonomy._normalize(text)
        tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンa-z0-9]{2,}", norm_compact):
            tok = m.group(0)
            if tok in _GENERIC_TOKENS:
                continue
            tokens.add(tok)

        def boost_scores(scores: dict[str, int], name_map: dict[str, str], base_boost: int = 3) -> None:
            if not norm_compact:
                return
            for code, name in name_map.items():
                name_norm = self.taxonomy._normalize(name)
                if not name_norm:
                    continue
                boost = 0
                if name_norm in norm_compact or norm_compact in name_norm:
                    boost += base_boost
                else:
                    for tok in tokens:
                        if tok in name_norm:
                            boost += 1
                if boost > 0:
                    scores[code] = scores.get(code, 0) + boost

        boost_scores(minor_scores, self.taxonomy.minor_names)
        if use_detail:
            boost_scores(detail_scores, self.taxonomy.detail_names)

        alias_hits = self._alias_hits_for_text(text)
        alias_hits.extend(self._semantic_alias_hits_for_text(text))
        alias_hits = self._dedupe_alias_hits(alias_hits)
        alias_hits = self._filter_alias_hits_by_major_context(alias_hits, text)
        for hit in alias_hits:
            if not hit.target_minor_code:
                continue
            boost = max(1, int(hit.priority))
            if str(hit.alias or "").startswith("[semantic]"):
                boost = min(boost, 3)
            if hit.requires_review:
                boost = min(boost, 2)
            target = hit.target_minor_code
            if target in self.taxonomy.detail_names:
                detail_scores[target] = detail_scores.get(target, 0) + boost
                mapped_minor = self.taxonomy.detail_to_minor.get(target, "")
                if mapped_minor:
                    minor_scores[mapped_minor] = minor_scores.get(mapped_minor, 0) + max(1, boost - 1)
            elif target in self.taxonomy.minor_names:
                minor_scores[target] = minor_scores.get(target, 0) + boost

        middle_scores: dict[str, int] = {}
        for minor_code, score in minor_scores.items():
            middle_code = self.taxonomy.minor_to_middle.get(minor_code, "")
            if not middle_code:
                continue
            middle_scores[middle_code] = middle_scores.get(middle_code, 0) + score

        major_scores: dict[str, int] = {}
        for middle_code, score in middle_scores.items():
            major_code = self.taxonomy.middle_to_major.get(middle_code, "")
            if not major_code:
                continue
            major_scores[major_code] = major_scores.get(major_code, 0) + score

        boost_scores(major_scores, self.taxonomy.major_names)
        boost_scores(middle_scores, self.taxonomy.middle_names)

        return {
            "use_detail": use_detail,
            "detail_scores": detail_scores,
            "minor_scores": minor_scores,
            "middle_scores": middle_scores,
            "major_scores": major_scores,
            "alias_matches": [
                {
                    "alias": h.alias,
                    "target_minor_code": h.target_minor_code,
                    "priority": h.priority,
                    "requires_review": int(h.requires_review),
                    "domain_tag": h.domain_tag,
                    "allowed_major_codes": list(h.allowed_major_codes),
                    "notes": h.notes,
                }
                for h in alias_hits
            ],
            "alias_requires_review": any(h.requires_review for h in alias_hits),
        }

    def build_level_candidates(
        self,
        level: str,
        scores: dict[str, Any],
        top_n: int = 12,
        major_code: str = "",
        middle_code: str = "",
        minor_code: str = "",
    ) -> list[dict[str, str]]:
        if not self.loaded:
            return []
        if level not in {"major", "middle", "minor", "detail"}:
            return []
        if not scores:
            return []

        if level == "major":
            score_map = scores.get("major_scores") or {}
            candidates = []
            for code, score in score_map.items():
                name = self.taxonomy.major_names.get(code, "")
                if not name:
                    continue
                candidates.append(
                    {
                        "major_code": code,
                        "major_name": name,
                        "middle_code": "",
                        "middle_name": "",
                        "minor_code": "",
                        "minor_name": "",
                        "_score": score,
                    }
                )
        elif level == "middle":
            score_map = scores.get("middle_scores") or {}
            candidates = []
            for code, score in score_map.items():
                if major_code and self.taxonomy.middle_to_major.get(code) != major_code:
                    continue
                name = self.taxonomy.middle_names.get(code, "")
                maj_code = self.taxonomy.middle_to_major.get(code, "") or major_code
                maj_name = self.taxonomy.major_names.get(maj_code, "")
                if not (name and maj_code and maj_name):
                    continue
                candidates.append(
                    {
                        "major_code": maj_code,
                        "major_name": maj_name,
                        "middle_code": code,
                        "middle_name": name,
                        "minor_code": "",
                        "minor_name": "",
                        "_score": score,
                    }
                )
        elif level == "minor":
            score_map = scores.get("minor_scores") or {}
            candidates = []
            for code, score in score_map.items():
                mid = self.taxonomy.minor_to_middle.get(code, "")
                if middle_code and mid != middle_code:
                    continue
                maj = self.taxonomy.middle_to_major.get(mid, "")
                if major_code and maj != major_code:
                    continue
                major_code_val, major_name, middle_code_val, middle_name, minor_code_val, minor_name = (
                    self.taxonomy.resolve_hierarchy(code)
                )
                if not (major_code_val and middle_code_val and minor_code_val):
                    continue
                candidates.append(
                    {
                        "major_code": major_code_val,
                        "major_name": major_name,
                        "middle_code": middle_code_val,
                        "middle_name": middle_name,
                        "minor_code": minor_code_val,
                        "minor_name": minor_name,
                        "_score": score,
                    }
                )
        else:
            score_map = scores.get("detail_scores") or {}
            candidates = []
            for code, score in score_map.items():
                min_code = self.taxonomy.detail_to_minor.get(code, "")
                if minor_code and min_code != minor_code:
                    continue
                if middle_code:
                    mid = self.taxonomy.detail_to_middle.get(code, "") or self.taxonomy.minor_to_middle.get(min_code, "")
                    if mid != middle_code:
                        continue
                if major_code:
                    maj = self.taxonomy.detail_to_major.get(code, "")
                    if not maj:
                        mid = self.taxonomy.detail_to_middle.get(code, "") or self.taxonomy.minor_to_middle.get(min_code, "")
                        maj = self.taxonomy.middle_to_major.get(mid, "")
                    if maj != major_code:
                        continue
                major_code_val, major_name, middle_code_val, middle_name, _minor_code_val, _minor_name, detail_code, detail_name = (
                    self.taxonomy.resolve_detail_hierarchy(code)
                )
                if not (major_code_val and middle_code_val and detail_code):
                    continue
                candidates.append(
                    {
                        "major_code": major_code_val,
                        "major_name": major_name,
                        "middle_code": middle_code_val,
                        "middle_name": middle_name,
                        "minor_code": detail_code,
                        "minor_name": detail_name,
                        "_score": score,
                    }
                )

        candidates.sort(key=lambda x: (-(int(x.get("_score") or 0)), x.get("minor_code", ""), x.get("middle_code", ""), x.get("major_code", "")))
        out = candidates[: max(1, top_n)]
        for c in out:
            c.pop("_score", None)
        return out

    def rule_classify(self, text_blocks: list[str], min_score: int = 2) -> Optional[dict[str, Any]]:
        if not self.loaded:
            return None

        scores_bundle = self.score_levels(text_blocks)
        scores = scores_bundle.get("minor_scores") or {}
        if not scores:
            return None

        best_code, best_score = max(scores.items(), key=lambda x: x[1])
        if int(best_score) < int(min_score):
            return None

        major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(best_code)
        if not (major_code and middle_code and minor_code):
            return None

        alias_codes = {
            str(m.get("target_minor_code") or "")
            for m in (scores_bundle.get("alias_matches") or [])
            if str(m.get("target_minor_code") or "")
        }
        alias_based = best_code in alias_codes
        confidence = min(1.0, 0.45 + 0.08 * int(best_score))
        if alias_based:
            confidence = min(confidence, 0.5)

        return {
            "major_code": major_code,
            "major_name": major_name,
            "middle_code": middle_code,
            "middle_name": middle_name,
            "minor_code": minor_code,
            "minor_name": minor_name,
            "confidence": confidence,
            "source": "rule_alias" if alias_based else "rule",
            "review_required": bool(alias_based and (scores_bundle.get("alias_requires_review") or confidence <= 0.5)),
        }

    def build_ai_candidates(self, text_blocks: list[str], top_n: int = 12) -> list[dict[str, str]]:
        if not self.loaded:
            return []
        text = "\n".join([t for t in text_blocks if t])
        use_detail = bool(self.taxonomy.detail_names)
        scores = self.taxonomy.score_details(text) if use_detail else self.taxonomy.score_minors(text)
        if not scores:
            return []
        sorted_codes = sorted(scores.items(), key=lambda x: (-x[1], x[0]))[: max(1, top_n)]
        out: list[dict[str, str]] = []
        for code, _ in sorted_codes:
            if use_detail:
                major_code, major_name, middle_code, middle_name, _minor_code, _minor_name, detail_code, detail_name = (
                    self.taxonomy.resolve_detail_hierarchy(code)
                )
                if not (major_code and middle_code and detail_code):
                    continue
                out.append(
                    {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": detail_code,
                        "minor_name": detail_name,
                    }
                )
            else:
                major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(code)
                if not (major_code and middle_code and minor_code):
                    continue
                out.append(
                    {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": minor_code,
                        "minor_name": minor_name,
                    }
                )
        return out

    def build_candidates_from_industry_name(self, industry_text: str, top_n: int = 12) -> list[dict[str, str]]:
        if not self.loaded:
            return []
        norm_spaced = norm_text(industry_text)
        norm = norm_spaced.replace(" ", "")
        if not norm:
            return []

        tokens = [t for t in norm_spaced.split(" ") if len(t) >= 2]

        use_detail = bool(self.taxonomy.detail_names)
        source = self.taxonomy.detail_names if use_detail else self.taxonomy.minor_names
        scores: list[tuple[int, str]] = []
        for code, name in source.items():
            name_norm = self.taxonomy._normalize(name)
            score = 0
            if not name_norm:
                continue
            if norm in name_norm or name_norm in norm:
                score += 5
            for tok in tokens:
                if tok and tok in name_norm:
                    score += 1
            if score > 0:
                scores.append((score, code))
        if not scores:
            return []

        scores.sort(key=lambda x: (-x[0], x[1]))
        out: list[dict[str, str]] = []
        for _, code in scores[: max(1, top_n)]:
            if use_detail:
                major_code, major_name, middle_code, middle_name, _minor_code, _minor_name, detail_code, detail_name = (
                    self.taxonomy.resolve_detail_hierarchy(code)
                )
                if not (major_code and middle_code and detail_code):
                    continue
                out.append(
                    {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": detail_code,
                        "minor_name": detail_name,
                    }
                )
            else:
                major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(code)
                if not (major_code and middle_code and minor_code):
                    continue
                out.append(
                    {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": minor_code,
                        "minor_name": minor_name,
                    }
                )
        return out

    def resolve_exact_candidate_from_name(self, industry_text: str) -> Optional[dict[str, str]]:
        if not self.loaded:
            return None
        norm = self.taxonomy._normalize(industry_text)
        if not norm:
            return None

        entries = self.taxonomy.normalized_name_index.get(norm, [])
        if entries:
            entries = sorted(entries, key=lambda e: (0 if e.get("detail_code") else 1, 0 if e.get("minor_code") else 1))
            for e in entries:
                detail_code = str(e.get("detail_code") or "")
                minor_code = str(e.get("minor_code") or "")
                if detail_code and detail_code in self.taxonomy.detail_names:
                    (
                        major_code,
                        major_name,
                        middle_code,
                        middle_name,
                        _minor_code,
                        _minor_name,
                        resolved_detail_code,
                        detail_name,
                    ) = self.taxonomy.resolve_detail_hierarchy(detail_code)
                    if not (major_code and middle_code and resolved_detail_code):
                        continue
                    return {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": resolved_detail_code,
                        "minor_name": detail_name,
                    }

                if minor_code and minor_code in self.taxonomy.minor_names:
                    major_code, major_name, middle_code, middle_name, resolved_minor_code, minor_name = self.taxonomy.resolve_hierarchy(minor_code)
                    if not (major_code and middle_code and resolved_minor_code):
                        continue
                    return {
                        "major_code": major_code,
                        "major_name": major_name,
                        "middle_code": middle_code,
                        "middle_name": middle_name,
                        "minor_code": resolved_minor_code,
                        "minor_name": minor_name,
                    }

        for code, name in self.taxonomy.detail_names.items():
            if self.taxonomy._normalize(name) != norm:
                continue
            (
                major_code,
                major_name,
                middle_code,
                middle_name,
                _minor_code,
                _minor_name,
                detail_code,
                detail_name,
            ) = self.taxonomy.resolve_detail_hierarchy(code)
            if not (major_code and middle_code and detail_code):
                continue
            return {
                "major_code": major_code,
                "major_name": major_name,
                "middle_code": middle_code,
                "middle_name": middle_name,
                "minor_code": detail_code,
                "minor_name": detail_name,
            }

        for code, name in self.taxonomy.minor_names.items():
            if self.taxonomy._normalize(name) != norm:
                continue
            major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(code)
            if not (major_code and middle_code and minor_code):
                continue
            return {
                "major_code": major_code,
                "major_name": major_name,
                "middle_code": middle_code,
                "middle_name": middle_name,
                "minor_code": minor_code,
                "minor_name": minor_name,
            }
        return None

    def format_candidates_text(self, candidates: list[dict[str, str]]) -> str:
        lines = []
        for c in candidates:
            line = (
                f"{c.get('major_code','')} {c.get('major_name','')} / "
                f"{c.get('middle_code','')} {c.get('middle_name','')} / "
                f"{c.get('minor_code','')} {c.get('minor_name','')}"
            )
            lines.append(line)
        return "\n".join(lines)
