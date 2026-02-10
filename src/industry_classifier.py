import csv
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Optional

log = logging.getLogger(__name__)

DEFAULT_JSIC_CSV_PATH = os.getenv("JSIC_CSV_PATH", "docs/industry_select.csv")
DEFAULT_JSIC_JSON_PATH = os.getenv("JSIC_JSON_PATH", "docs/industry_select.json")

_GENERIC_TOKENS = {
    "事業", "業", "サービス", "製品", "商品", "販売", "製造", "提供", "運営", "管理",
    "開発", "加工", "設計", "施工", "保守", "メンテナンス", "関連", "その他", "附随",
}

# 短い略称・カタカナ英語をJSIC小分類コードに補助マッピング
# ※最終決定はAI側が行うため、ここでは候補スコアを加点するのみ
_ALIAS_TO_MINOR = {
    "IT": ["392"], "ＩＴ": ["392"], "システム": ["392"], "ソフトウェア": ["392"], "SAAS": ["392"], "ＳＡＡＳ": ["392"], "DX": ["392"], "ＤＸ": ["392"], "クラウド": ["392"],
    "WEB": ["401"], "ＷＥＢ": ["401"], "EC": ["401"], "ＥＣ": ["401"], "ＥＣサイト": ["401"],
    "マーケ": ["731"], "マーケティング": ["731"], "広告": ["731"], "PR": ["731"], "ＰＲ": ["731"],
    "コンサル": ["728"], "コンサルティング": ["728"],
    "物流": ["441"], "運送": ["441"], "配送": ["441"], "倉庫": ["470"],
    "派遣": ["912"], "人材": ["912"], "紹介": ["911"],
    "介護": ["854"], "福祉": ["854"],
    "医療": ["831"], "病院": ["831"],
    "飲食": ["761"], "レストラン": ["761"], "カフェ": ["761"],
    "不動産": ["681"], "賃貸": ["681"], "仲介": ["681"],
    "建設": ["062"], "工事": ["062"], "施工": ["062"], "設備": ["063"], "電気工事": ["064"],
    "小売": ["602"],
    "保険": ["670"],
}


@dataclass
class IndustryEntry:
    level: str
    code: str
    name: str
    major_code: str
    middle_code: str
    minor_code: str


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
        self._token_index: dict[str, set[str]] = {}

    def load(self) -> bool:
        prefer_json = os.getenv("JSIC_PREFER_JSON", "false").lower() == "true"

        def _load_from_csv() -> bool:
            if not self.csv_path or not os.path.exists(self.csv_path):
                return False
            rows = self._read_rows(self.csv_path)
            if not rows:
                log.warning("JSIC taxonomy CSV empty: %s", self.csv_path)
                return False

            # Prefer structured JSIC CSV with explicit major/middle/minor/detail columns.
            if self._looks_like_industry_select(rows):
                self._load_from_industry_select(rows)
                self._build_token_index()
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
                else:
                    # detail or unknown; skip storing as selectable
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

            self._build_token_index()
            return True

        def _load_from_json_wrapper() -> bool:
            if self.json_path and os.path.exists(self.json_path):
                if self._load_from_json(self.json_path):
                    self._build_token_index()
                    return True
            return False

        # デフォルトではCSV優先。JSIC_PREFER_JSON=true のときのみJSONを優先する。
        if prefer_json:
            if _load_from_json_wrapper():
                return True
            if _load_from_csv():
                return True
        else:
            if _load_from_csv():
                return True
            if _load_from_json_wrapper():
                return True

        log.warning("JSIC taxonomy not loaded. CSV=%s JSON=%s", self.csv_path, self.json_path)
        return False

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
            # 大分類代表: 中分類=00 かつ 小分類=000（細分類は 0000 を想定）
            if middle == "00" and minor == "000" and detail == "0000":
                level = "major"
                code = major
                self.major_names[major] = name
            # 中分類代表: 小分類=000 かつ 細分類=0000
            elif minor == "000" and detail == "0000":
                level = "middle"
                code = middle
                self.middle_names[middle] = name
                if major:
                    self.middle_to_major[middle] = major
            # 小分類代表: 細分類=0000 かつ 小分類!=000
            elif detail == "0000" and minor != "000":
                level = "minor"
                code = minor
                self.minor_names[minor] = name
                self._minor_names_norm[minor] = self._normalize(name)
                if middle:
                    self.minor_to_middle[minor] = middle
                if major and middle:
                    self.middle_to_major[middle] = major
            # 小分類: 細分類コードが0000以外（= 詳細行を小分類として扱う）
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
                # detail rows are not selectable in our classification
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

    def _read_rows(self, path: str) -> list[dict[str, str]]:
        for enc in ("utf-8-sig", "cp932", "shift_jis"):
            try:
                with open(path, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    return [dict(row) for row in reader]
            except Exception:
                continue
        # fallback: raw CSV rows without header
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

        # Support "industry_select.csv" style headers:
        # 大分類コード, 中分類コード, 小分類コード, 細分類コード, 項目名
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

        # try to parse from a single field: "A 農業、林業" or "0121 露地野菜作"
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

    def _build_token_index(self) -> None:
        index: dict[str, set[str]] = {}
        for code, name in self.minor_names.items():
            tokens = self._tokenize_name(name)
            for t in tokens:
                index.setdefault(t, set()).add(code)
        self._token_index = index

    @staticmethod
    def _normalize(text: str) -> str:
        s = unicodedata.normalize("NFKC", text or "")
        s = re.sub(r"\s+", "", s)
        return s

    def _tokenize_name(self, name: str) -> set[str]:
        s = self._normalize(name)
        if not s:
            return set()
        parts = re.split(r"[・、/()（）・\-〜～~]", s)
        tokens = set()
        for p in parts:
            p = p.strip()
            if len(p) < 2:
                continue
            if p in _GENERIC_TOKENS:
                continue
            tokens.add(p)
        return tokens

    def score_minors(self, text: str) -> dict[str, int]:
        if not text:
            return {}
        norm = self._normalize(text)
        if not norm:
            return {}
        # extract candidate tokens from text
        text_tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンA-Za-z0-9]{2,}", norm):
            tok = m.group(0)
            if tok in _GENERIC_TOKENS:
                continue
            text_tokens.add(tok)

        scores: dict[str, int] = {}
        for tok in text_tokens:
            for code in self._token_index.get(tok, set()):
                scores[code] = scores.get(code, 0) + 1
            # substring match against minor names (helps short industry hints)
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
        text_tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンA-Za-z0-9]{2,}", norm):
            tok = m.group(0)
            if tok in _GENERIC_TOKENS:
                continue
            text_tokens.add(tok)
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
    def __init__(self, csv_path: str | None = None) -> None:
        self.csv_path = csv_path or DEFAULT_JSIC_CSV_PATH
        self.taxonomy = JSICTaxonomy(self.csv_path, DEFAULT_JSIC_JSON_PATH)
        self.loaded = self.taxonomy.load()

    def score_levels(self, text_blocks: list[str]) -> dict[str, Any]:
        if not self.loaded:
            return {
                "use_detail": False,
                "detail_scores": {},
                "minor_scores": {},
                "middle_scores": {},
                "major_scores": {},
            }
        text = "\n".join([t for t in text_blocks if t])
        if not text:
            return {
                "use_detail": False,
                "detail_scores": {},
                "minor_scores": {},
                "middle_scores": {},
                "major_scores": {},
            }

        use_detail = bool(self.taxonomy.detail_names)
        detail_scores: dict[str, int] = {}
        if use_detail:
            detail_scores = self.taxonomy.score_details(text)
        minor_scores: dict[str, int]
        if use_detail:
            # 通常は細分類スコアを小分類に集計する。
            # ただし細分類に全くヒットしない場合は、小分類スコアを直接計算して落ちないようにする。
            minor_scores = {}
            for detail_code, score in detail_scores.items():
                minor_code = self.taxonomy.detail_to_minor.get(detail_code, "")
                if not minor_code:
                    continue
                minor_scores[minor_code] = minor_scores.get(minor_code, 0) + score
            if not minor_scores:
                minor_scores = self.taxonomy.score_minors(text)
        else:
            minor_scores = self.taxonomy.score_minors(text)

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

        norm_text = self.taxonomy._normalize(text)
        tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンA-Za-z0-9]{2,}", norm_text):
            tok = m.group(0)
            if tok in _GENERIC_TOKENS:
                continue
            tokens.add(tok)

        def boost_scores(scores: dict[str, int], name_map: dict[str, str], base_boost: int = 3) -> None:
            if not norm_text:
                return
            for code, name in name_map.items():
                name_norm = self.taxonomy._normalize(name)
                if not name_norm:
                    continue
                boost = 0
                if name_norm in norm_text or norm_text in name_norm:
                    boost += base_boost
                else:
                    for tok in tokens:
                        if tok in name_norm:
                            boost += 1
                if boost > 0:
                    scores[code] = scores.get(code, 0) + boost

        boost_scores(major_scores, self.taxonomy.major_names)
        boost_scores(middle_scores, self.taxonomy.middle_names)
        boost_scores(minor_scores, self.taxonomy.minor_names)
        if use_detail:
            boost_scores(detail_scores, self.taxonomy.detail_names)

        # エイリアス（略称・英語）の加点。存在しないコードは無視。
        norm_text_upper = norm_text.upper()
        for alias, codes in _ALIAS_TO_MINOR.items():
            if alias and alias in norm_text_upper:
                for code in codes:
                    if code in self.taxonomy.detail_names:
                        detail_scores[code] = detail_scores.get(code, 0) + 1
                    if code in self.taxonomy.minor_names:
                        minor_scores[code] = minor_scores.get(code, 0) + 1

        return {
            "use_detail": use_detail,
            "detail_scores": detail_scores,
            "minor_scores": minor_scores,
            "middle_scores": middle_scores,
            "major_scores": major_scores,
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
                major_code_val, major_name, middle_code_val, middle_name, minor_code_val, minor_name, detail_code, detail_name = (
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
        text = "\n".join([t for t in text_blocks if t])
        scores = self.taxonomy.score_minors(text)
        if not scores:
            return None
        best_code, best_score = max(scores.items(), key=lambda x: x[1])
        if best_score < min_score:
            return None
        major_code, major_name, middle_code, middle_name, minor_code, minor_name = self.taxonomy.resolve_hierarchy(best_code)
        if not (major_code and middle_code and minor_code):
            return None
        return {
            "major_code": major_code,
            "major_name": major_name,
            "middle_code": middle_code,
            "middle_name": middle_name,
            "minor_code": minor_code,
            "minor_name": minor_name,
            "confidence": min(1.0, 0.5 + 0.1 * best_score),
            "source": "rule",
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
                major_code, major_name, middle_code, middle_name, minor_code, minor_name, detail_code, detail_name = (
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
        norm = self.taxonomy._normalize(industry_text)
        if not norm:
            return []
        seps = "・,、/()（）-〜～~"
        tokens: list[str] = []
        buf = ""
        for ch in norm:
            if ch in seps:
                if buf:
                    tokens.append(buf)
                    buf = ""
            else:
                buf += ch
        if buf:
            tokens.append(buf)
        tokens = [t for t in tokens if len(t) >= 2]

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
                if tok in name_norm:
                    score += 1
            if score > 0:
                scores.append((score, code))
        if not scores:
            return []

        scores.sort(key=lambda x: (-x[0], x[1]))
        out: list[dict[str, str]] = []
        for _, code in scores[: max(1, top_n)]:
            if use_detail:
                major_code, major_name, middle_code, middle_name, minor_code, minor_name, detail_code, detail_name = (
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

        # detail -> minor hierarchy
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
