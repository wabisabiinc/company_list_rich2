import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from src.text_normalizer import norm_text, norm_text_compact

log = logging.getLogger(__name__)

DEFAULT_JSIC_CSV_PATH = os.getenv("JSIC_CSV_PATH", "docs/industry_select.csv")
DEFAULT_JSIC_JSON_PATH = os.getenv("JSIC_JSON_PATH", "docs/industry_select.json")
DEFAULT_INDUSTRY_ALIASES_CSV_PATH = os.getenv("INDUSTRY_ALIASES_CSV_PATH", "industry_aliases.csv")

_GENERIC_TOKENS = {
    "事業", "業", "サービス", "製品", "商品", "販売", "製造", "提供", "運営", "管理",
    "開発", "加工", "設計", "施工", "保守", "メンテナンス", "関連", "その他", "附随",
}

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

    def _build_token_index(self) -> None:
        index: dict[str, set[str]] = {}
        for code, name in self.minor_names.items():
            tokens = self._tokenize_name(name)
            for t in tokens:
                index.setdefault(t, set()).add(code)
        self._token_index = index

    def _tokenize_name(self, name: str) -> set[str]:
        spaced = norm_text(name)
        if not spaced:
            return set()
        tokens = set()
        for p in spaced.split(" "):
            p = p.strip()
            if len(p) < 2:
                continue
            if p in _GENERIC_TOKENS:
                continue
            tokens.add(p)
        return tokens

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

        text_tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンa-z0-9]{2,}", norm):
            tok = m.group(0)
            if tok in _GENERIC_TOKENS:
                continue
            text_tokens.add(tok)

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
        text_tokens = set()
        for m in re.finditer(r"[一-龥ぁ-んァ-ンa-z0-9]{2,}", norm):
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
    def __init__(self, csv_path: str | None = None, aliases_csv_path: str | None = None) -> None:
        self.csv_path = csv_path or DEFAULT_JSIC_CSV_PATH
        self.taxonomy = JSICTaxonomy(self.csv_path, DEFAULT_JSIC_JSON_PATH)
        self.loaded = self.taxonomy.load()

        self.aliases_csv_path = aliases_csv_path or DEFAULT_INDUSTRY_ALIASES_CSV_PATH
        self.alias_entries: list[IndustryAliasEntry] = []
        self.alias_source = "fallback"
        if self.loaded:
            self._load_alias_entries()

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

    def _alias_hits_for_text(self, text: str) -> list[IndustryAliasEntry]:
        if not text or not self.alias_entries:
            return []
        spaced = norm_text(text)
        compact = spaced.replace(" ", "")
        if not compact:
            return []

        hits: list[IndustryAliasEntry] = []
        seen: set[tuple[str, str]] = set()
        for entry in self.alias_entries:
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
        tag_values = self._iter_tag_values(business_tags)
        tag_hits: list[IndustryAliasEntry] = []
        for tag in tag_values:
            tag_hits.extend(self._alias_hits_for_text(tag))
        context_text = "\n".join([description or ""] + tag_values).strip()
        desc_hits = self._filter_alias_hits_by_major_context(desc_hits, context_text)
        tag_hits = self._filter_alias_hits_by_major_context(tag_hits, context_text)

        scores: dict[str, dict[str, Any]] = {}

        def apply_hit(entry: IndustryAliasEntry, source: str) -> None:
            if not entry.target_minor_code:
                return
            slot = scores.setdefault(
                entry.target_minor_code,
                {
                    "score": 0,
                    "desc_hits": 0,
                    "tag_hits": 0,
                    "max_priority": 0,
                    "requires_review": False,
                    "aliases": set(),
                    "domain_tags": set(),
                },
            )
            boost = max(1, int(entry.priority))
            if source == "tag":
                boost += 1
            if entry.requires_review:
                # Review-required aliases are intentionally weak to reduce false positives.
                boost = min(boost, 2)
            if source == "tag":
                slot["tag_hits"] = int(slot.get("tag_hits") or 0) + 1
            else:
                slot["desc_hits"] = int(slot.get("desc_hits") or 0) + 1
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
        both_sources = desc_count > 0 and tag_count > 0

        base_conf = 0.45 + 0.04 * min(5, int(best_meta.get("score") or 0))
        if both_sources:
            confidence = min(0.72, max(0.55, base_conf))
        else:
            confidence = min(0.5, base_conf)

        review_required = bool(best_meta.get("requires_review") or confidence <= 0.5)

        return {
            **candidate,
            "confidence": float(max(0.0, min(1.0, confidence))),
            "source": "alias_desc_tags" if both_sources else "alias_single",
            "review_required": review_required,
            "alias_only": True,
            "alias_match_count": int(desc_count + tag_count),
            "alias_desc_hits": desc_count,
            "alias_tag_hits": tag_count,
            "alias_matches": sorted(list(best_meta.get("aliases") or [])),
            "alias_domain_tags": sorted(list(best_meta.get("domain_tags") or [])),
        }

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
        alias_hits = self._filter_alias_hits_by_major_context(alias_hits, text)
        for hit in alias_hits:
            if not hit.target_minor_code:
                continue
            boost = max(1, int(hit.priority))
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
