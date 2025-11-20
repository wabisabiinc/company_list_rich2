import csv
import os
import re
from typing import Dict, Iterable, Optional


CORPORATE_NUMBER_KEYS = (
    "corporate_number",
    "corporate_number_norm",
    "法人番号",
    "法人番号（名寄せ）",
)
HOMEPAGE_KEYS = ("homepage", "ホームページ", "HP", "Webサイト", "website")
PHONE_KEYS = ("phone", "電話番号", "TEL", "Phone")
ADDRESS_KEYS = (
    "found_address",
    "address",
    "住所",
    "所在地",
    "都道府県／地域",
    "都道府県",
)


def _clean(val: Optional[str]) -> str:
    return (val or "").strip()


def _normalize_phone(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = re.sub(r"[‐―－ー−]+", "-", value)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", normalized)
    if not m:
        return normalized.strip()
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def _normalize_address(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.strip().replace("　", " ")
    text = re.sub(r"[‐―－ー−]+", "-", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^〒\s*", "〒", text)
    return text


def _normalize_url(value: Optional[str]) -> str:
    if not value:
        return ""
    url = value.strip()
    if not url:
        return ""
    url = url.replace(" ", "")
    if url.endswith("/"):
        url = url.rstrip("/")
    return url


class ReferenceChecker:
    """
    HubSpotなどの既存データと突き合わせて精度を記録するためのヘルパー。
    corporate_number/corporate_number_norm をキーに参照レコードを保持し、
    evaluate() で比較結果と参照値を返す。
    """

    def __init__(self, records: Dict[str, dict]):
        self._records = records

    @classmethod
    def from_csvs(cls, csv_paths: Iterable[str]) -> "ReferenceChecker":
        records: Dict[str, dict] = {}
        for csv_path in csv_paths:
            path = csv_path.strip()
            if not path:
                continue
            if not os.path.exists(path):
                raise FileNotFoundError(f"Reference CSV not found: {path}")
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    corp = cls._extract_corporate_number(row)
                    if not corp:
                        continue
                    records[corp] = {
                        "homepage": _normalize_url(cls._first_non_empty(row, HOMEPAGE_KEYS)),
                        "phone": _normalize_phone(cls._first_non_empty(row, PHONE_KEYS)),
                        "address": _normalize_address(cls._first_non_empty(row, ADDRESS_KEYS)),
                    }
        return cls(records)

    @staticmethod
    def _first_non_empty(row: dict, keys: Iterable[str]) -> str:
        for key in keys:
            if key in row:
                candidate = _clean(row.get(key))
                if candidate:
                    return candidate
        return ""

    @staticmethod
    def _extract_corporate_number(row: dict) -> str:
        for key in CORPORATE_NUMBER_KEYS:
            if key not in row:
                continue
            cleaned = re.sub(r"\D+", "", row.get(key, "") or "")
            if cleaned:
                return cleaned
        return ""

    def __len__(self) -> int:
        return len(self._records)

    def evaluate(self, company: dict) -> dict:
        corp = company.get("corporate_number_norm") or company.get("corporate_number")
        corp = re.sub(r"\D+", "", str(corp or ""))
        if not corp:
            return {}
        reference = self._records.get(corp)
        if not reference:
            return {}

        homepage_new = _normalize_url(company.get("homepage"))
        phone_new = _normalize_phone(company.get("phone"))
        address_new = _normalize_address(company.get("found_address") or company.get("address"))

        return {
            "reference_homepage": reference.get("homepage", ""),
            "reference_phone": reference.get("phone", ""),
            "reference_address": reference.get("address", ""),
            "accuracy_homepage": self._status(homepage_new, reference.get("homepage")),
            "accuracy_phone": self._status(phone_new, reference.get("phone")),
            "accuracy_address": self._status(address_new, reference.get("address")),
        }

    @staticmethod
    def _status(new_value: str, ref_value: Optional[str]) -> str:
        ref_normalized = ref_value or ""
        if not ref_normalized:
            return "no_ref"
        if not new_value:
            return "missing_new"
        return "match" if new_value == ref_normalized else "mismatch"
