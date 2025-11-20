import csv
import re
from typing import Dict, Tuple, List, Optional

HUBSPOT_HOMEPAGE_KEYS = ("ホームページ", "HP", "Webサイト", "website")
HUBSPOT_ADDRESS_KEYS = ("都道府県／地域", "都道府県", "住所")


def _clean(val: Optional[str]) -> str:
    return (val or "").strip()


def _normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"[‐―－ー−]+", "-", s)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def _normalize_address(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip().replace("　", " ")
    s = re.sub(r"[‐―－ー−]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    return s or None


def _normalize_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    return u.rstrip("/")


def load_hubspot_data(csv_paths: List[str]) -> Dict[Tuple[str, str], dict]:
    """
    HubSpotの会社エクスポートCSVから会社名+住所（都道府県レベル）をキーにした辞書を返す。
    住所は都道府県の列が優先され、電話・HPは存在すれば格納される。
    """
    hubspot_data: Dict[Tuple[str, str], dict] = {}

    for csv_path in csv_paths:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                company_name = _clean(row.get("会社名"))
                if not company_name:
                    continue

                address = ""
                for key in HUBSPOT_ADDRESS_KEYS:
                    address = _clean(row.get(key))
                    if address:
                        break

                phone = _clean(row.get("電話番号"))
                homepage = ""
                for key in HUBSPOT_HOMEPAGE_KEYS:
                    homepage = _clean(row.get(key))
                    if homepage:
                        break

                normalized_addr = _normalize_address(address) or address
                key = (company_name, normalized_addr or "")
                hubspot_data[key] = {
                    "addr": normalized_addr or "",
                    "phone": _normalize_phone(phone) if phone else None,
                    "hp": _normalize_url(homepage) if homepage else None,
                }

    return hubspot_data
