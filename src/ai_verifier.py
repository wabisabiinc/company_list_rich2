import os
import json
import logging
import time
import re
import unicodedata
import base64
from typing import Optional, Dict, Any

# ---- .env -------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return (str(v).strip().lower() == "true") if v is not None else default

USE_AI: bool = _getenv_bool("USE_AI", False)
API_KEY: str = (os.getenv("GEMINI_API_KEY") or "").strip()
DEFAULT_MODEL: str = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash-lite").strip()

# ---- deps -------------------------------------------------------
try:
    import google.generativeai as generativeai
    GEN_IMPORT_ERROR = None
except Exception as e:
    generativeai = None  # type: ignore
    GEN_IMPORT_ERROR = e

try:
    from google.api_core.exceptions import GoogleAPIError  # type: ignore
except Exception:
    class GoogleAPIError(Exception):  # fallback
        pass

AI_ENABLED: bool = USE_AI and bool(API_KEY) and (generativeai is not None)
if AI_ENABLED:
    try:
        generativeai.configure(api_key=API_KEY)  # type: ignore
    except Exception as e:
        AI_ENABLED = False
        GEN_IMPORT_ERROR = e

# ---- logging ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)
log.info(f"[ai_verifier] AI_ENABLED={AI_ENABLED}, USE_AI={USE_AI}, KEY_SET={bool(API_KEY)}, GEN_OK={generativeai is not None}")

# ---- utils ------------------------------------------------------
def _extract_first_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    text = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).strip()
    brace_stack = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if brace_stack == 0:
                start = i
            brace_stack += 1
        elif ch == "}":
            brace_stack -= 1
            if brace_stack == 0 and start != -1:
                candidate = text[start:i+1]
                try:
                    return json.loads(candidate)
                except Exception:
                    pass
    try:
        return json.loads(text)
    except Exception:
        return None

def _normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"[‐―－ー]+", "-", s)
    s = re.sub(r"（.*?）", "", s)
    s = re.sub(r"TEL[:：]\s*", "", s, flags=re.I)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def _normalize_address(addr: Optional[str]) -> Optional[str]:
    if not addr:
        return None
    a = re.sub(r"\s+", " ", addr.strip())
    # 郵便番号があれば 〒を付けて統一
    m = re.search(r"(\d{3}-\d{4})\s*(.+)", a)
    if m:
        body = m.group(2).strip()
        return f"〒{m.group(1)} {body}"
    return a

def _resp_text(resp: Any) -> str:
    t = getattr(resp, "text", None)
    if isinstance(t, str) and t.strip():
        return t
    try:
        for cand in getattr(resp, "candidates", []) or []:
            parts = getattr(getattr(cand, "content", None), "parts", []) or []
            for p in parts:
                pt = getattr(p, "text", None)
                if isinstance(pt, str) and pt.strip():
                    return pt
    except Exception:
        pass
    return str(resp)

def _normalize_listing(val: Any) -> Optional[str]:
    if not val:
        return None
    text = re.sub(r"\s+", " ", str(val)).strip()
    if not text:
        return None
    # 上場/未上場/非上場/非公開/未公開 などをそのまま返す（詳細な市場名も許容）
    return text[:40]

def _normalize_amount(val: Any) -> Optional[str]:
    if not val:
        return None
    text = unicodedata.normalize("NFKC", str(val))
    text = re.sub(r"\s+", "", text)
    if not text:
        return None
    # 負号（赤字）を吸収
    sign = -1 if text.startswith(("▲", "△", "-")) else 1
    text = text.lstrip("▲△-")
    # ノイズ除去
    text = re.sub(r"[(),（）]", "", text)
    text = text.replace("約", "").replace("およそ", "").replace("程度", "").replace("前後", "").replace("強", "").replace("弱", "").replace("ほど", "").replace("規模", "")
    text = text.replace("以上", "").replace("未満", "").replace("超", "")
    if not re.search(r"[0-9０-９]", text):
        return None
    # 数字を半角に
    text = unicodedata.normalize("NFKC", text)
    text = text.replace(",", "")

    amount = 0.0

    def consume(pattern: str, factor: float) -> None:
        nonlocal text, amount
        matches = list(re.finditer(pattern, text))
        for m in matches:
            try:
                amount += float(m.group(1)) * factor
            except Exception:
                continue
        text = re.sub(pattern, "", text)

    # 兆 / 億 / 万 の複合表記に対応（例: 6億6700万）
    consume(r"([0-9]+(?:\.[0-9]+)?)兆", 1e12)
    consume(r"([0-9]+(?:\.[0-9]+)?)億", 1e8)
    consume(r"([0-9]+(?:\.[0-9]+)?)万", 1e4)
    # その他の単位
    consume(r"([0-9]+(?:\.[0-9]+)?)百万円", 1e6)
    consume(r"([0-9]+(?:\.[0-9]+)?)千円", 1e3)
    consume(r"([0-9]+(?:\.[0-9]+)?)万円", 1e4)

    # 残りが純数字なら円として扱う
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if m and amount == 0:
        try:
            amount = float(m.group(1))
        except Exception:
            pass

    if amount <= 0:
        return None
    yen = int(round(amount))
    prefix = "▲" if sign < 0 else ""
    return f"{prefix}{yen:,}円"[:40]

def _normalize_fiscal_month(val: Any) -> Optional[str]:
    if not val:
        return None
    text = str(val).strip()
    m = re.search(r"(1[0-2]|0?[1-9])", text)
    if not m:
        return None
    return f"{int(m.group(1))}月"

def _normalize_year(val: Any) -> Optional[str]:
    if not val:
        return None
    m = re.search(r"(18|19|20)\d{2}", str(val))
    if not m:
        return None
    return m.group(0)

# ---- main -------------------------------------------------------
class AIVerifier:
    def __init__(self, model=None, listoss_data: Dict[str, dict] = None, db_path: str = 'data/companies.db'):
        self.db_path = db_path
        self.listoss_data = listoss_data if listoss_data is not None else {}

        if model is not None:
            self.model = model
        else:
            if AI_ENABLED:
                try:
                    self.model = generativeai.GenerativeModel(DEFAULT_MODEL)  # type: ignore
                    log.info(f"AIVerifier: Gemini model initialized ({DEFAULT_MODEL}).")
                except Exception as e:
                    log.error(f"AIVerifier: Failed to init model: {e}", exc_info=True)
                    self.model = None
            else:
                self.model = None
                if GEN_IMPORT_ERROR:
                    log.warning(f"AIVerifier: AI disabled due to import/config error: {GEN_IMPORT_ERROR}")

    def _build_prompt(self, text: str) -> str:
        snippet = (text or "").strip()
        if len(snippet) > 6000:
            snippet = snippet[:6000]
        return (
            "あなたは企業サイトや関連資料から企業情報を抽出する専門家です。\n"
            "スクリーンショット（任意）と本文テキストを根拠に、もっとも信頼できる値を各1件ずつ返してください。\n"
            "まず本文から数値/単位/表記を探してください。見つからない場合のみ推測を検討してください。\n"
            "売上/利益は本文・表の数値+単位（円/万円/億円/百万円/千円）があれば null にせず必ず埋めてください。年度/期が分かれば値の前後に短く付記して構いません。\n"
            "上場/非上場は証券コードや市場名などの根拠がある場合のみ値を返し、根拠が無ければ null（推測で非上場としない）。\n"
            "出力は **必ず次のJSONのみ**（説明文やmarkdown禁止）：\n"
            "{\n"
            '  "phone_number": "03-1234-5678" または null,\n'
            '  "address": "〒123-4567 東京都..." または null,\n'
            '  "rep_name": "代表取締役 田中 太郎" または null,\n'
            '  "description": "会社概要（50字以内、業種を含める）" または null,\n'
            '  "listing": "上場/未上場/非上場 など" または null,\n'
            '  "capital": "資本金（例: 1億円）" または null,\n'
            '  "revenue": "売上高（例: 10億円）" または null,\n'
            '  "profit": "利益（例: 2億円）" または null,\n'
            '  "fiscal_month": "決算月（例: 3月）" または null,\n'
            '  "founded_year": "設立年（例: 1987）" または null\n'
            "}\n"
            "注意:\n"
            "- 代表電話でない営業直通/採用窓口等は除外。\n"
            "- 住所は都道府県からの表記を優先し、郵便番号があれば先頭に含めてください（例: 〒123-4567 ...）。\n"
            "- 代表者名は肩書+氏名が望ましい。\n"
            "- 説明は冗長な装飾を避け、50字以内に要約してください。\n"
            "- 財務系（上場/資本金/売上/利益/決算月/設立年）は本文の値を最優先。和暦は西暦に変換（昭和53年→1978、令和元年→2019）。\n"
            "- 上場区分: 記載が無い場合は推測せず null も可。証券コード（4桁）や市場名があれば上場扱い。\n"
            "- 売上/利益: 範囲が複数あれば最新年度・最も大きい額を1つ選び、単位を含めてください（例: 72.3億円、3,500万円）。注記があれば年度/期を簡潔に残してください。\n"
            "- 数値は本文から抽出し、単位（円/万円/億円/百万円/千円）を残す。兆/億/万/百万円/千円は円換算可能な表記で返す。手がかりが全く無いときのみ null。\n"
            "- 決算月: 記載がなければ null だが、「3月期」「3月決算」などがあれば 3月 と返す。\n"
            "- 設立年: 年だけを返す。和暦や年月日があれば年を抽出（昭和/平成/令和/M/T/S/H/R + 数字/元）。\n"
            "\n"
            f"# 本文テキスト抜粋\n{snippet}\n"
        )

    async def verify_info(self, text: str, screenshot: bytes, company_name: str, address: str) -> Optional[Dict[str, Any]]:
        if not self.model:
            log.error(f"No model initialized for {company_name} ({address})")
            return None

        content = [self._build_prompt(text)]
        if screenshot:
            try:
                b64 = base64.b64encode(screenshot).decode("utf-8")
                content.append({"inline_data": {"mime_type": "image/png", "data": b64}})
            except Exception:
                log.warning("Failed to encode screenshot; proceeding text-only.")

        try:
            t0 = time.time()
            try:
                resp = await self.model.generate_content_async(content, safety_settings=None)
            except TypeError:
                resp = await self.model.generate_content_async(content)
            dt = time.time() - t0
            log.info(f"Gemini API call ok: {company_name} ({address}) in {dt:.2f}s")

            raw = _resp_text(resp)
            result = _extract_first_json(raw)
            if not isinstance(result, dict):
                log.warning(f"JSON parse failed: {company_name} ({address}) / raw[:200]={raw[:200]!r}")
                return None

            phone = _normalize_phone(result.get("phone_number"))
            addr = _normalize_address(result.get("address"))

            rep_name = result.get("rep_name", result.get("representative"))
            if isinstance(rep_name, str):
                rep_name = rep_name.strip() or None
            else:
                rep_name = None

            description = result.get("description")
            if isinstance(description, str):
                description = re.sub(r"\s+", " ", description.strip()) or None
                if description and len(description) > 50:
                    description = description[:50]
            else:
                description = None

            out: Dict[str, Any] = {
                "phone_number": phone,
                "address": addr,
                "rep_name": rep_name,
                "description": description,
            }
            if rep_name is not None:
                out["representative"] = rep_name
            if "homepage_url" in result:
                out["homepage_url"] = result.get("homepage_url")
            listing = _normalize_listing(result.get("listing"))
            capital = _normalize_amount(result.get("capital"))
            revenue = _normalize_amount(result.get("revenue"))
            profit = _normalize_amount(result.get("profit") or result.get("income"))
            fiscal_month = _normalize_fiscal_month(result.get("fiscal_month"))
            founded_year = _normalize_year(result.get("founded_year") or result.get("established_year"))
            if listing is not None:
                out["listing"] = listing
            if capital is not None:
                out["capital"] = capital
            if revenue is not None:
                out["revenue"] = revenue
            if profit is not None:
                out["profit"] = profit
            if fiscal_month is not None:
                out["fiscal_month"] = fiscal_month
            if founded_year is not None:
                out["founded_year"] = founded_year
            return out

        except GoogleAPIError as e:
            log.error(f"Google API error for {company_name} ({address}): {e}")
            return None
        except Exception as e:
            log.error(f"Failed to verify info for {company_name} ({address}): {e}", exc_info=True)
            return None
