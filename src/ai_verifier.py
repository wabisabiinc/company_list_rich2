import os
import json
import logging
import time
import re
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
            "あなたは企業サイトから問い合わせ先の電話番号・住所・代表者名・会社説明を抽出する専門家です。\n"
            "スクリーンショット（任意）と本文テキストを根拠に、もっとも信頼できる値を各1件ずつ返してください。\n"
            "出力は **必ず次のJSONのみ**：\n"
            "{\n"
            '  "phone_number": "03-1234-5678" または null,\n'
            '  "address": "〒123-4567 東京都..." または null,\n'
            '  "rep_name": "代表取締役 田中 太郎" または null,\n'
            '  "description": "会社概要（50字以内、業種を含める）" または null\n'
            "}\n"
            "注意:\n"
            "- 代表電話でない営業直通/採用窓口等は除外。\n"
            "- 住所は都道府県からの表記を優先し、郵便番号があれば先頭に含めてください（例: 〒123-4567 ...）。\n"
            "- 代表者名は肩書+氏名が望ましい。\n"
            "- 説明は冗長な装飾を避け、50字以内に要約してください。\n"
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
            return out

        except GoogleAPIError as e:
            log.error(f"Google API error for {company_name} ({address}): {e}")
            return None
        except Exception as e:
            log.error(f"Failed to verify info for {company_name} ({address}): {e}", exc_info=True)
            return None
