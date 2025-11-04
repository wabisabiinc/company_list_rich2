# src/ai_verifier.py 
import os
import json
import logging
import time
import re
import base64
from typing import Optional, Dict, Any

# ---- .env の読み込みと正規化 ---------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass  # 無くても環境変数が直接渡っていればOK

def _getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return (str(v).strip().lower() == "true") if v is not None else default

USE_AI: bool = _getenv_bool("USE_AI", False)
API_KEY: str = (os.getenv("GEMINI_API_KEY") or "").strip()
DEFAULT_MODEL: str = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash-lite").strip()

# ---- 依存の import を分割（巻き添え防止） ----------------------------------
try:
    import google.generativeai as generativeai
    GEN_IMPORT_ERROR = None
except Exception as e:
    generativeai = None  # type: ignore
    GEN_IMPORT_ERROR = e

try:
    from google.api_core.exceptions import GoogleAPIError  # type: ignore
except Exception:
    class GoogleAPIError(Exception):  # フォールバック
        pass

# ---- AI 有効判定（import 成功も条件に含める） -------------------------------
AI_ENABLED: bool = USE_AI and bool(API_KEY) and (generativeai is not None)

if AI_ENABLED:
    try:
        generativeai.configure(api_key=API_KEY)  # type: ignore
    except Exception as e:
        AI_ENABLED = False
        GEN_IMPORT_ERROR = e

# ---- ロギング --------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)
log.info(f"[ai_verifier] AI_ENABLED={AI_ENABLED}, USE_AI={USE_AI}, KEY_SET={bool(API_KEY)}, GEN_OK={generativeai is not None}")

# ---- ユーティリティ ---------------------------------------------------------
def _extract_first_json(text: str) -> Optional[Dict[str, Any]]:
    """モデル出力から最初の JSON オブジェクトを抽出して dict にする。"""
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
    a = addr.strip()
    a = re.sub(r"\s+", " ", a)
    m = re.search(r"(\d{3}-\d{4})\s*(.+)", a)
    if m:
        return f"{m.group(1)} {m.group(2).strip()}"
    return a

def _resp_text(resp: Any) -> str:
    """google-generativeai の応答からテキスト部を頑健に取り出す。"""
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

# ---- 本体クラス -------------------------------------------------------------
class AIVerifier:
    def __init__(self, model=None, listoss_data: Dict[str, dict] = None, db_path: str = 'data/companies.db'):
        self.db_path = db_path
        self.listoss_data = listoss_data if listoss_data is not None else {}

        if model is not None:
            self.model = model
            log.debug("AIVerifier: external model injected.")
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
        prompt = (
            "あなたは企業サイトから『問い合わせ電話番号』と『住所』を高精度に抽出する専門家です。\n"
            "添付のスクリーンショットと本文テキストの両方を根拠に、最も正確な値を1つずつ返してください。\n"
            "出力は **必ず** 次の JSON 形式のみ：\n"
            '{\n'
            '  "phone_number": "03-1234-5678" または null,\n'
            '  "address": "住所" または null\n'
            '}\n'
            "注意:\n"
            "- 複数候補があっても最良の一つを選び、迷う場合は null。\n"
            "- 代表電話でない営業直通などは除外。\n"
            "- 住所は都道府県からの表記を優先。郵便番号はあれば先頭に含める。\n"
            "\n"
            f"# 本文テキスト抜粋\n{snippet}\n"
        )
        return prompt

    async def verify_info(self, text: str, screenshot: bytes, company_name: str, address: str) -> Optional[Dict[str, Any]]:
        if not self.model:
            log.error(f"No model initialized for {company_name} ({address})")
            return None

        # 入力準備（画像は inline_data で base64 に）
        content = [self._build_prompt(text)]
        if screenshot:
            try:
                b64 = base64.b64encode(screenshot).decode("utf-8")
                content.append({"inline_data": {"mime_type": "image/png", "data": b64}})
            except Exception:
                log.warning("Failed to encode screenshot; proceeding text-only.")

        try:
            t0 = time.time()
            # --- ここを互換化：DummyModel は safety_settings を受け取らない ---
            try:
                resp = await self.model.generate_content_async(content, safety_settings=None)
            except TypeError:
                # テストの DummyModel 互換
                resp = await self.model.generate_content_async(content)
            # -------------------------------------------------------------------
            dt = time.time() - t0
            log.info(f"Gemini API call ok: {company_name} ({address}) in {dt:.2f}s")

            raw = _resp_text(resp)
            result = _extract_first_json(raw)
            if not isinstance(result, dict):
                log.warning(f"JSON parse failed: {company_name} ({address}) / raw[:200]={raw[:200]!r}")
                return None

            # 正規化
            phone = _normalize_phone(result.get("phone_number"))
            addr = _normalize_address(result.get("address"))

            out: Dict[str, Any] = {
                "phone_number": phone,
                "address": addr,
            }
            if "homepage_url" in result:
                out["homepage_url"] = result.get("homepage_url")

            return out

        except GoogleAPIError as e:
            log.error(f"Google API error for {company_name} ({address}): {e}")
            return None
        except Exception as e:
            log.error(f"Failed to verify info for {company_name} ({address}): {e}", exc_info=True)
            return None
