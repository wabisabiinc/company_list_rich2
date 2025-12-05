import os
import json
import logging
import time
import re
import unicodedata
import base64
import hashlib
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
AI_CONTEXT_PATH: str = os.getenv("AI_CONTEXT_PATH", "docs/ai_context.md")

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

def _shorten_text(text: str, max_len: int = 3500) -> str:
    if not text:
        return ""
    t = text.strip()
    if len(t) <= max_len:
        return t
    head = t[: max_len // 2]
    tail = t[- max_len // 2 :]
    return head + "\n...\n" + tail

# ---- main -------------------------------------------------------
class AIVerifier:
    def __init__(self, model=None, listoss_data: Dict[str, dict] = None, db_path: str = 'data/companies.db'):
        self.db_path = db_path
        self.listoss_data = listoss_data if listoss_data is not None else {}
        self.system_prompt = self._load_system_prompt()

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

    def _load_system_prompt(self) -> Optional[str]:
        """
        docs/ai_context.md をsystemプロンプトとして読み込む。
        読み込みに失敗した場合は None を返す。
        """
        path = AI_CONTEXT_PATH
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]
            log.info(f"AIVerifier: loaded system prompt from %s (sha256[:8]=%s)", path, digest)
            return content
        except Exception as e:
            log.warning(f"AIVerifier: failed to load system prompt from %s: %s", path, e)
            return None

    def _build_prompt(self, text: str, company_name: str = "", address: str = "") -> str:
        snippet = _shorten_text(text or "", max_len=3500)
        return (
            "あなたは企業サイトや関連資料から企業情報を抽出する専門家です。別会社を混ぜず、わからない項目は必ずnullにしてください。推測で埋めないこと。\n"
            "返すべきJSONのみを出力してください（説明やマークダウン禁止）。\n"
            f"対象企業: {company_name or '不明'} / 住所: {address or '不明'}\n"
            "{\n"
            '  "phone_number": "03-1234-5678" または null,\n'
            '  "address": "〒123-4567 東京都..." または null,\n'
            '  "rep_name": "代表取締役 山田太郎" または null,\n'
            '  "description": "60-100文字で事業内容が分かる文章（お問い合わせ/採用/アクセス/予約等は禁止）" または null\n'
            "}\n"
            "禁止: 推測、問い合わせ/採用/アクセス情報をdescriptionに入れること、汎用語(氏名/名前/役職/担当/選任/概要など)をrep_nameにすること。\n"
            "優先度: 電話/住所/代表者/description を最優先。住所は都道府県から、郵便番号があれば先頭に含める。\n"
            f"# 本文テキスト抜粋\n{snippet}\n"
        )

    async def verify_info(self, text: str, screenshot: bytes, company_name: str, address: str) -> Optional[Dict[str, Any]]:
        if not self.model:
            log.error(f"No model initialized for {company_name} ({address})")
            return None

        def _build_content(use_image: bool) -> list[Any]:
            payload: list[Any] = []
            if self.system_prompt:
                payload.append(self.system_prompt)
            payload.append(self._build_prompt(text, company_name, address))
            if use_image and screenshot:
                try:
                    b64 = base64.b64encode(screenshot).decode("utf-8")
                    payload.append({"inline_data": {"mime_type": "image/png", "data": b64}})
                except Exception:
                    log.warning("Failed to encode screenshot; proceeding text-only.")
            return payload

        content: list[Any] = _build_content(True)

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
                if description:
                    banned_terms = ("お問い合わせ", "お問合せ", "採用", "求人", "アクセス", "予約")
                    if any(term in description for term in banned_terms):
                        description = None
                    elif len(description) > 120:
                        description = description[:120].rstrip()
            else:
                description = None

            out: Dict[str, Any] = {
                "phone_number": phone,
                "address": addr,
                "rep_name": rep_name,
                "description": description,
            }
            return out

        except GoogleAPIError as e:
            log.warning(f"Google API error for {company_name} ({address}) with image: {e}")
            try:
                resp = await self.model.generate_content_async(_build_content(False), safety_settings=None)  # type: ignore
            except Exception:
                return None
            raw = _resp_text(resp)
            return _extract_first_json(raw) or None
        except Exception as e:
            log.warning(f"Failed to verify info for {company_name} ({address}) with image: {e}", exc_info=True)
            try:
                resp = await self.model.generate_content_async(_build_content(False))  # type: ignore
            except Exception:
                return None
            raw = _resp_text(resp)
            return _extract_first_json(raw) or None

    async def judge_official_homepage(
        self,
        text: str,
        screenshot: bytes | None,
        company_name: str,
        address: str,
        url: str,
    ) -> Optional[Dict[str, Any]]:
        if not self.model:
            return None
        snippet = (text or "").strip()
        if len(snippet) > 4000:
            snippet = snippet[:4000]
        prompt = (
            "あなたは企業サイトの審査官です。候補URLが公式ホームページかどうかを判定してください。\n"
            "以下を根拠に、true/false と理由、信頼度(0-1)をJSONのみで出力してください。\n"
            "出力フォーマット:\n"
            "{\\\"is_official\\\": true/false, \\\"confidence\\\": 0.0-1.0, \\\"reason\\\": \"簡潔な根拠\"}\n"
            "判断基準:\n"
            "- 企業名・所在地・サービス内容の一致を確認。\n"
            "- 口コミ/求人/まとめサイトは false。\n"
            "- URLが企業ドメインや自治体/学校の公式ドメインなら true に寄せる。\n"
            f"企業名: {company_name}\n住所: {address}\n候補URL: {url}\n本文抜粋:\n{snippet}\n"
        )
        def _build_content(use_image: bool) -> list[Any]:
            payload: list[Any] = []
            if self.system_prompt:
                payload.append(self.system_prompt)
            payload.append(prompt)
            if use_image and screenshot:
                try:
                    b64 = base64.b64encode(screenshot).decode("utf-8")
                    payload.append({"inline_data": {"mime_type": "image/png", "data": b64}})
                except Exception:
                    pass
            return payload

        content: list[Any] = _build_content(True)
        try:
            try:
                resp = await self.model.generate_content_async(content, safety_settings=None)
            except TypeError:
                resp = await self.model.generate_content_async(content)
            raw = _resp_text(resp)
            result = _extract_first_json(raw)
            if not isinstance(result, dict):
                return None
            verdict = result.get("is_official")
            if isinstance(verdict, str):
                verdict = verdict.strip().lower()
                verdict = verdict in {"true", "yes", "official", "1"}
            elif isinstance(verdict, (int, float)):
                verdict = bool(verdict)
            elif not isinstance(verdict, bool):
                return None
            confidence_val = result.get("confidence")
            try:
                confidence = float(confidence_val) if confidence_val is not None else None
            except Exception:
                confidence = None
            reason = result.get("reason")
            if isinstance(reason, str):
                reason = reason.strip()
            else:
                reason = ""
            return {
                "is_official": bool(verdict),
                "confidence": confidence,
                "reason": reason,
            }
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(f"judge_official_homepage failed for {company_name} ({url}) with image: {exc}")
            try:
                resp = await self.model.generate_content_async(_build_content(False), safety_settings=None)  # type: ignore
            except Exception:
                return None
            raw = _resp_text(resp)
            result = _extract_first_json(raw)
            if not isinstance(result, dict):
                return None
            verdict = result.get("is_official")
            if isinstance(verdict, str):
                verdict = verdict.strip().lower()
                verdict = verdict in {"true", "yes", "official", "1"}
            elif isinstance(verdict, (int, float)):
                verdict = bool(verdict)
            elif not isinstance(verdict, bool):
                return None
            confidence_val = result.get("confidence")
            try:
                confidence = float(confidence_val) if confidence_val is not None else None
            except Exception:
                confidence = None
            reason = result.get("reason")
            if isinstance(reason, str):
                reason = reason.strip()
            else:
                reason = ""
            return {
                "is_official": bool(verdict),
                "confidence": confidence,
                "reason": reason,
            }
