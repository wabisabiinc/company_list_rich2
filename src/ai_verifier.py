import os
import asyncio
import json
import logging
import time
import re
import unicodedata
import base64
import hashlib
from typing import Optional, Dict, Any

from .jp_number import normalize_kanji_numbers

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
AI_DESCRIPTION_MAX_LEN = int(os.getenv("AI_DESCRIPTION_MAX_LEN", "140"))
AI_DESCRIPTION_MIN_LEN = int(os.getenv("AI_DESCRIPTION_MIN_LEN", "80"))

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
AI_CALL_TIMEOUT_SEC = float(os.getenv("AI_CALL_TIMEOUT_SEC", "20") or 0)

# ---- compiled regex (noise filters) -----------------------------
_NOISE_LITERALS = (
    "cookie",
    "privacy",
    "プライバシ",
    "利用規約",
    "サイトマップ",
    "copyright",
    "©",
    "nav",
    "menu",
    "footer",
    "javascript",
)
_NOISE_RE = re.compile(
    rf"(?:{'|'.join(re.escape(w) for w in _NOISE_LITERALS)}|function\s*\()",
    flags=re.IGNORECASE,
)

# ---- utils ------------------------------------------------------
def _looks_mojibake(text: Optional[str]) -> bool:
    if not text:
        return False
    if "\ufffd" in text:
        return True
    if re.search(r"[ぁ-んァ-ン一-龥]", text):
        return False
    latin_count = sum(1 for ch in text if "\u00c0" <= ch <= "\u00ff")
    if latin_count >= 3 and latin_count / max(len(text), 1) >= 0.15:
        return True
    return bool(re.search(r"[ÃÂãâæçïðñöøûüÿ]", text) and latin_count >= 2)

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
    if _looks_mojibake(addr):
        return None
    a = re.sub(r"\s+", " ", addr.strip())
    # 住所の後ろに混入しがちな付帯情報をカット（AI出力の誤混入対策）
    cut_re = re.compile(
        r"\s*(?:"
        r"TEL|電話|☎|℡|FAX|ファックス|メール|E[-\s]?mail|"
        r"地図|マップ|Google\s*マップ|アクセス|行き方|ルート|経路|"
        r"営業時間|受付時間|定休日|"
        r"従業員(?:数)?|社員(?:数)?|職員(?:数)?|スタッフ(?:数)?|人数|"
        r"資本金|設立|創業|沿革|代表者|代表取締役|"
        r"(?:市区町村|自治体)コ[-ー]ド"
        r")\b",
        re.IGNORECASE,
    )
    m = cut_re.search(a)
    if m:
        a = a[: m.start()].strip()
    # （市区町村コード:12208）等の括弧付帯情報を削除
    a = re.sub(r"[（(]\s*(?:市区町村|自治体)コ[-ー]ド\s*[:：]\s*\d+\s*[)）]", "", a)
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
    text = normalize_kanji_numbers(unicodedata.normalize("NFKC", str(val)))
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

def _text_contexts(text: str, pattern: str, window: int = 28) -> list[str]:
    """
    Return nearby contexts for regex matches in normalized text (NFKC).
    """
    if not text:
        return []
    t = unicodedata.normalize("NFKC", text)
    out: list[str] = []
    try:
        for m in re.finditer(pattern, t, flags=re.IGNORECASE):
            s = max(0, m.start() - window)
            e = min(len(t), m.end() + window)
            out.append(t[s:e])
    except re.error:
        return []
    return out

def _digits_fuzzy_pattern(digits: str) -> Optional[str]:
    d = re.sub(r"\D", "", digits or "")
    if len(d) < 8:
        return None
    sep = r"[\\s\\-‐―－ー]*"
    return sep.join(map(re.escape, list(d)))

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

    async def _generate_with_timeout(self, content: list[Any], timeout_sec: float | None = None) -> Any:
        """
        Gemini呼び出しに上限時間を設け、ハングでワーカー全体が止まらないようにする。
        """
        if not self.model:
            return None

        async def _call():
            try:
                return await self.model.generate_content_async(content, safety_settings=None)
            except TypeError:
                return await self.model.generate_content_async(content)

        timeout = AI_CALL_TIMEOUT_SEC if timeout_sec is None else float(timeout_sec)
        # 外側の asyncio.wait_for と同値タイムアウトを使うと境界で競合して TimeoutError が
        # 伝播することがあるため、少し短めにして余裕を作る。
        if timeout > 0:
            timeout = max(0.1, timeout - 0.5)
            try:
                return await asyncio.wait_for(_call(), timeout=timeout)
            except asyncio.TimeoutError:
                log.warning(f"Gemini call timed out after {timeout:.1f}s")
                return None
        return await _call()

    def _build_prompt(self, text: str, company_name: str = "", address: str = "") -> str:
        snippet = _shorten_text(text or "", max_len=3500)
        return (
            "あなたは日本企業の公式Webサイトから「代表電話番号」と「本社/本店所在地住所」を抽出する専門家です。\n"
            "加えて、事業内容だけを1文で要約（description）してください。\n"
            "推測は禁止。確証がない場合は null を返してください。\n"
            "出力はJSONのみ（説明文やマークダウンは禁止）。\n"
            "重要:\n"
            "- 支店・営業所・事業所・工場・店舗・センター・倉庫・拠点一覧の住所は、本社/本店である確証がない限り採用しない。\n"
            "- FAX/直通/採用窓口/問い合わせ窓口など、代表以外の番号は採用しない。\n"
            "- Cookie/利用規約/ナビ/フッター/メニュー等の定型文やHTML/CSS/JS断片を住所に混ぜない。\n"
            "- address は「住所のみ」を返す（アクセス案内/営業時間/従業員数/許認可/コード類/地図/連絡先を混ぜない）。\n"
            "- description は事業内容のみ（問い合わせ/採用/アクセス/所在地/電話/URL/メール等は除外）。日本語1文で60〜120文字。迷ったら null。\n"
            "厳守:\n"
            "- csv_address（入力住所）と抽出候補住所の都道府県が不一致なら、住所近傍に「本社所在地/本店所在地」等の明示があり、代表電話も同ページ等で確認できる場合に限り address を返す。迷ったら null。\n"
            f"対象企業: {company_name or '不明'} / csv_address: {address or '不明'}\n"
            "{\n"
            '  "phone_number": "03-1234-5678" または null,\n'
            '  "address": "東京都新宿区西新宿1-1-1 ○○ビル3F" または null,\n'
            '  "description": "事業内容の要約(60〜120文字・日本語1文)" または null,\n'
            '  "confidence": 0.0-1.0,\n'
            '  "evidence": "根拠となる原文抜粋（20〜80文字程度）" または null\n'
            "}\n"
            "evidence は必ず入力（本文テキスト/スクショ内テキスト）に存在する原文の短い抜粋のみ。存在しない文言を作らない。\n"
            f"# 本文テキスト抜粋\n{snippet}\n"
        )

    def _build_description_prompt(self, text: str, company_name: str = "", address: str = "") -> str:
        snippet = _shorten_text(text or "", max_len=3500)
        return (
            "あなたは企業サイトから事業内容だけを1文で要約する専門家です。問い合わせ・採用・アクセス・代表者情報は除外し、事業内容のみを60〜120文字で日本語1文にまとめてください。JSONのみ返してください。\n"
            "{\n"
            '  "description": "〇〇を行う企業です" または null\n'
            "}\n"
            f"対象企業: {company_name or '不明'} / 入力住所: {address or '不明'}\n"
            "禁止: URL, メール, 電話番号, 住所, 募集/採用/問い合わせ/アクセス情報, 記号の羅列。\n"
            f"# 本文テキスト抜粋\n{snippet}\n"
        )

    @staticmethod
    def _validate_description(desc: Optional[str]) -> Optional[str]:
        if not desc:
            return None
        if _looks_mojibake(desc):
            return None
        if "http://" in desc or "https://" in desc or "＠" in desc or "@" in desc:
            return None
        desc = re.sub(r"\s+", " ", desc.strip())
        if len(desc) < 20 or len(desc) > AI_DESCRIPTION_MAX_LEN:
            return None
        if not re.search(r"[ぁ-んァ-ン一-龥]", desc):
            return None
        if re.search(r"[\x00-\x1f\x7f]", desc):
            return None
        return desc

    @staticmethod
    def _validate_rich_description(desc: Optional[str]) -> Optional[str]:
        if not desc:
            return None
        if _looks_mojibake(desc):
            return None
        if "http://" in desc or "https://" in desc or "＠" in desc or "@" in desc:
            return None
        desc = re.sub(r"\s+", " ", desc.strip())
        if len(desc) < AI_DESCRIPTION_MIN_LEN or len(desc) > 160:
            return None
        if not re.search(r"[ぁ-んァ-ン一-龥]", desc):
            return None
        if re.search(r"[\x00-\x1f\x7f]", desc):
            return None
        # 事業内容以外の誘導語を軽く弾く（強すぎると落ちすぎるため最小限）
        if any(k in desc for k in ("お問い合わせ", "採用", "アクセス", "所在地", "電話", "TEL", "FAX")):
            return None
        return desc

    def _build_rich_prompt(self, payload_json: str, company_name: str = "", csv_address: str = "") -> str:
        """
        会社情報の最終選択（1回/社）用プロンプト。
        docs/ai_context.md は system prompt として別途渡される想定。
        """
        snippet = _shorten_text(payload_json or "", max_len=4200)
        return (
            "以下の JSON は、同一ドメイン内の最大2〜3ページからルール抽出した候補群です。\n"
            "推測は禁止。候補に無い値は返さないでください。迷ったら null。\n"
            "出力は JSON のみ。\n"
            f"対象企業: {company_name or '不明'} / csv_address: {csv_address or '不明'}\n"
            "OUTPUT SCHEMA:\n"
            "{\n"
            '  "phone_number": string|null,\n'
            '  "address": string|null,\n'
            '  "representative": string|null,\n'
            '  "company_facts": {"founded": string|null, "capital": string|null, "employees": string|null, "license": string|null},\n'
            '  "industry": string|null,\n'
            '  "business_tags": string[],\n'
            '  "description": string|null,\n'
            '  "confidence": number,\n'
            '  "evidence": string|null,\n'
            '  "description_evidence": [{"url": string, "snippet": string}]\n'
            "}\n"
            "制約:\n"
            "- business_tags は最大5件。\n"
            "- description は80〜160字、日本語1〜2文、事業内容のみ。根拠が薄い/材料が無い場合は null。\n"
            "- description!=null の場合、description_evidence は必ず2件（URLと短い抜粋）。\n"
            "- evidence は住所/電話/代表者の根拠の短い抜粋（無ければ null）。\n"
            f"# CANDIDATES_JSON\n{snippet}\n"
        )

    async def select_company_fields(
        self,
        payload: Dict[str, Any],
        screenshot: bytes | None,
        company_name: str,
        csv_address: str,
    ) -> Optional[Dict[str, Any]]:
        if not self.model:
            return None
        try:
            payload_json = json.dumps(payload, ensure_ascii=False)
        except Exception:
            payload_json = str(payload)
        prompt = self._build_rich_prompt(payload_json, company_name=company_name, csv_address=csv_address)

        def _build_content(use_image: bool) -> list[Any]:
            out: list[Any] = []
            if self.system_prompt:
                out.append(self.system_prompt)
            out.append(prompt)
            if use_image and screenshot:
                try:
                    b64 = base64.b64encode(screenshot).decode("utf-8")
                    out.append({"inline_data": {"mime_type": "image/png", "data": b64}})
                except Exception:
                    pass
            return out

        resp = await self._generate_with_timeout(_build_content(bool(screenshot)))
        if resp is None and screenshot:
            resp = await self._generate_with_timeout(_build_content(False))
        if resp is None:
            return None

        raw = _resp_text(resp)
        data = _extract_first_json(raw)
        if not isinstance(data, dict):
            return None

        phone = _normalize_phone(data.get("phone_number"))
        addr = _normalize_address(data.get("address"))
        rep = data.get("representative")
        rep = re.sub(r"\s+", " ", str(rep)).strip() if isinstance(rep, str) and rep.strip() else None

        facts_in = data.get("company_facts") if isinstance(data.get("company_facts"), dict) else {}
        def _as_str(v: Any, max_len: int = 80) -> Optional[str]:
            if v is None:
                return None
            if not isinstance(v, str):
                v = str(v)
            v = re.sub(r"\s+", " ", v.strip())
            return v[:max_len] if v else None
        company_facts = {
            "founded": _as_str(facts_in.get("founded")),
            "capital": _as_str(facts_in.get("capital")),
            "employees": _as_str(facts_in.get("employees")),
            "license": _as_str(facts_in.get("license")),
        }

        industry = _as_str(data.get("industry"), max_len=60)
        tags_raw = data.get("business_tags")
        tags: list[str] = []
        if isinstance(tags_raw, list):
            for t in tags_raw:
                if isinstance(t, str):
                    tt = re.sub(r"\s+", " ", t.strip())
                    if tt:
                        tags.append(tt[:24])
        tags = tags[:5]

        desc = data.get("description")
        desc = self._validate_rich_description(desc if isinstance(desc, str) else None)

        conf_val = data.get("confidence")
        try:
            conf = float(conf_val) if conf_val is not None else 0.0
        except Exception:
            conf = 0.0
        conf = max(0.0, min(1.0, conf))

        evidence = data.get("evidence")
        if isinstance(evidence, str):
            ev = re.sub(r"\s+", " ", evidence.strip())
            evidence = ev[:200] if len(ev) >= 12 else None
        else:
            evidence = None

        de_raw = data.get("description_evidence")
        description_evidence: list[dict[str, str]] = []
        if isinstance(de_raw, list):
            for item in de_raw:
                if not isinstance(item, dict):
                    continue
                u = item.get("url")
                sn = item.get("snippet")
                if not (isinstance(u, str) and u.strip() and isinstance(sn, str) and sn.strip()):
                    continue
                description_evidence.append(
                    {"url": u.strip()[:500], "snippet": re.sub(r"\s+", " ", sn.strip())[:180]}
                )
        if desc and len(description_evidence) < 2:
            # 仕様違反（根拠不足）なら description を捨てる
            desc = None
            description_evidence = []

        return {
            "phone_number": phone,
            "address": addr,
            "representative": rep,
            "company_facts": company_facts,
            "industry": industry,
            "business_tags": tags,
            "description": desc,
            "confidence": conf,
            "evidence": evidence,
            "description_evidence": description_evidence,
        }

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
            resp = await self._generate_with_timeout(content)
            if resp is None:
                log.warning(f"Gemini API returned no response (timeout/None) for {company_name} ({address}); retrying text-only.")
                resp = await self._generate_with_timeout(_build_content(False))
                if resp is None:
                    return None
            dt = time.time() - t0
            log.info(f"Gemini API call ok: {company_name} ({address}) in {dt:.2f}s")

            raw = _resp_text(resp)
            result = _extract_first_json(raw)
            if not isinstance(result, dict):
                log.warning(f"JSON parse failed: {company_name} ({address}) / raw[:200]={raw[:200]!r}")
                return None

            phone = _normalize_phone(result.get("phone_number"))
            addr = _normalize_address(result.get("address"))
            desc = result.get("description")
            if isinstance(desc, str) and desc.strip():
                desc = self._validate_description(desc)
            else:
                desc = None
            confidence_val = result.get("confidence")
            try:
                confidence = float(confidence_val) if confidence_val is not None else None
            except Exception:
                confidence = None
            if confidence is None:
                confidence = 0.0
            confidence = max(0.0, min(1.0, confidence))

            evidence_raw = result.get("evidence")
            evidence: Optional[str]
            if isinstance(evidence_raw, str):
                cleaned = re.sub(r"\s+", " ", evidence_raw.strip())
                evidence = cleaned[:80] if len(cleaned) >= 20 else None
            else:
                evidence = None

            # -------- AI結果バリデーション --------
            def _looks_like_address(a: str) -> bool:
                if not a:
                    return False
                if "<" in a or ">" in a:
                    return False
                if _NOISE_RE.search(a):
                    return False
                has_zip = bool(re.search(r"\d{3}-\d{4}", a))
                has_pref_city = bool(re.search(r"(都|道|府|県).+?(市|区|郡|町|村)", a))
                return has_zip or has_pref_city

            if addr and not _looks_like_address(addr):
                addr = None

            if evidence and not (phone or addr):
                evidence = None

            # ---- contextual rejection (avoid common mis-picks) ----
            # If the number/address appears only in clearly non-representative contexts, drop it.
            if phone and text:
                pat = _digits_fuzzy_pattern(phone)
                if pat:
                    ctxs = _text_contexts(text, pat, window=32)
                    if ctxs:
                        allow = ("代表", "代表電話", "TEL", "電話")
                        deny = (
                            "FAX", "ファックス", "ﾌｧｯｸｽ",
                            "直通", "ダイヤルイン", "内線",
                            "採用", "求人", "エントリー",
                            "問い合わせ", "お問合せ", "お問合わせ", "お問い合わせ",
                            "窓口", "サポート", "カスタマー",
                        )
                        ok = False
                        for c in ctxs:
                            has_allow = any(k in c for k in allow)
                            has_deny = any(k in c for k in deny)
                            if has_allow and not ("FAX" in c or "ファックス" in c or "ﾌｧｯｸｽ" in c):
                                ok = True
                                break
                            if not has_deny:
                                ok = True
                                break
                        if not ok:
                            phone = None

            if addr and text:
                # First: if the extracted address itself contains strong branch keywords and not HQ keywords, reject.
                hq_markers = ("本社", "本店", "本社所在地", "本店所在地")
                branch_markers = ("支店", "営業所", "事業所", "工場", "店舗", "センター", "倉庫", "拠点", "サテライト")
                if any(k in addr for k in branch_markers) and not any(k in addr for k in hq_markers):
                    addr = None
                else:
                    # If we can find the address in the text, use surrounding labels to reject branch-only picks.
                    nt = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", text))
                    addr_n = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", addr))
                    idx = nt.find(addr_n)
                    if idx != -1:
                        s = max(0, idx - 32)
                        e = min(len(nt), idx + len(addr_n) + 32)
                        ctx = nt[s:e]
                        has_hq = any(k in ctx for k in hq_markers)
                        has_branch = any(k in ctx for k in branch_markers)
                        if has_branch and not has_hq:
                            addr = None

            if evidence and not (phone or addr or desc):
                evidence = None

            return {
                "phone_number": phone,
                "address": addr,
                "description": desc,
                "confidence": confidence,
                "evidence": evidence,
            }

        except GoogleAPIError as e:
            log.warning(f"Google API error for {company_name} ({address}) with image: {e}")
            try:
                resp = await self._generate_with_timeout(_build_content(False))
            except Exception:
                return None
            raw = _resp_text(resp)
            return _extract_first_json(raw) or None
        except Exception as e:
            log.warning(f"Failed to verify info for {company_name} ({address}) with image: {e}", exc_info=True)
            try:
                resp = await self._generate_with_timeout(_build_content(False))
            except Exception:
                return None
            raw = _resp_text(resp)
            return _extract_first_json(raw) or None

    async def generate_description(self, text: str, screenshot: bytes, company_name: str, address: str) -> Optional[str]:
        if not self.model:
            return None

        prompt = self._build_description_prompt(text, company_name, address)

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
            resp = await self._generate_with_timeout(content)
            if resp is None and screenshot:
                log.warning(
                    "generate_description timed out/failed for %s (%s); retrying text-only.",
                    company_name,
                    address,
                )
                resp = await self._generate_with_timeout(_build_content(False))
            if resp is None:
                return None
        except GoogleAPIError as e:
            log.warning("Google API error in generate_description for %s (%s): %s", company_name, address, e)
            try:
                resp = await self._generate_with_timeout(_build_content(False))
            except Exception:
                return None
            if resp is None:
                return None
        except Exception:
            return None

        raw = _resp_text(resp)
        data = _extract_first_json(raw)
        if not isinstance(data, dict):
            return None
        desc = data.get("description")
        if isinstance(desc, str):
            desc = self._validate_description(desc)
        else:
            desc = None
        return desc

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
            "以下を根拠に、true/false と理由、信頼度(0-1)をJSONのみで出力してください。説明やマークダウンは禁止。\n"
            "出力フォーマット:\n"
            "{\\\"is_official\\\": true/false, \\\"confidence\\\": 0.0-1.0, \\\"reason\\\": \"簡潔な根拠\","
            " \\\"is_official_site\\\": true/false, \\\"official_confidence\\\": 0.0-1.0, \\\"official_evidence\\\": [\"根拠\", ...],"
            " \\\"description\\\": \"事業内容の要約(60〜120文字・日本語1文)\" または null}\n"
            "判断基準:\n"
            "- 企業名・所在地・サービス内容の一致を確認。\n"
            "- 口コミ/求人/まとめサイト・予約サイトは false。\n"
            "- URLが企業ドメインや自治体/学校の公式ドメインなら true に寄せる。\n"
            "description のルール:\n"
            "- 事業内容だけを60〜120文字の日本語1文で要約（推測せず、根拠が弱ければ null）\n"
            "- 禁止: URL/メール/電話番号/住所/採用/問い合わせ/アクセス/代表者情報\n"
            f"企業名: {company_name}\n住所: {address}\n候補URL: {url}\n本文抜粋:\n{snippet}\n"
        )
        def _build_content(use_image: bool) -> list[Any]:
            payload: list[Any] = []
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
            resp = await self._generate_with_timeout(content)
            if resp is None:
                log.warning(f"judge_official_homepage timed out/failed for {company_name} ({url}); retrying text-only.")
                resp = await self._generate_with_timeout(_build_content(False))
                if resp is None:
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
            official_verdict = result.get("is_official_site")
            if isinstance(official_verdict, bool):
                is_official_site = official_verdict
            else:
                is_official_site = bool(verdict)
            official_conf_val = result.get("official_confidence")
            try:
                official_confidence = float(official_conf_val) if official_conf_val is not None else confidence
            except Exception:
                official_confidence = confidence
            official_evidence = result.get("official_evidence")
            if isinstance(official_evidence, list):
                official_evidence_list = [str(x) for x in official_evidence if str(x).strip()][:8]
            else:
                official_evidence_list = [reason] if reason else []
            desc = result.get("description")
            if isinstance(desc, str):
                desc = self._validate_description(desc)
            else:
                desc = None
            return {
                "is_official": bool(verdict),
                "confidence": confidence,
                "reason": reason,
                "is_official_site": bool(is_official_site),
                "official_confidence": official_confidence,
                "official_evidence": official_evidence_list,
                "description": desc,
            }
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(f"judge_official_homepage failed for {company_name} ({url}) with image: {exc}")
            try:
                resp = await self._generate_with_timeout(_build_content(False))
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
            official_verdict = result.get("is_official_site")
            if isinstance(official_verdict, bool):
                is_official_site = official_verdict
            else:
                is_official_site = bool(verdict)
            official_conf_val = result.get("official_confidence")
            try:
                official_confidence = float(official_conf_val) if official_conf_val is not None else confidence
            except Exception:
                official_confidence = confidence
            official_evidence = result.get("official_evidence")
            if isinstance(official_evidence, list):
                official_evidence_list = [str(x) for x in official_evidence if str(x).strip()][:8]
            else:
                official_evidence_list = [reason] if reason else []
            desc = result.get("description")
            if isinstance(desc, str):
                desc = self._validate_description(desc)
            else:
                desc = None
            return {
                "is_official": bool(verdict),
                "confidence": confidence,
                "reason": reason,
                "is_official_site": bool(is_official_site),
                "official_confidence": official_confidence,
                "official_evidence": official_evidence_list,
                "description": desc,
            }
