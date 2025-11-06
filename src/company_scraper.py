# src/company_scraper.py
import re, urllib.parse
import asyncio
import unicodedata
from typing import List, Dict, Any, Optional, Iterable
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright, Browser, BrowserContext, Page,
    TimeoutError as PlaywrightTimeoutError, Route
)

try:
    from pykakasi import kakasi as _kakasi_constructor
except Exception:
    _kakasi_constructor = None

try:
    from unidecode import unidecode as _unidecode
except Exception:
    _unidecode = None

# 深掘り時に優先して辿るパス（日本語含む）
PRIORITY_PATHS = [
    "/company", "/about", "/profile", "/corporate", "/overview",
    "/contact", "/inquiry", "/access",
    "/会社概要", "/企業情報", "/企業概要", "/会社情報", "/窓口案内", "/お問い合わせ", "/アクセス"
]
PRIO_WORDS = ["会社概要", "企業情報", "お問い合わせ", "アクセス", "連絡先", "窓口"]

PHONE_RE = re.compile(r"(?:TEL|Tel|tel|電話)\s*[:：]?\s*(0\d{1,4})[-‐―－ー]?(\d{1,4})[-‐―－ー]?(\d{3,4})")
ZIP_RE = re.compile(r"(〒?\s*\d{3})[-‐―－ー]?(\d{4})")
ADDR_HINT = re.compile(r"(都|道|府|県).+?(市|区|郡|町|村)")
ADDR_FALLBACK_RE = re.compile(
    r"(〒\d{3}-\d{4}[^。\n]*|[一-龥]{2,3}[都道府県][^。\n]{0,120}[市区町村郡][^。\n]{0,140})"
)
REP_RE = re.compile(r"(?:代表者|代表取締役|理事長|学長)\s*[:：]?\s*([^\s　<>\|（）\(\)]+)")
LISTING_RE = re.compile(r"(?:上場(?:区分|市場|先)?|株式上場)\s*[:：]?\s*([^\s、。\n]+)")
CAPITAL_RE = re.compile(r"資本金\s*[:：]?\s*([0-9０-９,.]+(?:億|万|千)?円)")
REVENUE_RE = re.compile(r"(?:売上高|売上)\s*[:：]?\s*([0-9０-９,.]+(?:億|万|千)?円(?:以上|程度|規模)?)")
PROFIT_RE = re.compile(r"(?:営業利益|経常利益)\s*[:：]?\s*([0-9０-９,.]+(?:億|万|千)?円)")
FISCAL_RE = re.compile(r"(?:決算(?:月|期)|会計年度|会計期)\s*[:：]?\s*([0-9０-９]{1,2}月(?:末)?|[0-9０-９]{1,2}月期)")
LISTING_KEYWORDS = ("非上場", "未上場", "上場予定なし")
FOUNDED_RE = re.compile(r"(?:設立|創業|創立)\s*[:：]?\s*([0-9０-９]{2,4})年")


class CompanyScraper:
    """
    DuckDuckGo 非JS(html.duckduckgo.com/html)で検索 → 上位リンク取得
    ＋ Playwrightで本文/スクショ取得。
    各ワーカーでブラウザ/コンテキストを使い回して高速化＆安定化。
    """

    # 除外したいドメイン（口コミ/地図/求人など）
    EXCLUDE_DOMAINS = [
        "facebook.com", "twitter.com", "instagram.com", "x.com",
        "linkedin.com", "youtube.com",
        "google.com/maps", "maps.google.com", "map.yahoo.co.jp", "mapion.co.jp",
        "yahoo.co.jp", "itp.ne.jp", "hotpepper.jp", "r.gnavi.co.jp",
        "tabelog.com", "ekiten.jp", "goo.ne.jp", "recruit.net", "en-gage.net",
        "townpage.goo.ne.jp", "jp-hp.com",
        # 集客・旅行・ショッピング系（公式サイトではないケースが多い）
        "rakuten.co.jp", "rakuten.com", "travelko.com", "jalan.net",
        "ikyu.com", "rurubu.jp", "booking.com", "expedia.co.jp",
        "agoda.com", "tripadvisor.jp", "tripadvisor.com", "hotels.com",
        "travel.yahoo.co.jp", "trivago.jp", "trivago.com",
        "jalan.jp", "asoview.com", "tabikobo.com",
    ]

    PRIORITY_PATHS = [
        "/company", "/about", "/profile", "/corporate", "/overview",
        "/contact", "/inquiry", "/access",
        "/会社概要", "/企業情報", "/企業概要", "/会社情報", "/窓口案内", "/お問い合わせ", "/アクセス",
    ]

    NON_OFFICIAL_HOSTS = {
        "travel.rakuten.co.jp",
        "navitime.co.jp",
        "ja.wikipedia.org",
        "kensetumap.com",
        "kaisharesearch.com",
        "hotpepper.jp",
        "tblg.jp",
        "retty.me",
        "goguynet.jp",
        "yahoo.co.jp",
        "mapion.co.jp",
        "google.com",
    }

    NON_OFFICIAL_KEYWORDS = {
        "recruit", "career", "job", "jobs", "kyujin", "haken", "派遣",
        "hotel", "travel", "tour", "booking", "reservation", "yoyaku",
        "mall", "store", "shop", "coupon", "catalog", "price",
        "seikyu", "delivery", "ranking", "review", "口コミ", "比較",
    }

    NON_OFFICIAL_SNIPPET_KEYWORDS = (
        "口コミ", "求人", "求人情報", "転職", "派遣", "予約", "地図", "アクセスマップ",
        "リストス", "上場区分", "企業情報サイト", "まとめ", "一覧", "ランキング", "プラン",
        "sales promotion", "booking", "reservation", "hotel", "travel", "camp",
    )

    CORP_SUFFIXES = [
        "株式会社", "（株）", "(株)", "有限会社", "合同会社", "合名会社", "合資会社",
        "Inc.", "Inc", "Co.", "Co", "Corporation", "Company", "Ltd.", "Ltd",
        "Holding", "Holdings", "HD", "グループ", "ホールディングス", "本社",
    ]

    # 優先的に巡回したいURLのキーワード
    CANDIDATE_PRIORITIES = (
        "会社概要", "会社情報", "企業情報", "corporate", "about",
        "お問い合わせ", "問い合わせ", "contact",
        "アクセス", "access", "本社", "所在地", "沿革",
    )

    _romaji_converter = None  # lazy pykakasi converter

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._pw = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

    # ===== 公式判定ヘルパ =====
    @classmethod
    def _normalize_company_name(cls, company_name: str) -> str:
        if not company_name:
            return ""
        norm = unicodedata.normalize("NFKC", company_name)
        for suffix in cls.CORP_SUFFIXES:
            norm = norm.replace(suffix, "")
        norm = re.sub(r"[\s　]+", "", norm)
        return norm

    @classmethod
    def _romanize(cls, text: str) -> str:
        if not text:
            return ""
        if _kakasi_constructor:
            try:
                if cls._romaji_converter is None:
                    cls._romaji_converter = _kakasi_constructor()
                converter = cls._romaji_converter
                if hasattr(converter, "convert"):
                    parts = converter.convert(text)
                    converted = "".join(
                        item.get("hepburn") or item.get("kana") or item.get("hira") or ""
                        for item in parts
                    )
                    if converted:
                        return converted
                elif hasattr(converter, "getConverter"):
                    legacy = converter.getConverter()
                    converted = legacy.do(text)
                    if converted:
                        return converted
                elif callable(converter):
                    converted = str(converter(text))
                    if converted:
                        return converted
            except Exception:
                cls._romaji_converter = None
        if _unidecode:
            try:
                converted = _unidecode(text)
                if converted:
                    return converted
            except Exception:
                pass
        return ""

    @classmethod
    def _company_tokens(cls, company_name: str) -> List[str]:
        norm = cls._normalize_company_name(company_name)
        tokens = cls._ascii_tokens(norm)
        romaji = cls._romanize(norm)
        tokens.extend(cls._ascii_tokens(romaji))
        seen: set[str] = set()
        ordered: List[str] = []
        for tok in tokens:
            if not tok or tok in seen:
                continue
            seen.add(tok)
            ordered.append(tok)
        return ordered

    @staticmethod
    def _ascii_tokens(text: str) -> List[str]:
        return [tok.lower() for tok in re.findall(r"[A-Za-z0-9]{2,}", text or "")]

    @staticmethod
    def clean_rep_name(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        text = str(raw).strip()
        if not text:
            return None
        # remove parentheses content
        text = re.sub(r"[（(][^）)]*[）)]", "", text)
        # keep only segment before punctuation/newline
        text = re.split(r"[、。\n/|｜,;；]", text)[0]
        text = text.strip(" 　:：-‐―－ー")
        titles = (
            "代表取締役社長", "代表取締役副社長", "代表取締役会長", "代表取締役",
            "代表社員", "代表理事", "理事長", "学長", "園長", "社長",
            "院長", "所長", "支配人", "店主", "代表者", "代表", "CEO", "COO",
        )
        for t in titles:
            if text.startswith(t):
                text = text[len(t):]
                break
        text = text.strip(" 　")
        if text.endswith(("氏", "様")):
            text = text[:-1]
        text = re.sub(r"\s+", " ", text)
        text = text.strip()
        if not text:
            return None
        if len(text) < 2 or len(text) > 20:
            return None
        if any(word in text for word in ("株式会社", "有限会社", "合名会社", "合資会社", "合同会社")):
            return None
        for stop in ("創業", "創立", "創設", "メッセージ", "ご挨拶", "からの", "決裁", "沿革", "代表挨拶"):
            if stop in text:
                return None
        if not re.search(r"[一-龥ぁ-んァ-ン]", text):
            return None
        return text

    @staticmethod
    def _domain_tokens(url: str) -> List[str]:
        host = urlparse(url).netloc.lower()
        host = host.split(":")[0]
        pieces = re.split(r"[.\-]", host)
        ignore = {"www", "co", "or", "ne", "go", "gr", "ed", "lg", "jp", "com", "net", "biz", "inc"}
        return [p for p in pieces if p and p not in ignore]

    def _domain_score(self, company_tokens: List[str], url: str) -> int:
        host = urlparse(url).netloc.lower()
        score = 0
        if re.search(r"\.(co|or|go|ac)\.jp$", host):
            score += 3
        elif host.endswith(".jp"):
            score += 2
        elif host.endswith(".com") or host.endswith(".net"):
            score += 1

        domain_tokens = self._domain_tokens(url)
        for token in company_tokens:
            if any(token in dt for dt in domain_tokens):
                score += 4
            if token and token in host:
                score += 3
        lowered = host + urlparse(url).path.lower()
        if any(kw in lowered for kw in self.NON_OFFICIAL_KEYWORDS):
            score -= 3
        return score

    def _path_priority_value(self, url: str) -> int:
        try:
            path = urllib.parse.urlparse(url).path.lower()
        except Exception:
            return 0
        score = 0
        for idx, marker in enumerate(self.PRIORITY_PATHS):
            if marker.lower() in path:
                score += max(6 - idx, 1)
        return score

    def _is_excluded(self, url: str) -> bool:
        lowered = url.lower()
        return any(ex in lowered for ex in self.EXCLUDE_DOMAINS)

    def _clean_candidate_url(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        href = self._decode_uddg(raw)
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urljoin("https://duckduckgo.com", href)
        return href

    def _extract_search_urls(self, html: str) -> Iterable[str]:
        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select("a.result__a")
        for a in anchors:
            cleaned = self._clean_candidate_url(a.get("href"))
            if not cleaned or self._is_excluded(cleaned):
                continue
            yield cleaned

    async def _fetch_duckduckgo(self, query: str) -> str:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ja,en-US;q=0.9",
            "Referer": "https://duckduckgo.com/",
        }
        for attempt in range(3):
            try:
                resp = requests.get(
                    "https://html.duckduckgo.com/html",
                    params={"q": query, "kl": "jp-jp"},
                    headers=headers,
                    timeout=(5, 30),
                )
                if resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(0.8 * (2 ** attempt))
                    continue
                resp.raise_for_status()
                return resp.text
            except Exception:
                if attempt == 2:
                    return ""
                await asyncio.sleep(0.8 * (2 ** attempt))
        return ""

    def is_likely_official_site(self, company_name: str, url: str, snippet: str = "") -> bool:
        try:
            parsed = urllib.parse.urlparse(url)
        except Exception:
            return False
        host = (parsed.netloc or "").lower().split(":")[0]
        if not host:
            return False

        if any(host == domain or host.endswith(f".{domain}") for domain in self.NON_OFFICIAL_HOSTS):
            return False
        if host.endswith(".go.jp"):
            # 行政機関は企業公式サイトとして扱わない
            return False

        score = 0
        if host.endswith(('.co.jp', '.or.jp', '.ac.jp', '.ed.jp', '.lg.jp', '.gr.jp')):
            score += 4
        elif host.endswith('.jp'):
            score += 2
        elif host.endswith('.com') or host.endswith('.net'):
            score += 1

        company_tokens = self._company_tokens(company_name)
        domain_tokens = self._domain_tokens(url)
        for token in company_tokens:
            if any(token in dt for dt in domain_tokens):
                score += 3
            if token and token in host:
                score += 2

        lowered = (snippet or "").lower()
        if "公式" in snippet or "official" in lowered:
            score += 2
        if any(kw in lowered for kw in self.NON_OFFICIAL_SNIPPET_KEYWORDS):
            score -= 3
        if any(kw in host for kw in self.NON_OFFICIAL_KEYWORDS):
            score -= 3

        return score >= 2

    # ===== 高速化の肝：ブラウザを起動して使い回す =====
    async def start(self):
        if self.browser:
            return
        self._pw = await async_playwright().start()
        self.browser = await self._pw.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",  # /dev/shm不足でのクラッシュ回避
            ],
        )
        self.context = await self.browser.new_context(
            locale="ja-JP",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        # 軽量化：画像/フォント/メディア/スタイルをブロック
        await self.context.route("**/*", self._handle_route)

    async def close(self):
        try:
            if self.context:
                await self.context.close()
        finally:
            try:
                if self.browser:
                    await self.browser.close()
            finally:
                if self._pw:
                    await self._pw.stop()
        self._pw = None
        self.browser = None
        self.context = None

    async def _handle_route(self, route: Route):
        rtype = route.request.resource_type
        if rtype in {"image", "media", "font", "stylesheet"}:
            await route.abort()
        else:
            await route.continue_()

    # ===== 検索 =====
    @staticmethod
    def _decode_uddg(url: str) -> str:
        if not url:
            return url
        try:
            parsed = urlparse("https://duckduckgo.com" + url) if url.startswith("/l") else urlparse(url)
            if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l"):
                qs = parse_qs(parsed.query)
                if "uddg" in qs and qs["uddg"]:
                    return unquote(qs["uddg"][0])
        except Exception:
            pass
        return url

    def _prioritize(self, urls: List[str]) -> List[str]:
        def score(u: str) -> int:
            s = 0
            low = u.lower()
            if any(k in low for k in ("recruit", "採用", "ir", "faq", "support", "news")):
                s -= 3
            for k in self.CANDIDATE_PRIORITIES:
                if k.lower() in low:
                    s += 2
            if low.startswith("https://"):
                s += 1
            return s
        return sorted(urls, key=score, reverse=True)

    def _prioritize_paths(self, urls: List[str]) -> List[str]:
        def score(u: str) -> int:
            path = urllib.parse.urlparse(u).path.lower()
            total = 0
            for idx, marker in enumerate(self.PRIORITY_PATHS):
                if marker.lower() in path:
                    total += len(self.PRIORITY_PATHS) - idx
            return total

        return sorted(urls, key=score, reverse=True)

    @staticmethod
    def _phone_variants_regex(phone: str) -> re.Pattern:
        digits = re.sub(r"\D", "", phone or "")
        if not digits:
            return re.compile(r"$^")
        pattern = r"\D*".join(map(re.escape, digits))
        return re.compile(pattern)

    @staticmethod
    def _addr_key(addr: str) -> str:
        if not addr:
            return ""
        text = unicodedata.normalize("NFKC", addr)
        text = re.sub(r"[‐―－ーｰ-]+", "-", text)
        text = re.sub(r"\s+", "", text)
        return text.lower()

    async def verify_on_site(
        self,
        base_url: str,
        phone: Optional[str],
        address: Optional[str],
        fetch_limit: int = 5,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "phone_ok": False,
            "address_ok": False,
            "phone_url": None,
            "address_url": None,
        }
        if not base_url:
            return result

        try:
            parsed = urllib.parse.urlparse(base_url)
        except Exception:
            return result

        if not parsed.scheme or not parsed.netloc:
            return result

        base_root = f"{parsed.scheme}://{parsed.netloc}"
        candidates: List[str] = [base_url]
        for path in self.PRIORITY_PATHS:
            try:
                candidate = urllib.parse.urljoin(base_root, path)
            except Exception:
                continue
            candidates.append(candidate)

        seen: set[str] = set()
        targets: List[str] = []
        for url in candidates:
            parsed_candidate = urllib.parse.urlparse(url)
            if parsed_candidate.netloc != parsed.netloc:
                continue
            if url in seen:
                continue
            seen.add(url)
            targets.append(url)
            if len(targets) >= fetch_limit:
                break

        phone_pattern = self._phone_variants_regex(phone) if phone else None
        addr_key = self._addr_key(address) if address else ""

        for target in targets:
            try:
                info = await self.get_page_info(target)
            except Exception:
                continue
            text = info.get("text", "") or ""
            if phone_pattern and not result["phone_ok"]:
                if phone_pattern.search(text):
                    result["phone_ok"] = True
                    result["phone_url"] = target
            if addr_key and not result["address_ok"]:
                text_key = self._addr_key(text)
                if addr_key and addr_key in text_key:
                    result["address_ok"] = True
                    result["address_url"] = target
            if result["phone_ok"] and result["address_ok"]:
                break

        return result

    async def search_company(self, company_name: str, address: str, num_results: int = 3) -> List[str]:
        """
        DuckDuckGoで検索し、候補URLを返す（「公式サイト」クエリを優先）。
        """
        base_name = (company_name or "").strip()
        base_address = (address or "").strip()
        queries: List[str] = []
        if base_name:
            queries.append(f"{base_name} 公式サイト")
        if base_name and base_address:
            q_addr = f"{base_name} {base_address}".strip()
            if q_addr and q_addr not in queries:
                queries.append(q_addr)
        if base_name and base_name not in queries:
            queries.append(base_name)
        if not queries:
            return []

        candidates: List[Dict[str, Any]] = []
        seen: set[str] = set()
        max_candidates = max(num_results * 3, 12)

        for q_idx, query in enumerate(queries):
            html = await self._fetch_duckduckgo(query)
            if not html:
                continue
            for rank, url in enumerate(self._extract_search_urls(html)):
                if url in seen:
                    continue
                seen.add(url)
                candidates.append({"url": url, "query_idx": q_idx, "rank": rank})
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= num_results:
                break

        if not candidates:
            return []

        company_tokens = self._company_tokens(company_name)
        scored: List[tuple[int, int, int, str]] = []
        for item in candidates:
            url = item["url"]
            score = self._domain_score(company_tokens, url)
            score += max(0, 6 - item["rank"])
            if item["query_idx"] == 0:
                score += 3
            score += self._path_priority_value(url)
            scored.append((score, item["query_idx"], item["rank"], url))

        scored.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
        ordered: List[str] = []
        for _, _, _, url in scored:
            ordered.append(url)
        return ordered[:num_results]

    # ===== ページ取得（ブラウザ再利用＋軽いリトライ） =====
    async def get_page_info(self, url: str, timeout: int = 25000) -> Dict[str, Any]:
        """
        対象URLの本文テキストとフルページスクショを取得（2回まで再試行）
        """
        if not self.context:
            await self.start()

        for attempt in range(2):
            page: Page = await self.context.new_page()
            page.set_default_timeout(timeout)
            try:
                await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
                try:
                    text = await page.inner_text("body", timeout=5000)
                except Exception:
                    try:
                        await page.wait_for_load_state("load", timeout=timeout)
                    except Exception:
                        pass
                    text = await page.inner_text("body") if await page.locator("body").count() else ""
                try:
                    html = await page.content()
                except Exception:
                    html = ""
                screenshot = await page.screenshot(full_page=True)
                return {"url": url, "text": text, "html": html, "screenshot": screenshot}

            except PlaywrightTimeoutError:
                # 軽く待ってリトライ
                await asyncio.sleep(0.7 * (attempt + 1))
            except Exception:
                # 予期せぬ例外も1回だけ再試行
                await asyncio.sleep(0.7 * (attempt + 1))
            finally:
                await page.close()

        return {"url": url, "text": "", "html": "", "screenshot": b""}

    # ===== 同一ドメイン内を浅く探索 =====
    def _rank_links(self, base: str, html: str) -> List[str]:
        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html or "", flags=re.I)
        base_host = urlparse(base).netloc
        candidates: List[tuple[int, str]] = []
        for href in hrefs:
            url = urljoin(base, href)
            parsed = urlparse(url)
            if not parsed.netloc or parsed.netloc != base_host:
                continue
            path = parsed.path or "/"
            score = 0
            for p in PRIORITY_PATHS:
                if p in path:
                    score += 10
            lowered = url.lower()
            for word in PRIO_WORDS:
                if word in lowered:
                    score += 5
            if score > 0:
                candidates.append((score, url))

        candidates.sort(key=lambda x: (-x[0], x[1]))
        seen: set[str] = set()
        ordered: List[str] = []
        for _, url in candidates:
            if url not in seen:
                ordered.append(url)
                seen.add(url)
            if len(ordered) >= 20:
                break
        return ordered

    async def crawl_related(
        self,
        homepage: str,
        need_phone: bool,
        need_addr: bool,
        need_rep: bool,
        max_pages: int = 6,
        max_hops: int = 2,
    ) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        if not homepage:
            return results

        visited: set[str] = {homepage}
        queue: List[tuple[int, str]] = [(0, homepage)]
        while queue and len(results) < max_pages:
            hop, url = queue.pop(0)
            try:
                info = await self.get_page_info(url)
            except Exception:
                continue

            results[url] = {
                "text": info.get("text", "") or "",
                "screenshot": info.get("screenshot"),
                "html": info.get("html", ""),
            }

            if hop >= max_hops:
                continue

            missing: List[str] = []
            if need_phone:
                missing.append("phone")
            if need_addr:
                missing.append("addr")
            if need_rep:
                missing.append("rep")
            if not missing:
                continue

            html = info.get("html", "") or ""
            if not html:
                continue
            for child in self._rank_links(url, html):
                if child not in visited:
                    visited.add(child)
                    queue.append((hop + 1, child))
                    if len(queue) + len(results) >= max_pages:
                        break

        return results

    # ===== 抽出 =====
    def extract_candidates(self, text: str) -> Dict[str, List[str]]:
        phones: List[str] = []
        addrs: List[str] = []
        reps: List[str] = []

        for p in PHONE_RE.finditer(text or ""):
            phones.append(f"{p.group(1)}-{p.group(2)}-{p.group(3)}")

        for zm in ZIP_RE.finditer(text or ""):
            zip_code = f"〒{zm.group(1).replace('〒', '').strip()}-{zm.group(2)}"
            cursor = zm.end()
            snippet = (text or "")[cursor:cursor + 120].replace("\n", " ")
            if ADDR_HINT.search(snippet):
                seg = snippet.split(" ")[0].strip()
                addrs.append(f"{zip_code} {seg}")

        if not addrs:
            addrs.extend(ADDR_FALLBACK_RE.findall(text or ""))

        for rm in REP_RE.finditer(text or ""):
            cleaned = self.clean_rep_name(rm.group(1))
            if cleaned:
                reps.append(cleaned)

        listings: List[str] = []
        for lm in LISTING_RE.finditer(text or ""):
            val = lm.group(1).strip()
            val = re.split(r"[、。\s/|]", val)[0]
            if val:
                listings.append(val)
        if not listings:
            lowered = (text or "").lower()
            for term in LISTING_KEYWORDS:
                if term in text or term.lower() in lowered:
                    listings.append(term)
                    break

        capitals = [m.group(1).strip() for m in CAPITAL_RE.finditer(text or "")]
        revenues = [m.group(1).strip() for m in REVENUE_RE.finditer(text or "")]
        profits = [m.group(1).strip() for m in PROFIT_RE.finditer(text or "")]
        fiscal_months = [m.group(1).strip() for m in FISCAL_RE.finditer(text or "")]
        founded_years: List[str] = []
        for fm in FOUNDED_RE.finditer(text or ""):
            val = fm.group(1).strip()
            val = unicodedata.normalize("NFKC", val)
            if len(val) == 2 and val.isdigit():
                # Heisei/Showa not handled; skip ambiguous short years
                continue
            if val.isdigit():
                founded_years.append(val)

        def dedupe(seq: List[str]) -> List[str]:
            seen: set[str] = set()
            out: List[str] = []
            for item in seq:
                if item and item not in seen:
                    seen.add(item)
                    out.append(item)
            return out

        return {
            "phone_numbers": dedupe(phones),
            "addresses": dedupe(addrs),
            "rep_names": dedupe(reps),
            "listings": dedupe(listings),
            "capitals": dedupe(capitals),
            "revenues": dedupe(revenues),
            "profits": dedupe(profits),
            "fiscal_months": dedupe(fiscal_months),
            "founded_years": dedupe(founded_years),
        }
