# src/company_scraper.py
import re
import asyncio
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright, Browser, BrowserContext, Page,
    TimeoutError as PlaywrightTimeoutError, Route
)


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
    ]

    # 優先的に巡回したいURLのキーワード
    CANDIDATE_PRIORITIES = (
        "会社概要", "会社情報", "企業情報", "corporate", "about",
        "お問い合わせ", "問い合わせ", "contact",
        "アクセス", "access", "本社", "所在地", "沿革",
    )

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._pw = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

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

    async def search_company(self, company_name: str, address: str, num_results: int = 3) -> List[str]:
        """
        DuckDuckGoで検索し、候補URLを返す（軽いリトライ＆バックオフ付き）
        """
        query = f"{company_name} {address}".strip()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ja,en-US;q=0.9",
            "Referer": "https://duckduckgo.com/",
        }

        # 最大3回リトライ（指数バックオフ）
        resp_text = ""
        for attempt in range(3):
            try:
                resp = requests.get(
                    "https://html.duckduckgo.com/html",
                    params={"q": query, "kl": "jp-jp"},
                    headers=headers,
                    timeout=(5, 30),  # 接続5秒 / 応答30秒
                )
                # 429/5xx は待ってリトライ
                if resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(0.8 * (2 ** attempt))
                    continue
                resp.raise_for_status()
                resp_text = resp.text
                break
            except Exception:
                if attempt == 2:
                    return []
                await asyncio.sleep(0.8 * (2 ** attempt))

        soup = BeautifulSoup(resp_text, "html.parser")
        anchors = soup.select("a.result__a")
        results: List[str] = []
        for a in anchors:
            raw = a.get("href")
            if not raw:
                continue
            href = self._decode_uddg(raw)
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = urljoin("https://duckduckgo.com", href)
            if any(ex in href for ex in self.EXCLUDE_DOMAINS):
                continue
            results.append(href)

        if not results:
            return []
        ordered = self._prioritize(results)
        seen: set[str] = set()
        deduped: List[str] = []
        for u in ordered:
            if u not in seen:
                seen.add(u)
                deduped.append(u)
        return deduped[:num_results]

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
                screenshot = await page.screenshot(full_page=True)
                return {"text": text, "screenshot": screenshot}

            except PlaywrightTimeoutError:
                # 軽く待ってリトライ
                await asyncio.sleep(0.7 * (attempt + 1))
            except Exception:
                # 予期せぬ例外も1回だけ再試行
                await asyncio.sleep(0.7 * (attempt + 1))
            finally:
                await page.close()

        return {"text": "", "screenshot": b""}

    # ===== 抽出 =====
    def extract_candidates(self, text: str) -> Dict[str, List[str]]:
        phone_pattern = re.compile(
            r"(?:\+?81[-\s]?)?(0\d{1,4})[-\s]?(\d{1,4})[-\s]?(\d{3,4})"
        )
        address_pattern = re.compile(
            r"(〒\d{3}-\d{4}[^。\n]*|[一-龥]{2,3}[都道府県][^。\n]{0,120}[市区町村郡][^。\n]{0,140})"
        )

        phones: List[str] = []
        for m in phone_pattern.finditer(text):
            g1, g2, g3 = m.groups()
            phones.append(f"{g1}-{g2}-{g3}")

        addrs = address_pattern.findall(text)

        def dedupe(seq: List[str]) -> List[str]:
            seen = set()
            out: List[str] = []
            for x in seq:
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        return {"phone_numbers": dedupe(phones), "addresses": dedupe(addrs)}
