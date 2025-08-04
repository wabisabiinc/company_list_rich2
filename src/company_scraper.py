import os
import re
from typing import List, Dict, Any
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Browser,
    Page
)

class CompanyScraper:
    """
    DuckDuckGo の静的 HTML レスポンス (html.duckduckgo.com/html/) を使って
    企業ホームページ URL を自動取得するクラス。
    実行時の状況を分かりやすくするためにデバッグプリントを随所に挿入しています。
    """

    # 除外したいドメインをこちらに列挙
    EXCLUDE_DOMAINS = [
        "facebook.com",
        "twitter.com",
        "instagram.com",
        "maps.google.com",
    ]

    def __init__(self, headless: bool = None):
        """
        :param headless: ヘッドレスモードの有効/無効。
                         None の場合、環境変数 HEADLESS を優先し、なければ True。
        """
        if headless is None:
            env = os.getenv("HEADLESS")
            self.headless = env.lower() in ("true", "1") if env else True
        else:
            self.headless = headless

    async def search_company(
        self,
        company_name: str,
        address: str,
        num_results: int = 3,
        timeout: int = 30000
    ) -> List[str]:
        """
        DuckDuckGo HTML 版 (静的、XHRなし) で企業名＋住所を検索し、上位リンクを取得。
        :return: フィルタ済み URL のリスト
        """
        query = f"{company_name} {address}".strip()
        # ← ここを duckduckgo.com から html.duckduckgo.com に変更！
        url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        print(f"[DEBUG] search URL → {url}")

        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(
                headless=self.headless,
                args=["--no-sandbox"]
            )
            page: Page = await browser.new_page()
            try:
                # 検索ページを開く
                await page.goto(url, timeout=timeout)
                print("[DEBUG] page loaded, collecting anchors")
                selector = "a.result__a"
                await page.wait_for_selector(selector, timeout=timeout)
                anchors = await page.query_selector_all(selector)

                results: List[str] = []
                for a in anchors[:num_results]:
                    raw = await a.get_attribute("href")
                    if not raw:
                        continue
                    # 不要ドメイン除外
                    if any(domain in raw for domain in self.EXCLUDE_DOMAINS):
                        continue

                    href = raw
                    # DuckDuckGo のリダイレクト /l/… の場合 uddg パラメータをデコード
                    if raw.startswith("/l/"):
                        qs = parse_qs(urlparse(raw).query)
                        if "uddg" in qs and qs["uddg"]:
                            href = unquote(qs["uddg"][0])
                        else:
                            continue
                    # //… → https://…
                    if href.startswith("//"):
                        href = "https:" + href
                    # /path → duckduckgo.com ドメインを補完
                    elif href.startswith("/"):
                        href = "https://duckduckgo.com" + href

                    results.append(href)

                print(f"[DEBUG] found URLs → {results}")
                return results

            except PlaywrightTimeoutError:
                print("[DEBUG] TimeoutError → returning empty list")
                return []

            finally:
                await browser.close()

    async def get_page_info(
        self,
        url: str,
        timeout: int = 30000
    ) -> Dict[str, Any]:
        """
        指定 URL のページ本文テキストとスクリーンショットを取得。
        :return: {"text": ページ本文テキスト, "screenshot": PNGバイト列}
        """
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(
                headless=self.headless,
                args=["--no-sandbox"]
            )
            page: Page = await browser.new_page()
            try:
                await page.goto(url, timeout=timeout)
                text = await page.inner_text("body")
                screenshot = await page.screenshot(full_page=True)
                return {"text": text, "screenshot": screenshot}
            finally:
                await browser.close()

    def extract_candidates(self, text: str) -> Dict[str, List[str]]:
        """
        ページ本文テキストから電話番号と住所候補を抽出。
        テストが期待する形で返却します。
        """
        phone_re = re.compile(
            r"(?:\+?81[-\s]?)?(?:0\d{1,4})[-\s]?\d{1,4}[-\s]?\d{4}"
        )
        addr_re = re.compile(
            r"(〒\d{3}-\d{4}|[一-龥]{2,3}[都道府県].{0,20}[市区町村].{0,20})"
        )
        phones = phone_re.findall(text)
        addrs = addr_re.findall(text)
        return {
            "phone_numbers": list(set(phones)),
            "addresses": list(set(addrs))
        }
