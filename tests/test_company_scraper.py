import re
import urllib.parse
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page

class CompanyScraper:
    """
    DuckDuckGo の非JS版 (html.duckduckgo.com/html) を叩いて
    企業名＋住所の検索結果リンク上位を取得するクラスです。
    """

    # 除外したいドメインのリスト
    EXCLUDE_DOMAINS = [
        "facebook.com",
        "twitter.com",
        "instagram.com",
        "maps.google.com",
    ]

    def __init__(self, headless: bool = True):
        """
        :param headless: True ならブラウザを表示せずに起動、False ならウィンドウを開く
        （get_page_info 用）
        """
        self.headless = headless

    async def search_company(
        self,
        company_name: str,
        address: str,
        num_results: int = 3
    ) -> List[str]:
        """
        DuckDuckGo の非JS版 HTML エンドポイントを呼び出して
        結果リンクを取得します。

        :param company_name: 企業名（例: "トヨタ自動車株式会社"）
        :param address:       住所（例: "愛知県豊田市"）
        :param num_results:   取得する上位リンク数
        :return: リンクのリスト（除外ドメインをフィルタリング）
        """
        query = f"{company_name} {address}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        # 非JS版エンドポイントに GET
        resp = requests.get(
            "https://html.duckduckgo.com/html",
            params={"q": query},
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()

        # HTML をパース
        soup = BeautifulSoup(resp.text, "html.parser")
        # result__a クラスのリンクを優先して取得
        anchors = soup.select("a.result__a")
        urls: List[str] = []

        for a in anchors:
            href = a.get("href")
            if not href:
                continue

            # DuckDuckGo の /l/?uddg=... リダイレクトを展開
            if "duckduckgo.com/l/" in href and "uddg=" in href:
                parsed = urllib.parse.urlparse(href)
                qs = urllib.parse.parse_qs(parsed.query)
                real = qs.get("uddg", [None])[0]
                if real:
                    href = urllib.parse.unquote(real)

            # 相対URLは絶対化
            if href.startswith("/"):
                href = urllib.parse.urljoin("https://duckduckgo.com", href)

            # 除外ドメインをフィルタ
            if any(dom in href for dom in self.EXCLUDE_DOMAINS):
                continue

            urls.append(href)
            if len(urls) >= num_results:
                break

        return urls

    async def get_page_info(self, url: str) -> Dict[str, Any]:
        """
        与えられた URL を Playwright で開き、
        ページ本文テキストとフルページスクリーンショットを取得します。
        """
        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(
                headless=self.headless,
                args=["--no-sandbox"]
            )
            page: Page = await browser.new_page()
            await page.goto(url, timeout=30000)
            text = await page.inner_text("body")
            screenshot = await page.screenshot(full_page=True)
            await browser.close()
            return {"text": text, "screenshot": screenshot}

    def extract_candidates(self, text: str) -> Dict[str, List[str]]:
        """
        ページ本文テキストから電話番号と住所候補を抽出
        （テスト済みの既存実装と同一です）。
        """
        phone_pattern = re.compile(
            r"(?:\+?81[-\s]?)?(?:0\d{1,4})[-\s]?\d{1,4}[-\s]?\d{4}"
        )
        address_pattern = re.compile(
            r"(〒\d{3}-\d{4}|[一-龥]{2,3}[都道府県].{0,20}[市区町村].{0,20})"
        )

        phones = phone_pattern.findall(text)
        addrs  = address_pattern.findall(text)
        return {
            "phone_numbers": list(set(phones)),
            "addresses":     list(set(addrs))
        }
