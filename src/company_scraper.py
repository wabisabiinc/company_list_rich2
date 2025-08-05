# src/company_scraper.py
import os
import re
from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError


class CompanyScraper:
    """
    DuckDuckGo の非JS版 HTML エンドポイント (html.duckduckgo.com/html) を使って
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
        上位リンクを取得します。

        :param company_name: 企業名（例: "トヨタ自動車株式会社"）
        :param address:       住所（例: "愛知県豊田市"）
        :param num_results:   取得する上位リンク数
        :return: フィルタ済み URL のリスト
        """
        query = f"{company_name} {address}".strip()
        try:
            resp = requests.get(
                "https://html.duckduckgo.com/html",
                params={"q": query},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30
            )
            resp.raise_for_status()
        except Exception:
            # HTTP エラー等が起きたら空リストで返す
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        anchors = soup.select("a.result__a")
        results: List[str] = []

        for a in anchors:
            raw = a.get("href")
            if not raw:
                continue

            # まず除外ドメインチェック（raw段階でも大体フィルタ）
            if any(dom in raw for dom in self.EXCLUDE_DOMAINS):
                continue

            href = raw

            # DuckDuckGo のリダイレクト (/l/?uddg=...) を展開
            if raw.startswith("/l/") and "uddg=" in raw:
                parsed = urlparse(raw)
                qs = parse_qs(parsed.query)
                if "uddg" in qs and qs["uddg"]:
                    href = unquote(qs["uddg"][0])
                else:
                    continue

            # プロトコルなし //foo の場合
            elif href.startswith("//"):
                href = "https:" + href
            # 相対パス /foo の場合
            elif href.startswith("/"):
                href = urljoin("https://duckduckgo.com", href)

            # 展開後にも除外ドメインがあればスキップ
            if any(dom in href for dom in self.EXCLUDE_DOMAINS):
                continue

            results.append(href)
            if len(results) >= num_results:
                break

        return results

    async def get_page_info(
        self,
        url: str,
        timeout: int = 30000
    ) -> Dict[str, Any]:
        """
        指定 URL のページ本文テキストとフルページスクリーンショットを取得します。
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
            except PlaywrightTimeoutError:
                return {"text": "", "screenshot": b""}
            finally:
                await browser.close()

    def extract_candidates(self, text: str) -> Dict[str, List[str]]:
        """
        ページ本文テキストから電話番号と住所候補を抽出します。
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
