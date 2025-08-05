# tests/test_company_scraper.py

import pytest
import urllib.parse
from unittest.mock import patch, MagicMock
from src.company_scraper import CompanyScraper

# テスト用サンプル HTML （DuckDuckGo の非JS版レスポンスを模倣）
SAMPLE_HTML = """
<html>
  <body>
    <!-- リダイレクトURL（/l/?uddg=...） -->
    <a class="result__a" href="/l/?uddg=https%3A%2F%2Fexample.com%2Fhome">Example</a>
    <!-- 除外ドメイン facebook.com -->
    <a class="result__a" href="https://facebook.com/profile">FB</a>
    <!-- プロトコルなし //bar.com → https://bar.com -->
    <a class="result__a" href="//bar.com/page">Bar</a>
    <!-- 相対パス /relative/path → https://duckduckgo.com/relative/path -->
    <a class="result__a" href="/relative/path">Rel</a>
    <!-- 空 href はスキップ -->
    <a class="result__a" href="">Empty</a>
    <!-- さらに別ドメイン -->
    <a class="result__a" href="https://twitter.com/foo">TW</a>
  </body>
</html>
"""

@pytest.fixture
def scraper():
    # headless やタイムアウトはテスト意図に合わせればOK
    return CompanyScraper(headless=True)

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_filters_and_resolves(mock_get, scraper):
    # モックレスポンスのセットアップ
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    # 実行
    urls = await scraper.search_company("トヨタ自動車株式会社", "愛知県豊田市", num_results=10)

    # → リダイレクト URL が正しく展開されている
    assert "https://example.com/home" in urls

    # → facebook.com, twitter.com は除外されている
    assert not any("facebook.com" in u for u in urls)
    assert not any("twitter.com"  in u for u in urls)

    # → プロトコルなし URL が https に変換されている
    assert any(u.startswith("https://bar.com") for u in urls)

    # → 相対パスが https://duckduckgo.com に貼り直されている
    assert any(u.startswith("https://duckduckgo.com/relative/path") for u in urls)

    # → 空文字 href は含まれない
    assert all(u for u in urls)

    # → num_results を超えていない
    assert len(urls) <= 10

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_limit_num_results(mock_get, scraper):
    # 同じ SAMPLE_HTML を返すモックをセット
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    # num_results を 2 に指定
    urls = await scraper.search_company("社名", "住所", num_results=2)

    # 返却件数が 2 件のみであること
    assert len(urls) == 2

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_empty_on_http_error(mock_get, scraper):
    # HTTP エラーを発生させる
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("HTTP Error")
    mock_get.return_value = mock_resp

    urls = await scraper.search_company("社名", "住所")
    # エラー発生時は空リスト返却
    assert urls == []

