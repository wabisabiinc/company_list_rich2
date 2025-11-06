import pytest
from unittest.mock import patch, MagicMock
from src.company_scraper import CompanyScraper

# 絶対/相対の /l/?uddg=..., 相対パス, 除外ドメインを含むサンプル
SAMPLE_HTML = """
<html>
  <body>
    <!-- 相対の /l/?uddg= -->
    <a class="result__a" href="/l/?uddg=https%3A%2F%2Fexample.com%2Fhome">Example</a>
    <!-- 絶対の /l/?uddg= -->
    <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fabs.co.jp%2F">ABS</a>
    <!-- 除外ドメイン -->
    <a class="result__a" href="https://facebook.com/profile">FB</a>
    <!-- プロトコルなし -->
    <a class="result__a" href="//bar.com/page">Bar</a>
    <!-- 相対パス -->
    <a class="result__a" href="/relative/path">Rel</a>
    <!-- 空 href -->
    <a class="result__a" href="">Empty</a>
    <!-- 旅行系集客ドメイン（除外対象） -->
    <a class="result__a" href="https://travel.rakuten.co.jp/hotel/123">Rakuten Travel</a>
    <!-- 除外ドメイン -->
    <a class="result__a" href="https://twitter.com/foo">TW</a>
  </body>
</html>
"""

@pytest.fixture
def scraper():
    return CompanyScraper(headless=True)

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_filters_and_resolves(mock_get, scraper):
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    urls = await scraper.search_company("トヨタ自動車株式会社", "愛知県豊田市", num_results=10)
    first_query = mock_get.call_args_list[0].kwargs["params"]["q"]
    assert first_query.endswith("公式サイト")

    # /l/?uddg= が正しく剥がれている（相対/絶対）
    assert "https://example.com/home" in urls
    assert any(u.startswith("https://abs.co.jp") for u in urls)

    # 除外ドメインが含まれない
    assert not any("facebook.com" in u for u in urls)
    assert not any("twitter.com" in u for u in urls)
    assert not any("rakuten" in u for u in urls)

    # プロトコルなし → https
    assert any(u.startswith("https://bar.com") for u in urls)

    # 相対パス → duckduckgo に連結
    assert any(u.startswith("https://duckduckgo.com/relative/path") for u in urls)

    # 空文字を含まない & 上限件数
    assert all(u for u in urls)
    assert len(urls) <= 10

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_limit_num_results(mock_get, scraper):
    mock_resp = MagicMock()
    mock_resp.text = SAMPLE_HTML
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    urls = await scraper.search_company("社名", "住所", num_results=2)
    assert len(urls) == 2

@pytest.mark.asyncio
@patch("src.company_scraper.requests.get")
async def test_search_company_empty_on_http_error(mock_get, scraper):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("HTTP Error")
    mock_get.return_value = mock_resp

    urls = await scraper.search_company("社名", "住所")
    assert urls == []


def test_is_likely_official_site_true(scraper):
    text = "会社概要\n株式会社Exampleは・・・"
    assert scraper.is_likely_official_site(
        "株式会社Example",
        "https://www.example.co.jp/about",
        {"text": text, "html": f"<title>{text}</title>"},
    )


def test_is_likely_official_site_false(scraper):
    text = "楽天トラベルで株式会社Exampleの宿泊プラン"
    assert not scraper.is_likely_official_site(
        "株式会社Example",
        "https://travel.rakuten.co.jp/hotel/123",
        {"text": text},
    )


def test_is_likely_official_site_romaji(scraper):
    text = "会社概要とお問い合わせ"
    assert scraper.is_likely_official_site(
        "株式会社創明社",
        "https://someisha.co.jp",
        text,
    )
