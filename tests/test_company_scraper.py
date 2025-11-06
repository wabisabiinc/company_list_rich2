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


def test_normalize_homepage_url_contact(scraper):
    html = """
    <html>
      <head><title>お問い合わせ</title></head>
      <body>問い合わせフォームです。</body>
    </html>
    """
    normalized = scraper.normalize_homepage_url(
        "https://www.example.co.jp/contact/index.html",
        {"html": html},
    )
    assert normalized == "https://www.example.co.jp/"


def test_normalize_homepage_url_canonical(scraper):
    html = """
    <html>
      <head>
        <link rel="canonical" href="https://corp.example.co.jp/company/overview" />
      </head>
      <body>会社概要</body>
    </html>
    """
    normalized = scraper.normalize_homepage_url(
        "https://corp.example.co.jp/company/overview?ref=ddg",
        {"html": html},
    )
    assert normalized == "https://corp.example.co.jp/company/overview"


def test_clean_rep_name_removes_union_title(scraper):
    assert scraper.clean_rep_name("組合長　田中太郎") == "田中太郎"
    assert scraper.clean_rep_name("代表理事組合長 田中太郎") == "田中太郎"
    assert scraper.clean_rep_name("組合長") is None


def test_is_likely_official_site_excludes_known_aggregator(scraper):
    text = "観陽亭のご案内"
    assert not scraper.is_likely_official_site(
        "株式会社観陽亭",
        "https://tsukumado.com/member/51/",
        {"text": text},
    )


def test_is_likely_official_site_suspect_host_with_address(scraper):
    text = "〒409-2937 山梨県南巨摩郡身延町身延一色1350 株式会社創明社の公式サイトです。"
    extracted = {
        "addresses": ["〒409-2937 山梨県南巨摩郡身延町身延一色1350"],
    }
    assert scraper.is_likely_official_site(
        "株式会社創明社",
        "https://www.big-advance.site/c/158/1300",
        {"text": text},
        "〒409-2937 山梨県南巨摩郡身延町身延一色1350",
        extracted,
    )


def test_is_likely_official_site_suspect_host_without_address(scraper):
    text = "株式会社創明社の紹介ページです。"
    assert not scraper.is_likely_official_site(
        "株式会社創明社",
        "https://www.big-advance.site/c/158/1300",
        {"text": text},
    )


def test_is_likely_official_site_partial_address(scraper):
    text = "アクセス\n〒409-2524 山梨県南巨摩郡身延町身延3678 甘養亭製菓店"
    extracted = {
        "addresses": ["〒409-2524 山梨県南巨摩郡身延町身延3678"],
    }
    assert scraper.is_likely_official_site(
        "甘養亭製菓店",
        "https://www.kanyoutei.com/",
        {"text": text},
        "〒409-2524 上町3678",
        extracted,
    )


def test_is_likely_official_site_excludes_note(scraper):
    text = "[公式] 甘養亭製菓店の最新情報"
    assert not scraper.is_likely_official_site(
        "甘養亭製菓店",
        "https://note.com/company_official/n/abc",
        {"text": text},
    )
