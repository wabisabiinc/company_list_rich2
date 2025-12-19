from src.company_scraper import CompanyScraper
from src.site_validator import normalize_company_name, score_name_match


def test_normalize_company_name_strips_corp_suffix():
    assert normalize_company_name("株式会社テスト") == "テスト"
    assert normalize_company_name("（株）テスト") == "テスト"


def test_score_name_match_exact_and_partial():
    signals = {"title": "HONMA", "h1": "HONMA"}
    r = score_name_match("HONMA", signals)
    assert r.exact is True
    assert r.ratio == 1.0

    signals2 = {"title": "HONMA GOLF 公式サイト"}
    r2 = score_name_match("HONMA", signals2)
    assert r2.partial_only is True
    assert r2.ratio <= 0.69


def test_company_scraper_rejects_partial_domain_only():
    scraper = CompanyScraper(headless=True)
    page_info = {
        "text": "HONMA GOLF 公式サイト",
        "html": "<html><head><title>HONMA GOLF 公式サイト</title></head><body>HONMA GOLF</body></html>",
    }
    # ドメイン側が強く一致しても、タイトル/h1等に社名の完全一致が無い場合は除外する
    assert scraper.is_likely_official_site("株式会社ホンマ", "https://www.honmagolf.co.jp/", page_info) is False


def test_company_scraper_accepts_when_name_signal_strong():
    scraper = CompanyScraper(headless=True)
    page_info = {
        "text": "会社概要 株式会社ホンマ",
        "html": "<html><head><title>株式会社ホンマ | 会社概要</title></head><body><h1>株式会社ホンマ</h1></body></html>",
    }
    assert scraper.is_likely_official_site("株式会社ホンマ", "https://www.honma.co.jp/company", page_info) is True
