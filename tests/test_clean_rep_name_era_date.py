from src.company_scraper import CompanyScraper


def test_clean_rep_name_rejects_era_date_like_text():
    assert CompanyScraper.clean_rep_name("昭和34年10月") is None
    assert CompanyScraper.clean_rep_name("令和5年") is None
    assert CompanyScraper.clean_rep_name("平成") is None

