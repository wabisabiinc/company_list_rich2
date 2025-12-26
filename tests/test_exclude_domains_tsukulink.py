from src.company_scraper import CompanyScraper


def test_tsukulink_is_excluded():
    assert "tsukulink.net" in CompanyScraper.EXCLUDE_DOMAINS
    assert "tsukulink.net" in CompanyScraper.HARD_EXCLUDE_HOSTS

