from src.company_scraper import CompanyScraper


def test_korps_is_excluded():
    assert "korps.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "korps.co.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "korps.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS
    assert "korps.co.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS

