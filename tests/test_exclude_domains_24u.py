from src.company_scraper import CompanyScraper


def test_24u_is_excluded():
    assert "24u.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "24u.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS
    assert "www.24u.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "www.24u.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS
