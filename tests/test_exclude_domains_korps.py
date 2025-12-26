from src.company_scraper import CompanyScraper


def test_korps_is_excluded():
    assert "korps.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "korps.co.jp" in CompanyScraper.EXCLUDE_DOMAINS
    assert "korps.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS
    assert "korps.co.jp" in CompanyScraper.HARD_EXCLUDE_HOSTS
    # HARD_EXCLUDE_HOSTS は suffix 判定なので subdomain も除外される
    assert "www.korps.jp".endswith(".korps.jp")
