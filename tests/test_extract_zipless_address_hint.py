from src.company_scraper import CompanyScraper


def test_extract_zipless_address_with_profile_hint() -> None:
    text = "会社概要 所在地 東京都渋谷区神宮前1-2-3 代表取締役 山田太郎"
    scraper = CompanyScraper(headless=True)
    cands = scraper.extract_candidates(text, html=None, page_type_hint="COMPANY_PROFILE")
    addrs = cands.get("addresses") or []
    assert any("東京都渋谷区" in str(a) and "1-2-3" in str(a) for a in addrs)

