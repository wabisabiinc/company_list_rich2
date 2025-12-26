from src.company_scraper import CompanyScraper


def test_find_priority_links_detects_nested_corporate_profile_path():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/"
    html = """
    <html><body>
      <header>
        <a href="/corporate/profile/">会社情報</a>
        <a href="/contact/">お問い合わせ</a>
      </header>
    </body></html>
    """
    links = scraper._find_priority_links(base, html, max_links=2, target_types=["about"])  # type: ignore[attr-defined]
    assert links
    assert links[0].endswith("/corporate/profile/")

