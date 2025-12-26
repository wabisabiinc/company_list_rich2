from src.company_scraper import CompanyScraper


def test_find_priority_links_prefers_summary_as_company_profile():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/"
    html = """
    <html><body>
      <nav>
        <a href="/corporate/message/">トップメッセージ</a>
        <a href="/corporate/summary/">会社概要</a>
        <a href="/contact/">お問い合わせ</a>
      </nav>
    </body></html>
    """
    links = scraper._find_priority_links(base, html, max_links=2, target_types=["about"])  # type: ignore[attr-defined]
    assert links
    assert links[0].endswith("/corporate/summary/")

