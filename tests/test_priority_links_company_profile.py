from src.company_scraper import CompanyScraper


def test_find_priority_links_prefers_company_profile_over_contact_when_about_enabled():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/message"
    html = """
    <html><body>
      <nav>
        <a href="/contact">お問い合わせ</a>
        <a href="/company02/outline.html">会社概要</a>
      </nav>
      <main>
        <h1>ご挨拶</h1>
        <p>理念ページです。</p>
      </main>
    </body></html>
    """
    links = scraper._find_priority_links(base, html, max_links=2, target_types=["contact", "about"])  # type: ignore[attr-defined]
    assert links
    assert links[0].endswith("/company02/outline.html")

