from src.company_scraper import CompanyScraper


def test_rank_links_prioritizes_phone_contact():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/"
    html = """
    <html><body>
      <a href="/company">会社情報</a>
      <a href="/company/overview">会社概要</a>
      <a href="/contact">お問い合わせ</a>
      <a href="/access">アクセス</a>
      <a href="/recruit">採用</a>
    </body></html>
    """
    ranked = scraper._rank_links(base, html, focus={"phone"})
    assert ranked
    assert ranked[0].endswith("/contact")


def test_rank_links_prioritizes_address_access():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/"
    html = """
    <html><body>
      <a href="/company">会社情報</a>
      <a href="/company/overview">会社概要</a>
      <a href="/contact">お問い合わせ</a>
      <a href="/access">アクセス</a>
    </body></html>
    """
    ranked = scraper._rank_links(base, html, focus={"address"})
    assert ranked
    assert ranked[0].endswith("/access")

