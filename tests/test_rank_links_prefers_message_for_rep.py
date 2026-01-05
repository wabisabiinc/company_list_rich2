from src.company_scraper import CompanyScraper


def test_rank_links_prefers_message_page_when_rep_needed():
    scraper = CompanyScraper(headless=True)
    base = "https://example.co.jp/company/"
    html = """
    <html><body>
      <nav>
        <a href="/company/">会社情報</a>
        <a href="/contact/">お問い合わせ</a>
        <a href="/message/">ごあいさつ</a>
      </nav>
    </body></html>
    """
    ranked = scraper._rank_links(base, html, focus={"phone", "rep"})  # type: ignore[attr-defined]
    assert ranked
    # rep は誤爆しやすいので、まずは会社概要（テーブル/ラベル）系を優先し、message はフォールバックに回す
    assert ranked.index("https://example.co.jp/company/") < ranked.index("https://example.co.jp/message/")
