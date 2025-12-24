from src.company_scraper import CompanyScraper


def test_classify_contact_by_text_address_only() -> None:
    html = """
    <html>
      <head><title>店舗情報</title></head>
      <body>
        <header>
          <div>お問い合わせ</div>
          <div>〒150-0001 東京都渋谷区神宮前1-2-3</div>
        </header>
        <main>
          <h1>店舗情報</h1>
        </main>
      </body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    result = scraper.classify_page_type("https://example.test/store", text="", html=html)
    assert result.get("page_type") == "ACCESS_CONTACT"

    cands = scraper.extract_candidates("お問い合わせ 〒150-0001 東京都渋谷区神宮前1-2-3", html)
    addrs = cands.get("addresses") or []
    assert any("〒150-0001" in str(a) and "東京都渋谷区" in str(a) for a in addrs)

