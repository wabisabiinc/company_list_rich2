from src.company_scraper import CompanyScraper


def test_classify_contact_by_text_phone() -> None:
    html = """
    <html>
      <head><title>店舗情報</title></head>
      <body>
        <header>
          <nav>お問い合わせ TEL: 03-1234-5678</nav>
        </header>
        <main>
          <h1>店舗情報</h1>
          <p>営業時間: 9:00-18:00</p>
        </main>
      </body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    result = scraper.classify_page_type("https://example.test/store", text="", html=html)
    assert result.get("page_type") == "ACCESS_CONTACT"

    cands = scraper.extract_candidates("", html)
    phones = cands.get("phone_numbers") or []
    assert any(str(p).endswith("03-1234-5678") for p in phones)

