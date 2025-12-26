from src.company_scraper import CompanyScraper


def test_profile_not_misclassified_as_contact_when_rep_present() -> None:
    html = """
    <html>
      <head><title>会社概要</title></head>
      <body>
        <header>お問い合わせ</header>
        <main>
          <h1>会社概要</h1>
          <p>代表者: 山田 太郎</p>
          <p>所在地: 〒150-0001 東京都渋谷区神宮前1-2-3</p>
          <p>TEL: 03-1234-5678</p>
        </main>
      </body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    result = scraper.classify_page_type("https://example.test/about", text="", html=html)
    assert result.get("page_type") == "COMPANY_PROFILE"

