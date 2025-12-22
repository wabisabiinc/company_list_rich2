from src.company_scraper import CompanyScraper


def test_classify_page_type_company_profile_with_table_labels():
    scraper = CompanyScraper(headless=True)
    html = """
    <html>
      <head><title>会社概要 | テスト株式会社</title></head>
      <body>
        <h1>会社概要</h1>
        <table>
          <tr><th>本社所在地</th><td>〒100-0001 東京都千代田区千代田1-1</td></tr>
          <tr><th>TEL</th><td>03-1234-5678</td></tr>
          <tr><th>代表取締役</th><td>代表取締役社長 山田 太郎</td></tr>
          <tr><th>設立</th><td>2001年</td></tr>
          <tr><th>資本金</th><td>1,000万円</td></tr>
          <tr><th>事業内容</th><td>製造・販売</td></tr>
        </table>
      </body>
    </html>
    """
    res = scraper.classify_page_type("https://example.co.jp/company02/outline.html", text="", html=html)
    assert res.get("page_type") == "COMPANY_PROFILE"


def test_classify_page_type_bases_list():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><head><title>拠点一覧</title></head><body>
      <h1>拠点一覧</h1>
      <ul>
        <li>東京営業所 〒100-0001 東京都千代田区千代田1-1</li>
        <li>大阪営業所 〒530-0001 大阪府大阪市北区1-1-1</li>
        <li>名古屋営業所 〒450-0002 愛知県名古屋市中村区2-2-2</li>
      </ul>
    </body></html>
    """
    res = scraper.classify_page_type("https://example.co.jp/bases", text="", html=html)
    assert res.get("page_type") == "BASES_LIST"


def test_classify_page_type_bases_list_with_hq_profile_table_is_profile():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><head><title>拠点一覧 | テスト株式会社</title></head><body>
      <h1>拠点一覧</h1>
      <table>
        <tr><th>本社所在地</th><td>〒100-0001 東京都千代田区千代田1-1</td></tr>
        <tr><th>TEL</th><td>03-1234-5678</td></tr>
        <tr><th>代表取締役</th><td>代表取締役社長 山田 太郎</td></tr>
        <tr><th>事業内容</th><td>製造・販売</td></tr>
      </table>
      <ul>
        <li>東京営業所 〒100-0001 東京都千代田区千代田1-1</li>
        <li>大阪営業所 〒530-0001 大阪府大阪市北区1-1-1</li>
        <li>名古屋営業所 〒450-0002 愛知県名古屋市中村区2-2-2</li>
      </ul>
    </body></html>
    """
    res = scraper.classify_page_type("https://example.co.jp/company/bases", text="", html=html)
    assert res.get("page_type") == "COMPANY_PROFILE"


def test_rank_links_prefers_company_profile_link_even_with_odd_path():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><body>
      <nav>
        <a href="/top">トップ</a>
        <a href="/company02/outline.html">会社概要</a>
        <a href="/contact">お問い合わせ</a>
      </nav>
      <main>
        <h1>ご挨拶</h1>
        <p>理念ページです。</p>
      </main>
    </body></html>
    """
    ranked = scraper._rank_links("https://example.co.jp/message", html, focus={"profile"})  # type: ignore[attr-defined]
    assert ranked
    assert ranked[0].endswith("/company02/outline.html")
