from src.company_scraper import CompanyScraper


def test_classify_profile_by_table_labels() -> None:
    html = """
    <html>
      <head><title>三慶交通株式会社本社営業所</title></head>
      <body>
        <table>
          <tr><th>代表者</th><td>岡田 彰</td></tr>
          <tr><th>所在地</th><td>〒221-0863 横浜市神奈川区羽沢町55</td></tr>
          <tr><th>電話番号</th><td>045-383-1611</td></tr>
          <tr><th>FAX番号</th><td>045-383-1614</td></tr>
        </table>
      </body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    result = scraper.classify_page_type(
        "https://example.test/taxi/%E4%B8%89%E6%85%B6%E4%BA%A4%E9%80%9A/",
        text="",
        html=html,
    )
    assert result.get("page_type") == "COMPANY_PROFILE"

    cands = scraper.extract_candidates("", html)
    phones = cands.get("phone_numbers") or []
    assert any(str(p).endswith("045-383-1611") for p in phones)
