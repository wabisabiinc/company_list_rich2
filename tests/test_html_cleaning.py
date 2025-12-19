from src.company_scraper import CompanyScraper


def test_clean_text_from_html_removes_nav_and_cookie():
    html = """
    <html>
      <head><title>テスト</title></head>
      <body>
        <nav>ホーム / 会社概要 / お問い合わせ</nav>
        <div id="cookie-banner">当サイトはCookieを使用します。プライバシーポリシー</div>
        <main>
          <p>〒113-0033 東京都文京区本郷3-35</p>
          <p>TEL: 03-1111-2222</p>
        </main>
        <footer>Cookie Policy</footer>
      </body>
    </html>
    """
    text = CompanyScraper._clean_text_from_html(html)
    assert "〒113-0033 東京都文京区本郷3-35" in text
    assert "03-1111-2222" in text
    assert "Cookie" not in text
    assert "プライバシー" not in text
    assert "会社概要" not in text  # navだけの短行は除外される

