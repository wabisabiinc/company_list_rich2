from main import pick_best_phone
from src.company_scraper import CompanyScraper


def test_extract_candidates_table_tel_fax_prefers_tel_and_extracts_address():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><body>
      <table>
        <tr><th>本社</th><td>〒100-0001 東京都千代田区千代田1-1</td></tr>
        <tr><th>TEL/FAX</th><td>TEL:03-1234-5678 FAX:03-1111-2222</td></tr>
        <tr><th>代表取締役</th><td>代表取締役社長 山田 太郎</td></tr>
      </table>
    </body></html>
    """
    cc = scraper.extract_candidates(text="", html=html)
    assert pick_best_phone(cc.get("phone_numbers") or []) == "03-1234-5678"
    assert any("東京都" in a for a in (cc.get("addresses") or []))
    assert cc.get("rep_names")


def test_extract_candidates_footer_tel_and_tel_link():
    scraper = CompanyScraper(headless=True)
    html = """
    <html><body>
      <main>
        <p>本文は電話なし</p>
      </main>
      <footer>
        <dl>
          <dd class="f_tel pc">TEL:03-6667-5800</dd>
          <dd class="f_tel sp"><a href="tel:03-6667-5800">TEL:03-6667-5800</a></dd>
        </dl>
      </footer>
    </body></html>
    """
    cc = scraper.extract_candidates(text="", html=html)
    assert pick_best_phone(cc.get("phone_numbers") or []) == "03-6667-5800"
