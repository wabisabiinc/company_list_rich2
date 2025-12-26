from src.company_scraper import CompanyScraper


def test_extract_tel_href_tagged() -> None:
    html = """
    <html>
      <body>
        <a href="tel:0312345678">TEL</a>
      </body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    cands = scraper.extract_candidates("", html)
    phones = cands.get("phone_numbers") or []
    assert any(str(p).startswith("[TELHREF]") and str(p).endswith("03-1234-5678") for p in phones)


def test_extract_jsonld_phone_tagged() -> None:
    html = """
    <html>
      <head>
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "Organization",
          "name": "Example",
          "telephone": "03-1234-5678",
          "address": {
            "@type": "PostalAddress",
            "addressRegion": "東京都",
            "addressLocality": "渋谷区",
            "streetAddress": "神宮前1-2-3",
            "postalCode": "150-0001"
          }
        }
        </script>
      </head>
      <body>Example</body>
    </html>
    """
    scraper = CompanyScraper(headless=True)
    cands = scraper.extract_candidates("", html)
    phones = cands.get("phone_numbers") or []
    assert any(str(p).startswith("[JSONLD]") and str(p).endswith("03-1234-5678") for p in phones)

