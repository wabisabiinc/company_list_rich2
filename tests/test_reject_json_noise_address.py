from src.company_scraper import CompanyScraper


def test_reject_embedded_json_noise_as_address() -> None:
    # Wix等の埋め込みJSONに postalCode が含まれるが、住所として扱うと誤爆する
    text = 'businessPostalCode\":\"633-2164\",\"businessLocationCity\":\"Uda\"'
    scraper = CompanyScraper(headless=True)
    cands = scraper.extract_candidates(text, html=None, page_type_hint="OTHER")
    assert not (cands.get("addresses") or [])

    # 〒付きでもJSON断片は住所採用しない
    text2 = '〒633-2164 \"businessPostalCode\":\"633-2164\",\"businessLocationCity\":\"Uda\"'
    cands2 = scraper.extract_candidates(text2, html=None, page_type_hint="OTHER")
    assert not (cands2.get("addresses") or [])

