from src.company_scraper import CompanyScraper


def test_extract_rep_ignores_company_name_after_role() -> None:
    scraper = CompanyScraper(headless=True)
    text = "会社概要\n代表取締役社長 株式会社宝輪\n所在地 〒513-0836 鈴鹿市国府町5696-1"
    extracted = scraper.extract_candidates(text, html=None, page_type_hint="COMPANY_PROFILE")
    assert extracted.get("rep_names") == []

