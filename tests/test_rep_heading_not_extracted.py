from src.company_scraper import CompanyScraper


def test_rep_heading_is_not_extracted_as_person_name() -> None:
    scraper = CompanyScraper(headless=True)
    text = "代表者あいさつ\n信頼を、ひとのなかへ。\n安心を、まちのそとへ。"
    extracted = scraper.extract_candidates(text, html=None, page_type_hint="OTHER")
    assert extracted.get("rep_names") == []

