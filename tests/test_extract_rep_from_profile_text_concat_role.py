from src.company_scraper import CompanyScraper


def test_extract_candidates_rep_finds_concat_role_in_profile_text() -> None:
    scraper = CompanyScraper(headless=True)
    text = "会社概要\n代表取締役社長 田中太郎\n所在地 東京都千代田区1-1-1"
    cands = scraper.extract_candidates(text=text, html="", page_type_hint="COMPANY_PROFILE")
    reps = cands.get("rep_names") or []
    assert any("田中太郎" in r for r in reps)

