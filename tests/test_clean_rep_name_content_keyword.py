from src.company_scraper import CompanyScraper


def test_clean_rep_name_rejects_content_keyword_words() -> None:
    assert CompanyScraper.clean_rep_name("コンテンツ") is None
    assert CompanyScraper.clean_rep_name("コンテンツ キーワード") is None
    assert CompanyScraper.clean_rep_name("キーワード") is None


def test_clean_rep_name_rejects_katakana_only_without_middle_dot() -> None:
    assert CompanyScraper.clean_rep_name("ヤマダ タロウ") is None


def test_clean_rep_name_allows_foreign_name_with_middle_dot() -> None:
    assert CompanyScraper.clean_rep_name("ジョン・スミス") == "ジョン・スミス"
