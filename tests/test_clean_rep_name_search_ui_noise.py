from src.company_scraper import CompanyScraper


def test_clean_rep_name_rejects_search_ui_noise():
    assert CompanyScraper.clean_rep_name("キーワード: 検索") is None
    assert CompanyScraper.clean_rep_name("キーワード：検索") is None
    assert CompanyScraper.clean_rep_name("検索") is None


def test_clean_rep_name_rejects_government_office_names():
    assert CompanyScraper.clean_rep_name("千葉県庁") is None
    assert CompanyScraper.clean_rep_name("東京都庁") is None
    assert CompanyScraper.clean_rep_name("渋谷区役所") is None


def test_clean_rep_name_strips_short_title_suffix():
    assert CompanyScraper.clean_rep_name("黒滝 寛 常務") == "黒滝 寛"

