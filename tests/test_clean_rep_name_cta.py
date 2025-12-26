from src.company_scraper import CompanyScraper


def test_clean_rep_name_rejects_cta_phrase():
    assert CompanyScraper.clean_rep_name("代表ごあいさつはこちらへ") is None
    assert CompanyScraper.clean_rep_name("代表ご挨拶はこちら") is None
    assert CompanyScraper.clean_rep_name("代表メッセージはこちら") is None


def test_clean_rep_name_allows_kana_person_name():
    assert CompanyScraper.clean_rep_name("やまだ たろう") == "やまだ たろう"
    assert CompanyScraper.clean_rep_name("やまだ・たろう") == "やまだ・たろう"

