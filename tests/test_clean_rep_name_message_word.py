from src.company_scraper import CompanyScraper


def test_clean_rep_name_rejects_message_word():
    assert CompanyScraper.clean_rep_name("トップメッセージ") is None
    assert CompanyScraper.clean_rep_name("TOP MESSAGE") is None
    assert CompanyScraper.clean_rep_name("トップﾒｯｾｰｼﾞ") is None

