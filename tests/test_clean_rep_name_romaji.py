from src.company_scraper import CompanyScraper


def test_clean_rep_name_strips_romaji_tail_when_japanese_present() -> None:
    # 日本語名 + ローマ字表記が並記されるケース
    raw = "代表取締役社長 蕪竹 理江 Rie Kabutake"
    assert CompanyScraper.clean_rep_name(raw) == "蕪竹 理江"

