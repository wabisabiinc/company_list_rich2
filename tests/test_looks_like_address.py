from main import looks_like_address as looks_like_address_main
from src.company_scraper import CompanyScraper


def test_looks_like_address_accepts_hyphenated_block_number_without_chome():
    # 例: 「本社所在地 京都市下京区和気町21-1」のような表記
    addr = "京都市下京区和気町21-1"
    assert looks_like_address_main(addr) is True
    assert CompanyScraper.looks_like_address(addr) is True


def test_looks_like_address_rejects_non_address_text():
    assert looks_like_address_main("会社概要") is False
    assert CompanyScraper.looks_like_address("会社概要") is False

