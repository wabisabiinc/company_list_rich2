from src.company_scraper import CompanyScraper


def test_directory_pattern_detects_corporations_numeric_path():
    hint = CompanyScraper._detect_directory_like(  # type: ignore[attr-defined]
        "https://korps.jp/corporations/4083388",
        text="企業データベース 法人番号 1234567890123",
        html="",
    )
    assert hint.get("is_directory_like") is True

