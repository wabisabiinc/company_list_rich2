import pytest

try:
    from src.company_scraper import CompanyScraper
except ModuleNotFoundError:
    from company_scraper import CompanyScraper


@pytest.mark.parametrize(
    "raw",
    [
        "受付時間 月",
        "ビジネス文書一覧",
        "ビジネス文書一覧｜株式会社サンプル",
    ],
)
def test_clean_rep_name_rejects_ui_noise(raw: str):
    assert CompanyScraper.clean_rep_name(raw) is None


def test_clean_rep_name_joins_single_kanji_spaced_name():
    assert CompanyScraper.clean_rep_name("喜 納 秀 智") == "喜納秀智"


def test_clean_rep_name_allows_kanji_name_containing_room_character():
    assert CompanyScraper.clean_rep_name("室田 博夫") == "室田 博夫"


def test_clean_rep_name_allows_extended_kanji_name():
    assert CompanyScraper.clean_rep_name("𠮷住 大樹") == "𠮷住 大樹"


def test_clean_rep_name_handles_role_colon_name_with_compat_kanji():
    assert CompanyScraper.clean_rep_name("社⻑：熊⾕ 弘司") == "熊谷 弘司"


def test_clean_rep_name_allows_compact_kanji_name_with_iteration_mark():
    assert CompanyScraper.clean_rep_name("佐々木太郎") == "佐々木太郎"
