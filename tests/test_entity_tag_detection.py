from src.company_scraper import CompanyScraper


def test_detect_entity_tags_does_not_misclassify_railway_as_gov():
    # 「道」が gov 判定に入ると「鉄道」でも誤って gov 扱いになるため回帰防止
    assert "gov" not in CompanyScraper._detect_entity_tags("近江鉄道株式会社")

def test_detect_entity_tags_does_not_misclassify_private_company_with_city_char():
    # 「市」が社名の一部（市場など）でも gov 扱いにしない
    assert "gov" not in CompanyScraper._detect_entity_tags("株式会社市場開発")


def test_detect_entity_tags_detects_prefecture_office_as_gov():
    assert "gov" in CompanyScraper._detect_entity_tags("滋賀県庁")


def test_detect_entity_tags_detects_hokkaido_as_gov():
    assert "gov" in CompanyScraper._detect_entity_tags("北海道")
