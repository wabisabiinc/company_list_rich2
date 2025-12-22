from main import clean_description_value


def test_clean_description_rejects_directory_site_blurb():
    raw = "【ドラエバーしようぜ！】運送・物流企業の総合データベースサイト、ドラマッチ、登録企業詳細ページです。"
    assert clean_description_value(raw) == ""


def test_clean_description_picks_first_business_sentence_when_trailing_noise():
    raw = "配送・運送事業を中心にサービスを提供しています。採用情報はこちら。"
    assert clean_description_value(raw) == "配送・運送事業を中心にサービスを提供しています"
