from main import pick_best_address


def test_pick_best_address_prefers_structured_sources_without_expected():
    candidates = [
        "[TEXT]〒520-0867 滋賀県大津市大平1丁目3-20 従業員 14名 一般貨物運送許可",
        "[FOOTER]〒520-0867 滋賀県大津市大平1丁目3-20",
        "[JSONLD]〒520-0867 滋賀県大津市大平1丁目3-20",
    ]
    assert pick_best_address(None, candidates) == "〒520-0867 滋賀県大津市大平1丁目3-20"


def test_pick_best_address_prefers_table_over_footer_when_expected_missing():
    candidates = [
        "[FOOTER]兵庫県姫路市御国野町御着86-6",
        "[LABEL]〒671-2232 兵庫県姫路市御国野町御着86-6",
    ]
    assert pick_best_address(None, candidates) == "〒671-2232 兵庫県姫路市御国野町御着86-6"


def test_pick_best_address_uses_expected_pref_filter_even_for_jsonld():
    candidates = [
        "[JSONLD]〒100-0001 東京都千代田区1-1-1",
        "[TEXT]〒530-0001 大阪府大阪市北区1-1-1",
    ]
    # expected is Osaka -> should not pick Tokyo JSON-LD
    assert pick_best_address("大阪府大阪市北区", candidates) == "〒530-0001 大阪府大阪市北区1-1-1"
