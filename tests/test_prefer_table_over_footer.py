from main import pick_best_address, pick_best_phone


def test_pick_best_phone_ignores_footer_when_table_exists():
    # TABLE があるなら footer 由来のノイズを採用しない
    candidates = [
        "[FOOTER]03-1111-2222",
        "[TABLE]03-3333-4444",
    ]
    assert pick_best_phone(candidates) == "03-3333-4444"


def test_pick_best_address_ignores_footer_when_table_exists():
    candidates = [
        "[FOOTER]〒100-0001 東京都千代田区千代田1-1",
        "[TABLE][HQ]〒530-0001 大阪府大阪市北区梅田1-1-1",
    ]
    assert pick_best_address(None, candidates) == "〒530-0001 大阪府大阪市北区梅田1-1-1"

