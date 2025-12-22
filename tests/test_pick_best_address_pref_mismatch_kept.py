from main import pick_best_address


def test_pick_best_address_keeps_candidate_even_when_pref_mismatch():
    candidates = [
        "[JSONLD]〒100-0001 東京都千代田区千代田1-1-1",
    ]
    # expected is Osaka but only Tokyo candidate exists -> should still return a candidate (保存してreview判断へ渡す)
    assert pick_best_address("大阪府大阪市北区", candidates) == "〒100-0001 東京都千代田区千代田1-1-1"

