from main import _rep_candidate_ok


def test_rep_candidate_ok_rejects_greeting_without_paired_tags():
    ok, reason = _rep_candidate_ok(
        "田中太郎",
        ["田中太郎"],  # no tags
        "OTHER",
        "https://example.co.jp/message/",
    )
    assert ok is False
    assert reason == "greeting_not_paired"

