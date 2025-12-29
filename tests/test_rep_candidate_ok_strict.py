from main import _rep_candidate_ok


def test_rep_candidate_ok_rejects_contact_like_url():
    ok, reason = _rep_candidate_ok(
        "田中太郎",
        ["[TABLE]田中太郎"],
        "ACCESS_CONTACT",
        "https://example.co.jp/contact/",
    )
    assert ok is False
    assert reason in {"contact_like_url", "contact_forbidden"}


def test_rep_candidate_ok_allows_greeting_like_url_even_if_other():
    ok, reason = _rep_candidate_ok(
        "田中太郎",
        ["[LABEL]田中太郎"],
        "OTHER",
        "https://example.co.jp/message/",
    )
    assert ok is True
    assert reason == ""


def test_rep_candidate_ok_rejects_other_even_if_strong_source_when_not_profile_like():
    ok, reason = _rep_candidate_ok(
        "キーワード",
        ["[LABEL]キーワード"],
        "OTHER",
        "https://houjin.example.com/search",
    )
    assert ok is False
    assert reason == "other_not_profile"
