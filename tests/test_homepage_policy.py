from src.homepage_policy import apply_provisional_homepage_policy


def test_apply_provisional_homepage_policy_drops_weak_provisional():
    decision = apply_provisional_homepage_policy(
        homepage="https://example.com/",
        homepage_official_flag=0,
        homepage_official_source="provisional_freehost",
        homepage_official_score=0.0,
        chosen_domain_score=3,
        provisional_host_token=False,
        provisional_name_present=False,
        provisional_address_ok=False,
    )
    assert decision.dropped is True
    assert decision.homepage == ""
    assert decision.homepage_official_source == ""
    assert decision.chosen_domain_score == 0


def test_apply_provisional_homepage_policy_keeps_strong_provisional_by_domain_score():
    decision = apply_provisional_homepage_policy(
        homepage="https://example.com/",
        homepage_official_flag=0,
        homepage_official_source="provisional",
        homepage_official_score=0.0,
        chosen_domain_score=4,
        provisional_host_token=False,
        provisional_name_present=False,
        provisional_address_ok=False,
    )
    assert decision.dropped is False
    assert decision.homepage == "https://example.com/"
    assert decision.homepage_official_source == "provisional"
    assert decision.chosen_domain_score == 4


def test_apply_provisional_homepage_policy_ignores_non_provisional_sources():
    decision = apply_provisional_homepage_policy(
        homepage="https://example.com/",
        homepage_official_flag=0,
        homepage_official_source="ai_review",
        homepage_official_score=0.0,
        chosen_domain_score=1,
        provisional_host_token=False,
        provisional_name_present=False,
        provisional_address_ok=False,
    )
    assert decision.dropped is False
    assert decision.homepage == "https://example.com/"


def test_apply_provisional_homepage_policy_applies_to_ai_provisional():
    decision = apply_provisional_homepage_policy(
        homepage="https://example.com/",
        homepage_official_flag=0,
        homepage_official_source="ai_provisional",
        homepage_official_score=0.0,
        chosen_domain_score=1,
        provisional_host_token=False,
        provisional_name_present=False,
        provisional_address_ok=False,
    )
    assert decision.dropped is True

