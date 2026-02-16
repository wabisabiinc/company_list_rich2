from main import (
    FINAL_DESCRIPTION_MAX_LEN,
    FINAL_DESCRIPTION_MIN_LEN,
    _build_rule_industry_result_from_scores,
    _needs_industry_ai_escalation,
    build_final_description_from_payloads,
    infer_industry_and_business_tags,
)


def test_infer_industry_logistics() -> None:
    industry, tags = infer_industry_and_business_tags(
        ["物流センター運営、倉庫保管、配送・輸送サービスを提供しています。"]
    )
    assert industry == "物流・運送"
    assert any(t in tags for t in ("物流", "配送", "倉庫", "運送"))


def test_infer_industry_unknown_when_no_signals() -> None:
    industry, tags = infer_industry_and_business_tags(["ようこそ。"])
    assert industry == ""
    assert tags == []


def test_build_final_description_from_payloads_length_and_content() -> None:
    payloads = [
        {
            "text": "\n".join(
                [
                    "当社は物流センターの運営や配送サービスを提供し、企業のサプライチェーン最適化を支援しています。",
                    "全国ネットワークで倉庫保管から輸送まで一貫した物流ソリューションを展開し、業務効率化に貢献します。",
                    "お問い合わせはこちら。",
                ]
            ),
            "html": "",
        }
    ]
    desc = build_final_description_from_payloads(payloads)
    assert desc
    assert FINAL_DESCRIPTION_MIN_LEN <= len(desc) <= FINAL_DESCRIPTION_MAX_LEN
    assert "お問い合わせ" not in desc


def test_needs_industry_ai_escalation_when_scores_are_ambiguous() -> None:
    scores = {
        "minor_scores": {"392": 6, "391": 5},
        "alias_requires_review": False,
    }
    assert _needs_industry_ai_escalation(scores, min_score=5, min_margin=2) is True


def test_needs_industry_ai_escalation_false_when_scores_are_clear() -> None:
    scores = {
        "minor_scores": {"392": 9, "391": 2},
        "alias_requires_review": False,
    }
    assert _needs_industry_ai_escalation(scores, min_score=5, min_margin=2) is False


def test_needs_industry_ai_escalation_true_when_semantic_alias_present() -> None:
    scores = {
        "minor_scores": {"392": 9, "391": 2},
        "alias_requires_review": False,
        "alias_matches": [
            {
                "alias": "[semantic]機械知能->AI",
                "target_minor_code": "392",
                "notes": "semantic_match:0.961",
            }
        ],
    }
    assert _needs_industry_ai_escalation(scores, min_score=5, min_margin=2) is True


def test_build_rule_industry_result_from_scores_returns_clear_candidate() -> None:
    scores = {
        "minor_scores": {"392": 9, "391": 2},
        "alias_requires_review": False,
    }
    res = _build_rule_industry_result_from_scores(
        scores,
        source="test_rule_clear",
        min_score=5,
        min_margin=2,
    )
    assert res is not None
    assert res.get("minor_code") == "392"
    assert res.get("source") == "test_rule_clear"
    assert float(res.get("confidence") or 0.0) > 0.7
