from main import ai_official_hint_from_judge


def test_ai_official_hint_from_judge_true_for_high_confidence():
    assert ai_official_hint_from_judge({"is_official": True, "confidence": 0.9}, 0.65) is True


def test_ai_official_hint_from_judge_false_for_low_confidence_or_non_official():
    assert ai_official_hint_from_judge({"is_official": True, "confidence": 0.5}, 0.65) is False
    assert ai_official_hint_from_judge({"is_official": False, "confidence": 0.9}, 0.65) is False

