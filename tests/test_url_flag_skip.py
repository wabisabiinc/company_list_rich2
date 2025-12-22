from main import should_skip_by_url_flag


def test_should_skip_by_url_flag_skips_rule_negative():
    flag = {"is_official": False, "judge_source": "rule", "confidence": 0.1, "reason": "directory_like"}
    assert should_skip_by_url_flag(flag) is True


def test_should_skip_by_url_flag_does_not_skip_low_conf_ai_negative():
    flag = {"is_official": False, "judge_source": "ai", "confidence": 0.2, "reason": "ai_not_official"}
    assert should_skip_by_url_flag(flag) is False


def test_should_skip_by_url_flag_skips_high_conf_ai_negative():
    flag = {"is_official": False, "judge_source": "ai", "confidence": 0.95, "reason": "ai_not_official"}
    assert should_skip_by_url_flag(flag) is True

