from main import is_over_deep_limit


def test_over_deep_limit_is_relative_to_official_phase_end():
    assert is_over_deep_limit(60.0, "https://example.com", official_phase_end=55.0, time_limit_deep=45.0) is False
    assert is_over_deep_limit(110.1, "https://example.com", official_phase_end=55.0, time_limit_deep=45.0) is True


def test_over_deep_limit_falls_back_to_total_when_no_official_phase_end():
    assert is_over_deep_limit(60.0, "https://example.com", official_phase_end=0.0, time_limit_deep=45.0) is True

