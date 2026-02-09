from src.industry_classifier import IndustryClassifier


def test_exact_lookup_resolves_detail_or_minor() -> None:
    cls = IndustryClassifier("docs/industry_select.csv")
    assert cls.loaded
    res = cls.resolve_exact_candidate_from_name("デザイン業")
    assert res is not None
    assert res.get("major_code") == "L"
    assert res.get("middle_code") == "72"
    assert res.get("minor_name") == "デザイン業"


def test_exact_lookup_rejects_non_taxonomy_label() -> None:
    cls = IndustryClassifier("docs/industry_select.csv")
    assert cls.loaded
    assert cls.resolve_exact_candidate_from_name("IT・ソフトウェア") is None
