import csv

from src.industry_classifier import IndustryClassifier
from src.text_normalizer import norm_text


def test_norm_text_basic() -> None:
    assert norm_text(" ＡＩ／DX (SaaS) ") == "ai dx saas"


def test_alias_classifier_single_source_is_review(tmp_path) -> None:
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alias", "target_minor_code", "priority", "requires_review", "notes"])
        w.writerow(["AI", "392", "8", "0", "test"])

    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path))
    assert cls.loaded

    res = cls.classify_from_aliases("AIを使った分析", "")
    assert res is not None
    assert res.get("minor_code") == "392"
    assert float(res.get("confidence") or 0.0) <= 0.5
    assert bool(res.get("review_required")) is True


def test_alias_classifier_desc_and_tags_relaxes_confidence(tmp_path) -> None:
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alias", "target_minor_code", "priority", "requires_review", "notes"])
        w.writerow(["AI", "392", "8", "0", "test"])
        w.writerow(["SaaS", "392", "8", "0", "test"])

    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path))
    assert cls.loaded

    res = cls.classify_from_aliases("AI活用を推進", ["SaaS"])
    assert res is not None
    assert res.get("minor_code") == "392"
    assert float(res.get("confidence") or 0.0) > 0.5
    assert bool(res.get("review_required")) is False


def test_alias_fallback_is_available_when_csv_missing() -> None:
    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path="missing_aliases.csv")
    assert cls.loaded
    assert cls.alias_source == "fallback"
    assert len(cls.alias_entries) > 0
