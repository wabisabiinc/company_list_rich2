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


def test_alias_row_with_allowed_major_mismatch_is_disabled(tmp_path) -> None:
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "alias",
                "target_minor_code",
                "priority",
                "requires_review",
                "domain_tag",
                "allowed_major_codes",
                "notes",
            ]
        )
        # 441 is major=H (運輸業，郵便業), but allowed_major_codes intentionally mismatches to G.
        w.writerow(["物流", "441", "8", "0", "物流", "G", ""])

    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path))
    assert cls.loaded
    assert len(cls.alias_entries) == 1
    entry = cls.alias_entries[0]
    assert entry.target_minor_code == ""
    assert entry.requires_review is True
    assert "allowed_major_mismatch" in (entry.notes or "")


def test_alias_major_context_guard_suppresses_conflicting_hits(tmp_path) -> None:
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "alias",
                "target_minor_code",
                "priority",
                "requires_review",
                "domain_tag",
                "allowed_major_codes",
                "notes",
            ]
        )
        # AI is allowed only for G, but the context strongly indicates 建設業 (D).
        w.writerow(["AI", "392", "8", "0", "情報通信", "G", ""])

    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path))
    assert cls.loaded

    res = cls.classify_from_aliases("当社の主たる事業は建設業です。", ["AI"])
    assert res is None
