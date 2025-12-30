import importlib
import os


def test_rep_candidate_requires_structured_source(monkeypatch) -> None:
    # Ensure env is applied at import time.
    monkeypatch.setenv("REP_REQUIRE_STRUCTURED_SOURCE", "true")
    import main as _main
    importlib.reload(_main)

    ok, reason = _main._rep_candidate_ok(
        chosen="山田太郎",
        candidates=["山田太郎"],  # no [TABLE]/[LABEL]/[ROLE]/[JSONLD] tags
        page_type="COMPANY_PROFILE",
        source_url="https://example.com/company",
    )
    assert ok is False
    assert reason == "not_structured_source"


def test_rep_candidate_allows_structured_source(monkeypatch) -> None:
    monkeypatch.setenv("REP_REQUIRE_STRUCTURED_SOURCE", "true")
    import main as _main
    importlib.reload(_main)

    ok, reason = _main._rep_candidate_ok(
        chosen="山田太郎",
        candidates=["[LABEL]山田太郎"],
        page_type="COMPANY_PROFILE",
        source_url="https://example.com/company",
    )
    assert ok is True
    assert reason == ""
