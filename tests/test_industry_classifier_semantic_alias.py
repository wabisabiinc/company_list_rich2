import csv

from src.industry_classifier import IndustryClassifier
from src.text_normalizer import norm_text_compact


class FakeSemanticEmbedder:
    def __init__(self, vectors: dict[str, list[float]]) -> None:
        self.vectors = vectors

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        out: list[list[float] | None] = []
        for text in texts:
            key = norm_text_compact(text)
            picked = None
            for pat, vec in self.vectors.items():
                if pat in key:
                    picked = list(vec)
                    break
            out.append(picked)
        return out


def test_semantic_alias_hit_returns_review_result(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("INDUSTRY_ALIAS_SEMANTIC_ENABLED", "true")
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alias", "target_minor_code", "priority", "requires_review", "notes"])
        w.writerow(["AI", "392", "8", "0", "test"])

    embedder = FakeSemanticEmbedder(
        {
            "ai": [1.0, 0.0, 0.0],
            "機械知能": [1.0, 0.0, 0.0],
        }
    )
    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path), semantic_embedder=embedder)
    assert cls.loaded

    res = cls.classify_from_aliases("機械知能を活用した基盤を提供", "")
    assert res is not None
    assert res.get("minor_code") == "392"
    assert str(res.get("source") or "").startswith("alias")
    assert bool(res.get("review_required")) is True
    assert int(res.get("alias_semantic_hits") or 0) >= 1


def test_semantic_alias_margin_guard_suppresses_ambiguous_hit(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("INDUSTRY_ALIAS_SEMANTIC_ENABLED", "true")
    alias_path = tmp_path / "industry_aliases.csv"
    with alias_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alias", "target_minor_code", "priority", "requires_review", "notes"])
        w.writerow(["AI", "392", "8", "0", "test"])
        w.writerow(["DX", "392", "8", "0", "test"])

    embedder = FakeSemanticEmbedder(
        {
            "ai": [1.0, 0.0, 0.0],
            "dx": [1.0, 0.0, 0.0],
            "機械知能": [1.0, 0.0, 0.0],
        }
    )
    cls = IndustryClassifier("docs/industry_select.csv", aliases_csv_path=str(alias_path), semantic_embedder=embedder)
    assert cls.loaded

    res = cls.classify_from_aliases("機械知能", "")
    assert res is None
