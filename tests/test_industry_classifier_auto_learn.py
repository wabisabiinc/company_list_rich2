import csv

import src.industry_classifier as icmod
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


def test_semantic_taxonomy_classifies_unknown_term(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "industry_memory.csv"
    monkeypatch.setattr(icmod, "DEFAULT_INDUSTRY_MEMORY_CSV_PATH", str(memory_path))
    embedder = FakeSemanticEmbedder(
        {
            "情報処理": [1.0, 0.0, 0.0],
            "一般貨物自動車運送業": [0.0, 1.0, 0.0],
            "機械知能": [1.0, 0.0, 0.0],
        }
    )
    cls = IndustryClassifier("docs/industry_select.csv", semantic_embedder=embedder)
    assert cls.loaded
    cls.semantic_taxonomy_enabled = True
    cls.semantic_taxonomy_min_sim = 0.90
    cls.semantic_taxonomy_min_margin = 0.05
    cls._build_semantic_taxonomy_index()

    res = cls.classify_from_semantic_taxonomy("機械知能を活用した基盤を提供", [])
    assert res is not None
    assert res.get("minor_code") == "392"
    assert str(res.get("source") or "").startswith("semantic_taxonomy")


def test_auto_learn_from_result_updates_memory_and_alias_hits(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "industry_memory.csv"
    monkeypatch.setattr(icmod, "DEFAULT_INDUSTRY_MEMORY_CSV_PATH", str(memory_path))
    cls = IndustryClassifier("docs/industry_select.csv")
    assert cls.loaded
    cls.memory_csv_path = str(memory_path)
    cls.auto_learn_min_confidence = 0.60
    cls.auto_learn_min_count = 1
    cls.auto_learn_max_terms = 5

    learned = cls.auto_learn_from_result(
        description="機械知能を活用した分析基盤を提供",
        business_tags=["機械知能", "予測分析"],
        result={
            "minor_code": "392",
            "confidence": 0.92,
            "source": "ai_final_homepage",
            "review_required": False,
        },
        company_name="テスト株式会社",
    )
    assert learned > 0
    assert memory_path.exists()

    with memory_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert any((r.get("target_minor_code") or "") == "392" for r in rows)

    res = cls.classify_from_aliases("機械知能の導入支援", [])
    assert res is not None
    assert res.get("minor_code") == "392"
    assert "機械知能" in "".join(str(v) for v in (res.get("alias_matches") or []))


def test_auto_learn_skips_low_confidence_result(tmp_path, monkeypatch) -> None:
    memory_path = tmp_path / "industry_memory.csv"
    monkeypatch.setattr(icmod, "DEFAULT_INDUSTRY_MEMORY_CSV_PATH", str(memory_path))
    cls = IndustryClassifier("docs/industry_select.csv")
    assert cls.loaded
    cls.memory_csv_path = str(memory_path)
    cls.auto_learn_min_confidence = 0.75

    learned = cls.auto_learn_from_result(
        description="機械知能を活用した分析基盤を提供",
        business_tags=["機械知能"],
        result={
            "minor_code": "392",
            "confidence": 0.40,
            "source": "ai_final_homepage",
            "review_required": False,
        },
        company_name="テスト株式会社",
    )
    assert learned == 0
    assert not memory_path.exists()
