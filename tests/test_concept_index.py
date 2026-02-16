from __future__ import annotations

from src.concept_index import ConceptIndex


class FakeEmbeddingsProvider:
    def __init__(self) -> None:
        self.model_name = "fake:test"

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        out: list[list[float] | None] = []
        for text in texts:
            t = str(text or "")
            if any(k in t for k in ("AI", "ＡＩ", "生成AI", "機械学習", "機械知能", "LLM", "ICT", "ソフトウェア", "情報通信", "AI開発")):
                out.append([1.0, 0.0, 0.0, 0.0])
            elif any(k in t for k in ("自動化", "RPA", "省人化", "自動制御", "自動運転", "業務効率化")):
                out.append([0.0, 1.0, 0.0, 0.0])
            elif any(k in t for k in ("コンサル", "戦略", "業務改善", "調査", "助言")):
                out.append([0.0, 0.0, 1.0, 0.0])
            elif any(k in t for k in ("EC", "通販", "オンラインショップ", "eコマース", "D2C")):
                out.append([0.0, 0.0, 0.0, 1.0])
            elif any(k in t for k in ("製造", "加工", "工場", "OEM", "ODM")):
                out.append([0.0, 0.8, 0.0, 0.2])
            elif any(k in t for k in ("建設", "施工", "土木", "設備工事", "リフォーム")):
                out.append([0.0, 0.6, 0.0, 0.4])
            else:
                out.append([0.25, 0.25, 0.25, 0.25])
        return out


def _new_index(tmp_path) -> ConceptIndex:
    return ConceptIndex(
        concepts_path="data/concepts.json",
        vectors_cache_path=str(tmp_path / "concept_vectors_cache.json"),
        embeddings_provider=FakeEmbeddingsProvider(),
        sim_threshold=0.82,
        margin_threshold=0.05,
        topk=10,
    )


def test_ai_related_tags_map_to_concept_ai_ict(tmp_path) -> None:
    index = _new_index(tmp_path)
    for tag in ["AI", "ＡＩ", "生成AI", "機械知能", "AI開発"]:
        result = index.normalize_tag_to_concept(tag)
        top1 = (result.get("top1") or {}).get("id")
        topk_ids = [str(v.get("id") or "") for v in (result.get("topk") or []) if isinstance(v, dict)]
        assert (top1 == "concept_ai_ict") or ("concept_ai_ict" in topk_ids)


def test_automation_tag_is_near_automation_or_hold(tmp_path) -> None:
    index = _new_index(tmp_path)
    result = index.normalize_tag_to_concept("自動化システム")
    top1 = (result.get("top1") or {}).get("id")
    decision = str(result.get("decision") or "")
    assert top1 == "concept_automation" or decision == "hold"
