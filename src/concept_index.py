import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from src.embeddings_provider import EmbeddingsProvider
from src.text_normalizer import norm_text_compact


log = logging.getLogger(__name__)

DEFAULT_CONCEPTS_PATH = (os.getenv("CONCEPTS_JSON_PATH") or "data/concepts.json").strip()
DEFAULT_CONCEPT_VECTORS_CACHE_PATH = (
    os.getenv("CONCEPT_VECTORS_CACHE_PATH") or "data/concept_vectors_cache.json"
).strip()
DEFAULT_SIM_THRESHOLD = float(os.getenv("CONCEPT_SIM_THRESHOLD", "0.82"))
DEFAULT_MARGIN_THRESHOLD = float(os.getenv("CONCEPT_MARGIN_THRESHOLD", "0.05"))
DEFAULT_TOPK = max(1, int(os.getenv("CONCEPT_TOPK", "10")))


def _cosine_similarity(v1: list[float] | None, v2: list[float] | None) -> float:
    if not v1 or not v2:
        return -1.0
    if len(v1) != len(v2):
        return -1.0
    return float(sum(a * b for a, b in zip(v1, v2)))


def _normalize_vector(vec: list[float] | tuple[float, ...] | None) -> list[float] | None:
    if not vec:
        return None
    total = 0.0
    out: list[float] = []
    for value in vec:
        try:
            fval = float(value)
        except Exception:
            continue
        out.append(fval)
        total += fval * fval
    if not out or total <= 0:
        return None
    inv = total ** -0.5
    return [v * inv for v in out]


def _average_vectors(vectors: list[list[float] | None]) -> list[float] | None:
    valid = [v for v in vectors if v]
    if not valid:
        return None
    dim = len(valid[0])
    if dim <= 0:
        return None
    acc = [0.0] * dim
    used = 0
    for vec in valid:
        if len(vec) != dim:
            continue
        for idx, value in enumerate(vec):
            acc[idx] += float(value)
        used += 1
    if used <= 0:
        return None
    avg = [v / float(used) for v in acc]
    return _normalize_vector(avg)


@dataclass
class ConceptDefinition:
    concept_id: str
    label: str
    aliases: list[str]
    anchor_texts: list[str]
    industry_hints: list[str]


class ConceptIndex:
    def __init__(
        self,
        *,
        concepts_path: str | None = None,
        vectors_cache_path: str | None = None,
        sim_threshold: float | None = None,
        margin_threshold: float | None = None,
        topk: int | None = None,
        embeddings_provider: Optional[EmbeddingsProvider] = None,
    ) -> None:
        self.concepts_path = (concepts_path or DEFAULT_CONCEPTS_PATH).strip()
        self.vectors_cache_path = (vectors_cache_path or DEFAULT_CONCEPT_VECTORS_CACHE_PATH).strip()
        self.sim_threshold = float(max(0.0, min(1.0, sim_threshold if sim_threshold is not None else DEFAULT_SIM_THRESHOLD)))
        self.margin_threshold = float(
            max(0.0, min(1.0, margin_threshold if margin_threshold is not None else DEFAULT_MARGIN_THRESHOLD))
        )
        self.topk = max(1, int(topk if topk is not None else DEFAULT_TOPK))

        self.embeddings_provider = embeddings_provider or EmbeddingsProvider()
        self.embedding_model_name = self.embeddings_provider.model_name

        self._concepts: list[ConceptDefinition] = []
        self._concepts_by_id: dict[str, ConceptDefinition] = {}
        self._concept_vectors: dict[str, list[float]] = {}
        self._ready = False
        self._declared_version = ""
        self.concept_version = ""

    def _compute_file_hash(self, path: str) -> str:
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except Exception:
            return "missing"
        return hashlib.sha256(data).hexdigest()[:16]

    def _fallback_concepts(self) -> list[ConceptDefinition]:
        return [
            ConceptDefinition(
                concept_id="concept_ai_ict",
                label="AI/ICT",
                aliases=["AI", "生成AI", "ICT", "LLM"],
                anchor_texts=["AIやソフトウェアを開発提供する情報通信事業"],
                industry_hints=["情報処理・提供サービス業", "ソフトウェア業"],
            ),
            ConceptDefinition(
                concept_id="concept_automation",
                label="Automation",
                aliases=["自動化", "RPA"],
                anchor_texts=["業務自動化や省人化ソリューションを提供する"],
                industry_hints=["情報処理・提供サービス業", "生産用機械器具製造業"],
            ),
            ConceptDefinition(
                concept_id="concept_consulting",
                label="Consulting",
                aliases=["コンサル", "コンサルティング"],
                anchor_texts=["経営課題の調査分析と改善提案を提供する"],
                industry_hints=["経営コンサルタント業", "専門サービス業"],
            ),
            ConceptDefinition(
                concept_id="concept_ec",
                label="EC",
                aliases=["EC", "通販", "eコマース"],
                anchor_texts=["オンラインで商品を販売するEC運営事業"],
                industry_hints=["無店舗小売業", "各種商品小売業"],
            ),
            ConceptDefinition(
                concept_id="concept_manufacturing",
                label="Manufacturing",
                aliases=["製造", "加工"],
                anchor_texts=["工場で製品を製造加工し出荷する"],
                industry_hints=["製造業"],
            ),
            ConceptDefinition(
                concept_id="concept_construction",
                label="Construction",
                aliases=["建設", "施工", "工事"],
                anchor_texts=["建築土木設備工事を請け負う"],
                industry_hints=["総合工事業", "職別工事業", "設備工事業"],
            ),
        ]

    def _load_concepts(self) -> None:
        self._concepts = []
        self._concepts_by_id = {}
        self._declared_version = ""

        file_hash = self._compute_file_hash(self.concepts_path)
        loaded = False
        if self.concepts_path and os.path.exists(self.concepts_path):
            try:
                with open(self.concepts_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    self._declared_version = str(data.get("version") or "").strip()
                    rows = data.get("concepts")
                else:
                    rows = None
                if isinstance(rows, list):
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        concept_id = str(row.get("id") or "").strip()
                        label = str(row.get("label") or concept_id).strip()
                        if not concept_id:
                            continue
                        aliases_raw = row.get("aliases")
                        anchors_raw = row.get("anchor_texts")
                        hints_raw = row.get("industry_hints")
                        aliases = [str(v).strip() for v in aliases_raw if str(v).strip()] if isinstance(aliases_raw, list) else []
                        anchors = [str(v).strip() for v in anchors_raw if str(v).strip()] if isinstance(anchors_raw, list) else []
                        hints = [str(v).strip() for v in hints_raw if str(v).strip()] if isinstance(hints_raw, list) else []
                        if not anchors:
                            anchors = [label]
                        concept = ConceptDefinition(
                            concept_id=concept_id,
                            label=label or concept_id,
                            aliases=aliases,
                            anchor_texts=anchors,
                            industry_hints=hints,
                        )
                        self._concepts.append(concept)
                    loaded = bool(self._concepts)
            except Exception:
                loaded = False

        if not loaded:
            self._concepts = self._fallback_concepts()
            self._declared_version = "fallback"
            file_hash = "fallback"
            log.warning("Concept definitions not loaded from %s, using fallback concepts", self.concepts_path)

        self._concepts_by_id = {c.concept_id: c for c in self._concepts}
        version_seed = f"{self._declared_version}:{file_hash}" if self._declared_version else file_hash
        self.concept_version = hashlib.sha256(version_seed.encode("utf-8", errors="ignore")).hexdigest()[:12]

    def _load_vectors_from_cache(self) -> bool:
        if not self.vectors_cache_path or (not os.path.exists(self.vectors_cache_path)):
            return False
        try:
            with open(self.vectors_cache_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        if str(data.get("concept_version") or "") != self.concept_version:
            return False
        if str(data.get("embedding_model_name") or "") != self.embedding_model_name:
            return False
        vec_map = data.get("vectors")
        if not isinstance(vec_map, dict):
            return False

        loaded: dict[str, list[float]] = {}
        for concept in self._concepts:
            raw = vec_map.get(concept.concept_id)
            if not isinstance(raw, list):
                continue
            vec = _normalize_vector([float(v) for v in raw if isinstance(v, (int, float))])
            if not vec:
                continue
            loaded[concept.concept_id] = vec

        if len(loaded) != len(self._concepts):
            return False
        self._concept_vectors = loaded
        return True

    def _save_vectors_to_cache(self) -> None:
        if not self.vectors_cache_path:
            return
        directory = os.path.dirname(self.vectors_cache_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        payload = {
            "concept_version": self.concept_version,
            "embedding_model_name": self.embedding_model_name,
            "vectors": self._concept_vectors,
        }
        try:
            with open(self.vectors_cache_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False)
        except Exception:
            log.debug("failed to save concept vector cache: %s", self.vectors_cache_path, exc_info=True)

    def _build_concept_vectors(self) -> None:
        self._concept_vectors = {}
        if not self._concepts:
            return

        concept_texts: list[str] = []
        concept_ids: list[str] = []
        for concept in self._concepts:
            seed_texts: list[str] = []
            seen: set[str] = set()
            for text in concept.anchor_texts + [concept.label] + concept.aliases:
                raw = str(text or "").strip()
                key = norm_text_compact(raw)
                if not raw or not key or key in seen:
                    continue
                seen.add(key)
                seed_texts.append(raw)
            for st in seed_texts:
                concept_ids.append(concept.concept_id)
                concept_texts.append(st)

        vectors = self.embeddings_provider.embed_texts(concept_texts)
        by_id: dict[str, list[list[float] | None]] = {}
        for concept_id, vec in zip(concept_ids, vectors):
            by_id.setdefault(concept_id, []).append(vec)

        for concept in self._concepts:
            avg = _average_vectors(by_id.get(concept.concept_id, []))
            if not avg:
                continue
            self._concept_vectors[concept.concept_id] = avg

        missing = [c.concept_id for c in self._concepts if c.concept_id not in self._concept_vectors]
        if missing:
            log.warning("Concept vectors missing for %s concepts: %s", len(missing), ",".join(missing[:5]))

    def ensure_ready(self) -> None:
        if self._ready:
            return
        self._load_concepts()
        if not self._load_vectors_from_cache():
            self._build_concept_vectors()
            self._save_vectors_to_cache()
        self._ready = True

    def get_concept(self, concept_id: str) -> Optional[ConceptDefinition]:
        self.ensure_ready()
        return self._concepts_by_id.get(str(concept_id or "").strip())

    def concept_card(self, concept_id: str) -> dict[str, Any] | None:
        concept = self.get_concept(concept_id)
        if concept is None:
            return None
        return {
            "id": concept.concept_id,
            "label": concept.label,
            "industry_hints": list(concept.industry_hints),
            "aliases": list(concept.aliases[:8]),
        }

    def normalize_tag_to_concept(self, tag: str) -> dict[str, Any]:
        self.ensure_ready()
        text = str(tag or "").strip()
        if not text or not self._concept_vectors:
            return {
                "concept_id": None,
                "top1": {"id": None, "sim": 0.0},
                "top2": {"id": None, "sim": 0.0},
                "margin": 0.0,
                "topk": [],
                "decision": "hold",
            }

        qvec = self.embeddings_provider.embed_texts([text])[0]
        if not qvec:
            return {
                "concept_id": None,
                "top1": {"id": None, "sim": 0.0},
                "top2": {"id": None, "sim": 0.0},
                "margin": 0.0,
                "topk": [],
                "decision": "hold",
            }

        ranked: list[tuple[str, float]] = []
        for concept_id, cvec in self._concept_vectors.items():
            sim = _cosine_similarity(qvec, cvec)
            ranked.append((concept_id, float(sim)))
        ranked.sort(key=lambda x: (-x[1], x[0]))

        top_k = ranked[: max(1, min(self.topk, len(ranked)))]
        top1_id, top1_sim = top_k[0] if top_k else (None, -1.0)
        top2_id, top2_sim = top_k[1] if len(top_k) >= 2 else (None, -1.0)
        margin = float(top1_sim - top2_sim) if (top1_sim >= 0 and top2_sim >= 0) else float(max(0.0, top1_sim))
        auto = bool((top1_sim >= self.sim_threshold) and (margin >= self.margin_threshold))

        return {
            "concept_id": top1_id if auto else None,
            "top1": {"id": top1_id, "sim": float(top1_sim)},
            "top2": {"id": top2_id, "sim": float(top2_sim)},
            "margin": float(max(0.0, margin)),
            "topk": [{"id": cid, "sim": float(sim)} for cid, sim in top_k],
            "decision": "auto" if auto else "hold",
        }

    def normalize_tags(self, tags: list[str]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for tag in tags:
            text = str(tag or "").strip()
            key = norm_text_compact(text)
            if not text or not key or key in seen:
                continue
            seen.add(key)
            normalized = self.normalize_tag_to_concept(text)
            row = {"tag": text}
            row.update(normalized)
            out.append(row)
        return out

    def build_prompt_payload(self, *, tags: list[str], evidence_text: str = "") -> dict[str, Any]:
        normalized = self.normalize_tags(tags)

        topk_union: list[str] = []
        topk_seen: set[str] = set()
        auto_concepts: list[str] = []
        hold_tags: list[str] = []
        for row in normalized:
            if row.get("decision") == "auto" and row.get("concept_id"):
                concept_id = str(row.get("concept_id") or "")
                if concept_id and concept_id not in auto_concepts:
                    auto_concepts.append(concept_id)
            else:
                hold_tags.append(str(row.get("tag") or ""))
            for cand in row.get("topk") or []:
                if not isinstance(cand, dict):
                    continue
                concept_id = str(cand.get("id") or "")
                if not concept_id or concept_id in topk_seen:
                    continue
                topk_seen.add(concept_id)
                topk_union.append(concept_id)

        concept_cards: list[dict[str, Any]] = []
        for concept_id in topk_union[: self.topk]:
            card = self.concept_card(concept_id)
            if card:
                concept_cards.append(card)

        return {
            "concept_version": self.concept_version,
            "embedding_model_name": self.embedding_model_name,
            "sim_threshold": self.sim_threshold,
            "margin_threshold": self.margin_threshold,
            "topk": self.topk,
            "normalized_concepts": normalized,
            "auto_concepts": auto_concepts,
            "hold_tags": [t for t in hold_tags if t],
            "concept_topk_union": topk_union[: self.topk],
            "concept_cards": concept_cards,
            "evidence_text": str(evidence_text or "")[:1600],
        }

    def rebuild(self) -> None:
        self._ready = False
        self.ensure_ready()

