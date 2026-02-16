import datetime as dt
import hashlib
import json
import logging
import math
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

from src.text_normalizer import norm_text_compact

try:
    import google.generativeai as _genai
except Exception:
    _genai = None  # type: ignore


log = logging.getLogger(__name__)

DEFAULT_EMBED_PROVIDER = (os.getenv("CONCEPT_EMBEDDING_PROVIDER") or "auto").strip().lower()
DEFAULT_EMBED_MODEL = (os.getenv("CONCEPT_EMBEDDING_MODEL") or "models/text-embedding-004").strip()
DEFAULT_SENTENCE_MODEL = (
    os.getenv("CONCEPT_SENTENCE_TRANSFORMERS_MODEL")
    or "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
).strip()
DEFAULT_EMBED_CACHE_PATH = (os.getenv("CONCEPT_EMBED_CACHE_PATH") or "data/embedding_cache.sqlite3").strip()
DEFAULT_NGRAM_DIM = max(64, int(os.getenv("CONCEPT_NGRAM_DIM", "384")))


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
    inv = 1.0 / math.sqrt(total)
    return [v * inv for v in out]


class _EmbedBackend:
    name = ""

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        raise NotImplementedError


class _GeminiEmbedBackend(_EmbedBackend):
    def __init__(self, model_name: str) -> None:
        self.model_name = (model_name or DEFAULT_EMBED_MODEL).strip()
        self.api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
        self.available = bool(self.api_key and self.model_name and _genai is not None)
        self.name = f"gemini:{self.model_name}" if self.available else "gemini:unavailable"
        if not self.available:
            return
        try:
            _genai.configure(api_key=self.api_key)  # type: ignore[union-attr]
        except Exception:
            self.available = False
            self.name = "gemini:unavailable"

    @staticmethod
    def _extract_vector(resp: Any) -> list[float] | None:
        if resp is None:
            return None
        if isinstance(resp, dict):
            emb = resp.get("embedding")
            if isinstance(emb, list):
                return _normalize_vector([float(x) for x in emb if isinstance(x, (int, float))])
            if isinstance(emb, dict):
                values = emb.get("values")
                if isinstance(values, list):
                    return _normalize_vector([float(x) for x in values if isinstance(x, (int, float))])
        emb_attr = getattr(resp, "embedding", None)
        if isinstance(emb_attr, list):
            return _normalize_vector([float(x) for x in emb_attr if isinstance(x, (int, float))])
        values_attr = getattr(emb_attr, "values", None)
        if isinstance(values_attr, list):
            return _normalize_vector([float(x) for x in values_attr if isinstance(x, (int, float))])
        return None

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        if not self.available:
            return [None for _ in texts]
        out: list[list[float] | None] = []
        for text in texts:
            item = str(text or "").strip()
            if not item:
                out.append(None)
                continue
            try:
                resp = _genai.embed_content(  # type: ignore[union-attr]
                    model=self.model_name,
                    content=item,
                    task_type="SEMANTIC_SIMILARITY",
                )
            except Exception:
                out.append(None)
                continue
            out.append(self._extract_vector(resp))
        return out


class _SentenceTransformersBackend(_EmbedBackend):
    def __init__(self, model_name: str) -> None:
        self.model_name = (model_name or DEFAULT_SENTENCE_MODEL).strip()
        self.name = f"sentence-transformers:{self.model_name}"
        self._model = None
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._model = SentenceTransformer(self.model_name)
        except Exception:
            self._model = None
            self.name = "sentence-transformers:unavailable"

    @property
    def available(self) -> bool:
        return self._model is not None

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        if self._model is None:
            return [None for _ in texts]
        clean = [str(t or "").strip() for t in texts]
        valid_idx = [idx for idx, txt in enumerate(clean) if txt]
        if not valid_idx:
            return [None for _ in texts]
        inputs = [clean[idx] for idx in valid_idx]
        try:
            vectors = self._model.encode(inputs, normalize_embeddings=True)  # type: ignore[operator]
        except Exception:
            return [None for _ in texts]
        out: list[list[float] | None] = [None for _ in texts]
        for pos, idx in enumerate(valid_idx):
            vec = vectors[pos]
            if hasattr(vec, "tolist"):
                vec = vec.tolist()
            if not isinstance(vec, list):
                out[idx] = None
                continue
            out[idx] = _normalize_vector([float(x) for x in vec if isinstance(x, (int, float))])
        return out


class _NgramBackend(_EmbedBackend):
    def __init__(self, dim: int = DEFAULT_NGRAM_DIM) -> None:
        self.dim = max(64, int(dim))
        self.name = f"ngram-hash-v1:{self.dim}"

    def _embed_one(self, text: str) -> list[float] | None:
        compact = norm_text_compact(text)
        if len(compact) < 2:
            return None
        vec = [0.0] * self.dim
        grams: list[str] = []
        for n in (2, 3):
            if len(compact) < n:
                continue
            for idx in range(0, len(compact) - n + 1):
                grams.append(compact[idx : idx + n])
        if not grams:
            return None
        for gram in grams:
            slot = (hash(gram) & 0x7FFFFFFF) % self.dim
            vec[slot] += 1.0
        return _normalize_vector(vec)

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        return [self._embed_one(str(t or "")) for t in texts]


@dataclass
class EmbeddingResult:
    text: str
    vector: list[float] | None


class EmbeddingsProvider:
    def __init__(
        self,
        *,
        provider: str | None = None,
        model_name: str | None = None,
        sentence_model_name: str | None = None,
        cache_path: str | None = None,
    ) -> None:
        chosen = (provider or DEFAULT_EMBED_PROVIDER or "auto").strip().lower()
        self.cache_path = (cache_path or DEFAULT_EMBED_CACHE_PATH).strip()
        self._backend = self._build_backend(
            chosen,
            model_name=(model_name or DEFAULT_EMBED_MODEL),
            sentence_model_name=(sentence_model_name or DEFAULT_SENTENCE_MODEL),
        )
        self.provider_name = self._backend.name.split(":", 1)[0]
        self.model_name = self._backend.name
        self._ensure_cache_table()

    def _build_backend(self, provider: str, *, model_name: str, sentence_model_name: str) -> _EmbedBackend:
        mode = (provider or "auto").strip().lower()
        if mode == "auto":
            gem = _GeminiEmbedBackend(model_name)
            if getattr(gem, "available", False):
                return gem
            st = _SentenceTransformersBackend(sentence_model_name)
            if getattr(st, "available", False):
                return st
            return _NgramBackend()
        if mode == "gemini":
            gem = _GeminiEmbedBackend(model_name)
            if getattr(gem, "available", False):
                return gem
            log.warning("Concept embeddings: gemini backend unavailable, fallback to ngram")
            return _NgramBackend()
        if mode in {"sentence", "sentence-transformers", "sentence_transformers"}:
            st = _SentenceTransformersBackend(sentence_model_name)
            if getattr(st, "available", False):
                return st
            log.warning("Concept embeddings: sentence-transformers backend unavailable, fallback to ngram")
            return _NgramBackend()
        if mode in {"ngram", "local"}:
            return _NgramBackend()
        if mode == "off":
            return _NgramBackend()
        log.warning("Concept embeddings: unknown provider=%s, fallback to ngram", mode)
        return _NgramBackend()

    def _ensure_cache_table(self) -> None:
        if not self.cache_path:
            return
        directory = os.path.dirname(self.cache_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(self.cache_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS embeddings_cache (
                    cache_key TEXT PRIMARY KEY,
                    model_name TEXT NOT NULL,
                    text_norm TEXT NOT NULL,
                    vector_json TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _cache_key(self, text: str) -> tuple[str, str]:
        norm = norm_text_compact(text)
        key_src = f"{self.model_name}\n{norm}".encode("utf-8", errors="ignore")
        return hashlib.sha256(key_src).hexdigest(), norm

    def _load_cached_vectors(self, texts: list[str]) -> tuple[list[list[float] | None], list[int], list[str]]:
        out: list[list[float] | None] = [None for _ in texts]
        missing_idx: list[int] = []
        missing_texts: list[str] = []
        if not self.cache_path:
            for idx, txt in enumerate(texts):
                if str(txt or "").strip():
                    missing_idx.append(idx)
                    missing_texts.append(txt)
            return out, missing_idx, missing_texts

        conn = sqlite3.connect(self.cache_path)
        try:
            for idx, text in enumerate(texts):
                item = str(text or "").strip()
                if not item:
                    continue
                cache_key, _norm = self._cache_key(item)
                row = conn.execute(
                    "SELECT vector_json FROM embeddings_cache WHERE cache_key=? AND model_name=? LIMIT 1",
                    (cache_key, self.model_name),
                ).fetchone()
                if row and row[0]:
                    try:
                        vec = json.loads(str(row[0]))
                    except Exception:
                        vec = None
                    if isinstance(vec, list):
                        out[idx] = _normalize_vector([float(v) for v in vec if isinstance(v, (int, float))])
                        continue
                missing_idx.append(idx)
                missing_texts.append(item)
        finally:
            conn.close()
        return out, missing_idx, missing_texts

    def _save_cached_vectors(self, items: list[EmbeddingResult]) -> None:
        if not self.cache_path or not items:
            return
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        conn = sqlite3.connect(self.cache_path)
        try:
            for item in items:
                text = str(item.text or "").strip()
                if not text:
                    continue
                cache_key, norm = self._cache_key(text)
                vec_json = json.dumps(item.vector or [], ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO embeddings_cache(cache_key, model_name, text_norm, vector_json, created_at)
                    VALUES(?, ?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        model_name=excluded.model_name,
                        text_norm=excluded.text_norm,
                        vector_json=excluded.vector_json,
                        created_at=excluded.created_at
                    """,
                    (cache_key, self.model_name, norm, vec_json, now),
                )
            conn.commit()
        finally:
            conn.close()

    def embed_texts(self, texts: list[str]) -> list[list[float] | None]:
        if not texts:
            return []
        cached, missing_idx, missing_texts = self._load_cached_vectors(texts)
        if missing_texts:
            new_vectors = self._backend.embed_texts(missing_texts)
            to_cache: list[EmbeddingResult] = []
            for idx, raw_vec, text in zip(missing_idx, new_vectors, missing_texts):
                norm_vec = _normalize_vector(raw_vec)
                cached[idx] = norm_vec
                to_cache.append(EmbeddingResult(text=text, vector=norm_vec))
            self._save_cached_vectors(to_cache)
        return cached

    def clear_cache(self) -> None:
        if not self.cache_path or (not os.path.exists(self.cache_path)):
            return
        conn = sqlite3.connect(self.cache_path)
        try:
            conn.execute("DELETE FROM embeddings_cache")
            conn.commit()
        finally:
            conn.close()

