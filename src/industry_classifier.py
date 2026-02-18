from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable

from src.text_normalizer import norm_text, norm_text_compact

DEFAULT_BASECONNECT_JSON_PATH = os.getenv(
    "BASECONNECT_CATEGORIES_JSON_PATH",
    "docs/baseconnect_categories.json",
)
DEFAULT_BASECONNECT_CSV_PATH = os.getenv(
    "BASECONNECT_CATEGORIES_CSV_PATH",
    "docs/baseconnect_categories.csv",
)

_SUFFIXES = (
    "業界の会社",
    "業界",
    "の会社",
    "の企業",
    "会社",
    "企業",
)

_PAREN_RE = re.compile(r"[（(].*?[)）]")
_SPLIT_RE = re.compile(r"[／/・,，.．\-‐‑‒–—―ー_\s]+")

_GENERIC_KEYWORDS = {
    "その他",
    "業界",
    "会社",
    "企業",
    "事業",
    "業",
    "系",
    "関連",
}


@dataclass(frozen=True)
class CategoryEntry:
    top: str
    sub: str
    top_norm: str
    sub_norm: str
    aliases: tuple[str, ...]
    keywords: tuple[str, ...]


class IndustryClassifier:
    """
    Baseconnect-style industry classifier.
    Keeps the legacy class name for compatibility but uses Baseconnect categories only.
    """

    def __init__(
        self,
        csv_path: str | None = None,
        *_: Any,
        **__: Any,
    ) -> None:
        self.csv_path = csv_path or ""
        self.entries: list[CategoryEntry] = []
        self._entries_by_top: dict[str, list[CategoryEntry]] = {}
        self._top_aliases: dict[str, set[str]] = {}
        self.loaded = self._load()


    def _build_blocks(
        self,
        *,
        description: str,
        business_tags: Iterable[str] | None,
        profile_blocks: Iterable[str] | None,
        extra_blocks: Iterable[str] | None,
        company_name: str | None,
    ) -> list[tuple[str, str, int]]:
        blocks: list[tuple[str, str, int]] = []
        if description:
            blocks.append(("description", description, 3))
        for tag in business_tags or []:
            t = str(tag or "").strip()
            if t:
                blocks.append(("tag", t, 4))
        for block in profile_blocks or []:
            b = str(block or "").strip()
            if b:
                blocks.append(("profile", b, 2))
        for block in extra_blocks or []:
            b = str(block or "").strip()
            if b:
                blocks.append(("extra", b, 1))
        if company_name:
            blocks.append(("name", str(company_name), 1))
        return blocks

    def _score_blocks(
        self,
        blocks: list[tuple[str, str, int]],
    ) -> tuple[dict[CategoryEntry, int], dict[str, int]]:
        scores: dict[CategoryEntry, int] = {}
        top_scores: dict[str, int] = {}
        for _kind, text, weight in blocks:
            text_norm = norm_text_compact(text)
            if not text_norm:
                continue
            for entry in self.entries:
                inc = self._score_entry(entry, text_norm, weight)
                if not inc:
                    continue
                scores[entry] = scores.get(entry, 0) + inc
                top_scores[entry.top] = max(top_scores.get(entry.top, 0), scores[entry])

            for top, aliases in self._top_aliases.items():
                for alias in aliases:
                    if alias and alias in text_norm:
                        top_scores[top] = max(top_scores.get(top, 0), weight * 2)
        return scores, top_scores

    def rank_candidates(
        self,
        *,
        description: str,
        business_tags: Iterable[str] | None = None,
        profile_blocks: Iterable[str] | None = None,
        extra_blocks: Iterable[str] | None = None,
        company_name: str | None = None,
        top_k: int = 8,
    ) -> list[dict[str, Any]]:
        if not self.loaded:
            return []
        blocks = self._build_blocks(
            description=description,
            business_tags=business_tags,
            profile_blocks=profile_blocks,
            extra_blocks=extra_blocks,
            company_name=company_name,
        )
        if not blocks:
            return []
        scores, top_scores = self._score_blocks(blocks)
        ranked = sorted(
            scores.items(),
            key=lambda x: (-x[1], -len(x[0].sub_norm), x[0].sub_norm),
        )
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for entry, score in ranked:
            key = (entry.top, entry.sub)
            if key in seen:
                continue
            seen.add(key)
            results.append({"top": entry.top, "sub": entry.sub, "score": score})
            if len(results) >= max(1, int(top_k)):
                break
        if results:
            return results
        if top_scores:
            top = sorted(top_scores.items(), key=lambda x: (-x[1], x[0]))[0][0]
            sub = ""
            candidates = [e for e in self._entries_by_top.get(top, []) if "その他" in e.sub]
            if len(candidates) == 1:
                sub = candidates[0].sub
            return [{"top": top, "sub": sub, "score": top_scores.get(top, 0)}]
        return []

    def _load(self) -> bool:
        paths: list[str] = []
        if self.csv_path:
            paths.append(self.csv_path)
        else:
            paths.extend([DEFAULT_BASECONNECT_JSON_PATH, DEFAULT_BASECONNECT_CSV_PATH])

        rows: list[dict[str, str]] = []
        for path in paths:
            if not path or not os.path.exists(path):
                continue
            if path.lower().endswith(".json"):
                rows = self._read_rows_json(path)
            else:
                rows = self._read_rows_csv(path)
            if rows:
                break
        if not rows:
            return False

        entries: list[CategoryEntry] = []
        by_top: dict[str, list[CategoryEntry]] = {}
        top_aliases: dict[str, set[str]] = {}
        seen: set[tuple[str, str]] = set()

        for row in rows:
            top = (row.get("top") or row.get("Top") or "").strip()
            sub = (row.get("sub") or row.get("Sub") or "").strip()
            if not top or not sub:
                continue
            key = (top, sub)
            if key in seen:
                continue
            seen.add(key)

            top_norm = norm_text_compact(top)
            sub_norm = norm_text_compact(sub)
            aliases = tuple(sorted(self._build_aliases(sub)))
            keywords = tuple(sorted(self._build_keywords(sub)))

            entry = CategoryEntry(
                top=top,
                sub=sub,
                top_norm=top_norm,
                sub_norm=sub_norm,
                aliases=aliases,
                keywords=keywords,
            )
            entries.append(entry)
            by_top.setdefault(top, []).append(entry)

            top_aliases.setdefault(top, set()).update(self._build_aliases(top))

        self.entries = entries
        self._entries_by_top = by_top
        self._top_aliases = top_aliases
        return bool(self.entries)

    def _read_rows_csv(self, path: str) -> list[dict[str, str]]:
        for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
            try:
                with open(path, "r", encoding=enc, newline="") as f:
                    reader = csv.DictReader(f)
                    return [dict(row) for row in reader if row]
            except Exception:
                continue
        return []

    def _read_rows_json(self, path: str) -> list[dict[str, str]]:
        try:
            import json
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return []
        entries = payload.get("entries") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            return []
        rows: list[dict[str, str]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            top = str(item.get("top") or "").strip()
            sub = str(item.get("sub") or "").strip()
            if top and sub:
                rows.append({"top": top, "sub": sub})
        return rows

    def _strip_suffixes(self, text: str) -> str:
        raw = text.strip()
        if not raw:
            return ""
        changed = True
        while changed:
            changed = False
            for suf in _SUFFIXES:
                if raw.endswith(suf) and len(raw) > len(suf):
                    raw = raw[: -len(suf)].strip()
                    changed = True
        return raw

    def _remove_paren(self, text: str) -> str:
        return _PAREN_RE.sub("", text).strip()

    def _build_aliases(self, label: str) -> set[str]:
        variants: set[str] = set()
        raw = (label or "").strip()
        if not raw:
            return variants

        base = self._strip_suffixes(raw)
        for val in (raw, base):
            if val:
                variants.add(val)
                variants.add(self._remove_paren(val))

        if base.startswith("その他") and len(base) > 2:
            variants.add(base[2:])
        if raw.startswith("その他") and len(raw) > 2:
            variants.add(raw[2:])

        out: set[str] = set()
        for v in variants:
            n = norm_text_compact(v)
            if n:
                out.add(n)
        return out

    def _build_keywords(self, label: str) -> set[str]:
        base = self._strip_suffixes(label)
        base = self._remove_paren(base)
        if base.startswith("その他"):
            base = base[2:].strip()

        parts = [p for p in _SPLIT_RE.split(base) if p]
        out: set[str] = set()
        for part in parts:
            p = part.strip()
            if not p:
                continue
            p = re.sub(r"(業界|業|事業|会社|企業)$", "", p).strip()
            if not p:
                continue
            if p in _GENERIC_KEYWORDS:
                continue
            n = norm_text_compact(p)
            if len(n) < 2:
                continue
            out.add(n)
        return out

    def _score_entry(self, entry: CategoryEntry, text_norm: str, weight: int) -> int:
        score = 0
        for alias in entry.aliases:
            if alias and alias in text_norm:
                score += weight * 4
        for kw in entry.keywords:
            if kw and kw in text_norm:
                score += weight
        return score

    def classify(
        self,
        *,
        description: str,
        business_tags: Iterable[str] | None = None,
        profile_blocks: Iterable[str] | None = None,
        extra_blocks: Iterable[str] | None = None,
        company_name: str | None = None,
    ) -> dict[str, Any]:
        if not self.loaded:
            return {"top": "", "sub": "", "score": 0, "source": "unloaded"}

        blocks = self._build_blocks(
            description=description,
            business_tags=business_tags,
            profile_blocks=profile_blocks,
            extra_blocks=extra_blocks,
            company_name=company_name,
        )

        if not blocks:
            return {"top": "", "sub": "", "score": 0, "source": "empty"}

        scores, top_scores = self._score_blocks(blocks)

        if not scores and not top_scores:
            return {"top": "", "sub": "", "score": 0, "source": "no_match"}

        best_entry: CategoryEntry | None = None
        best_score = 0
        for entry, score in scores.items():
            if score > best_score:
                best_entry = entry
                best_score = score
            elif score == best_score and best_entry is not None:
                if len(entry.sub_norm) > len(best_entry.sub_norm):
                    best_entry = entry

        if best_entry and best_score > 0:
            return {
                "top": best_entry.top,
                "sub": best_entry.sub,
                "score": best_score,
                "source": "match",
            }

        # Fallback to top-only match
        if top_scores:
            top = sorted(top_scores.items(), key=lambda x: (-x[1], x[0]))[0][0]
            sub = ""
            candidates = [e for e in self._entries_by_top.get(top, []) if "その他" in e.sub]
            if len(candidates) == 1:
                sub = candidates[0].sub
            return {"top": top, "sub": sub, "score": top_scores.get(top, 0), "source": "top_only"}

        return {"top": "", "sub": "", "score": 0, "source": "no_match"}

    def resolve_exact_candidate_from_name(self, industry_text: str) -> dict[str, str] | None:
        if not self.loaded:
            return None
        norm = norm_text_compact(industry_text)
        if not norm:
            return None
        for entry in self.entries:
            if norm in {entry.sub_norm, entry.top_norm}:
                return {
                    "major_code": "",
                    "middle_code": "",
                    "minor_code": "",
                    "major_name": entry.top,
                    "middle_name": "",
                    "minor_name": entry.sub,
                }
        return None

    def build_candidates_from_industry_name(self, industry_text: str, top_n: int = 12) -> list[dict[str, str]]:
        if not self.loaded:
            return []
        norm = norm_text_compact(industry_text)
        if not norm:
            return []
        results: list[dict[str, str]] = []
        for entry in self.entries:
            if norm in entry.sub_norm or norm in entry.top_norm:
                results.append(
                    {
                        "major_code": "",
                        "middle_code": "",
                        "minor_code": "",
                        "major_name": entry.top,
                        "middle_name": "",
                        "minor_name": entry.sub,
                    }
                )
            if len(results) >= max(1, int(top_n)):
                break
        return results

    def format_candidates_text(self, candidates: list[dict[str, str]]) -> str:
        lines: list[str] = []
        for cand in candidates or []:
            top = (cand.get("major_name") or "").strip()
            sub = (cand.get("minor_name") or "").strip()
            if top and sub:
                lines.append(f"{top} / {sub}")
            elif top:
                lines.append(top)
            elif sub:
                lines.append(sub)
        return "\n".join(lines)
