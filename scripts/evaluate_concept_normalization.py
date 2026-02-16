#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.concept_index import ConceptIndex
from src.industry_classifier import IndustryClassifier


def load_terms(path: str | None) -> list[str]:
    if not path:
        return [
            "AI",
            "ＡＩ",
            "生成AI",
            "機械知能",
            "AI開発",
            "自動化システム",
            "業務改善コンサルティング",
            "EC運営",
            "精密加工",
            "建設施工",
        ]
    p = Path(path)
    if not p.exists():
        return []
    if p.suffix.lower() in {".csv", ".tsv"}:
        delim = "\t" if p.suffix.lower() == ".tsv" else ","
        out: list[str] = []
        with p.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh, delimiter=delim)
            for row in reader:
                term = str(row.get("term") or row.get("tag") or "").strip()
                if term:
                    out.append(term)
        return out
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        term = line.strip()
        if term:
            out.append(term)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare legacy alias result vs concept normalization")
    parser.add_argument("--out", default="logs/concept_vs_alias_samples.jsonl")
    parser.add_argument("--terms", default="", help="optional file with term/tag column")
    args = parser.parse_args()

    terms = load_terms(args.terms)
    if not terms:
        print("no terms")
        return 1

    classifier = IndustryClassifier()
    concept_index = ConceptIndex()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    reduced_proxy = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for term in terms:
            alias_result = classifier.classify_from_aliases(term, [term])
            concept_result = concept_index.normalize_tag_to_concept(term)
            old_conf = float((alias_result or {}).get("confidence") or 0.0)
            old_source = str((alias_result or {}).get("source") or "")
            old_review = bool((alias_result or {}).get("review_required")) if alias_result else True
            new_decision = str(concept_result.get("decision") or "")
            likely_reduce_fp = bool(alias_result and old_source.startswith("alias") and (not old_review) and new_decision == "hold")
            if likely_reduce_fp:
                reduced_proxy += 1

            payload = {
                "term": term,
                "legacy_alias": {
                    "source": old_source,
                    "minor_code": (alias_result or {}).get("minor_code"),
                    "confidence": old_conf,
                    "review_required": old_review,
                },
                "concept_normalization": concept_result,
                "likely_reduce_false_positive": likely_reduce_fp,
            }
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    print(json.dumps({"terms": len(terms), "likely_reduce_false_positive": reduced_proxy, "out": str(out_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
