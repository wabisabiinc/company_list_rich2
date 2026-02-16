import argparse
import json

from src.concept_index import ConceptIndex


def main() -> int:
    parser = argparse.ArgumentParser(description="Build concept embedding index cache")
    parser.add_argument("--concepts", default="data/concepts.json", help="path to concepts.json")
    parser.add_argument(
        "--cache",
        default="data/concept_vectors_cache.json",
        help="path to concept vectors cache json",
    )
    parser.add_argument("--rebuild", action="store_true", help="force rebuild")
    args = parser.parse_args()

    index = ConceptIndex(concepts_path=args.concepts, vectors_cache_path=args.cache)
    if args.rebuild:
        index.rebuild()
    else:
        index.ensure_ready()

    sample_tags = ["AI", "自動化システム", "EC運営"]
    sample = index.build_prompt_payload(tags=sample_tags, evidence_text="")
    print(
        json.dumps(
            {
                "concept_version": index.concept_version,
                "embedding_model_name": index.embedding_model_name,
                "cache": args.cache,
                "sample": sample,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
