from main import (
    FINAL_DESCRIPTION_MAX_LEN,
    FINAL_DESCRIPTION_MIN_LEN,
    build_final_description_from_payloads,
    infer_industry_and_business_tags,
)


def test_infer_industry_logistics() -> None:
    industry, tags = infer_industry_and_business_tags(
        ["物流センター運営、倉庫保管、配送・輸送サービスを提供しています。"]
    )
    assert industry == "物流・運送"
    assert any(t in tags for t in ("物流", "配送", "倉庫", "運送"))


def test_infer_industry_unknown_when_no_signals() -> None:
    industry, tags = infer_industry_and_business_tags(["ようこそ。"])
    assert industry == ""
    assert tags == []


def test_build_final_description_from_payloads_length_and_content() -> None:
    payloads = [
        {
            "text": "\n".join(
                [
                    "当社は物流センターの運営や配送サービスを提供し、企業のサプライチェーン最適化を支援しています。",
                    "全国ネットワークで倉庫保管から輸送まで一貫した物流ソリューションを展開し、業務効率化に貢献します。",
                    "お問い合わせはこちら。",
                ]
            ),
            "html": "",
        }
    ]
    desc = build_final_description_from_payloads(payloads)
    assert desc
    assert FINAL_DESCRIPTION_MIN_LEN <= len(desc) <= FINAL_DESCRIPTION_MAX_LEN
    assert "お問い合わせ" not in desc
