from src.company_scraper import CompanyScraper


def test_rep_label_does_not_capture_employee_label_as_value() -> None:
    # ラベル行が続くケースで、次のラベル（従業員数）を代表者の値として誤採用しない
    text = "\n".join(
        [
            "会社概要",
            "代表者",
            "鶴 篤",
            "従業員数",
            "125名",
        ]
    )
    scraper = CompanyScraper(headless=True)
    extracted = scraper.extract_candidates(text, html=None, page_type_hint="COMPANY_PROFILE")
    assert extracted.get("rep_names") == ["[LABEL]鶴 篤"]

