from main import pick_best_rep


def test_pick_best_rep_rejects_employee_count_label() -> None:
    # 「従業員数」等が代表者候補に混ざっても採用しない
    assert pick_best_rep(["[LABEL]鶴 篤", "[LABEL]従業員数"]) == "鶴 篤"

