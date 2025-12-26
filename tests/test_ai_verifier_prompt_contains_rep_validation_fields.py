from src.ai_verifier import AIVerifier


def test_rich_prompt_includes_representative_validation_fields() -> None:
    v = AIVerifier(model=object())
    prompt = v._build_rich_prompt("{}", company_name="X", csv_address="Y")
    assert "representative_valid" in prompt
    assert "representative_invalid_reason" in prompt
