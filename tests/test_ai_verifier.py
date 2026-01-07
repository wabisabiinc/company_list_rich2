# tests/test_ai_verifier.py
import pytest
import json

# AIVerifier の import（src 配下でも直下でも通るように）
try:
    from src.ai_verifier import AIVerifier
except ModuleNotFoundError:
    from ai_verifier import AIVerifier


class DummyModel:
    def __init__(self, fake_result_json=None):
        self._fake_result_json = fake_result_json

    # 将来の引数追加に備えて互換化
    async def generate_content_async(self, items, *args, **kwargs):
        class DummyResponse:
            def __init__(self, text):
                self.text = text
        if self._fake_result_json is not None:
            return DummyResponse(json.dumps(self._fake_result_json))
        return DummyResponse("not a json")


@pytest.mark.asyncio
async def test_verify_info_success():
    fake_json = {
        "phone_number": "03-0000-0000",
        "address": "東京都中央区1-1-1",
        "description": "法人向けの業務支援システムを開発・提供する企業です。",
        "confidence": 0.93,
        "evidence": "代表電話：03-0000-0000　所在地：東京都中央区1-1-1",
    }

    listoss_data = {
        ("株式会社Example", "東京都中央区1-1-1"): {
            "addr": "東京都中央区1-1-1",
            "phone": "03-0000-0000",
            "hp": "https://example.com",
        }
    }

    verifier = AIVerifier(model=DummyModel(fake_json), listoss_data=listoss_data)
    result = await verifier.verify_info("dummy text", b"\x89PNG", "株式会社Example", "東京都中央区1-1-1")

    assert isinstance(result, dict)
    assert result["phone_number"] == "03-0000-0000"
    assert result["address"] == "東京都中央区1-1-1"
    assert isinstance(result.get("description"), str)
    assert abs(result["confidence"] - 0.93) < 1e-6
    assert isinstance(result.get("evidence"), str)


# 追加の健全性チェック
@pytest.mark.asyncio
async def test_verify_info_returns_none_on_non_json():
    verifier = AIVerifier(model=DummyModel(None))
    result = await verifier.verify_info("dummy", b"", "X", "Y")
    assert result is None


def test_normalize_amount_formats_and_converts():
    from src.ai_verifier import _normalize_amount

    assert _normalize_amount("30,449,952千円") == "30,449,952,000円"
    assert _normalize_amount("1,000万円") == "10,000,000円"
    assert _normalize_amount("6億6,700万円") == "667,000,000円"
    assert _normalize_amount("▲3百万円") == "▲3,000,000円"
    assert _normalize_amount("401,000千円") == "401,000,000円"

def test_description_prompt_requires_industry_when_hint_present():
    verifier = AIVerifier(model=DummyModel({}))
    prompt = verifier._build_description_prompt(
        "dummy text",
        company_name="株式会社Example",
        address="東京都中央区1-1-1",
        industry_hint="IT・ソフトウェア",
    )
    assert "業種ヒント" in prompt
    assert "業種を必ず含める" in prompt


@pytest.mark.asyncio
async def test_judge_official_homepage_parses_json():
    fake_json = {
        "is_official": True,
        "confidence": 0.85,
        "reason": "ドメイン一致",
        "description": "法人向けの業務支援システムを開発・提供する企業です。",
    }
    verifier = AIVerifier(model=DummyModel(fake_json))
    result = await verifier.judge_official_homepage("text", b"", "Example", "Tokyo", "https://example.com")
    assert result is not None
    assert result["is_official"] is True
    assert abs(result["confidence"] - 0.85) < 1e-6
    assert result["is_official_site"] is True
    assert abs(result["official_confidence"] - 0.85) < 1e-6
    assert isinstance(result.get("description"), str)
