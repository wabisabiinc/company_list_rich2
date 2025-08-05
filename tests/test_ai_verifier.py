import pytest
import json
from src.ai_verifier import AIVerifier

class DummyModel:
    def __init__(self, fake_result_json=None):
        self._fake_result_json = fake_result_json

    async def generate_content_async(self, items):
        class DummyResponse:
            def __init__(self, text):
                self.text = text
        if self._fake_result_json is not None:
            return DummyResponse(json.dumps(self._fake_result_json))
        else:
            return DummyResponse("not a json")

@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "DUMMY_KEY")

def test_build_prompt():
    verifier = AIVerifier(model=DummyModel())
    sample_text = "電話番号: 03-0000-0000\n住所: 東京都中央区1-1-1"
    prompt = verifier._build_prompt(sample_text)
    assert "電話番号" in prompt
    assert sample_text in prompt
    assert prompt.strip().endswith("}")

@pytest.mark.asyncio
async def test_verify_info_success():
    fake_json = {"phone_number": "03-0000-0000", "address": "東京都中央区1-1-1"}
    verifier = AIVerifier(model=DummyModel(fake_json))
    result = await verifier.verify_info("dummy text", b"\x89PNG")
    assert isinstance(result, dict)
    assert result == fake_json

@pytest.mark.asyncio
async def test_verify_info_failure():
    verifier = AIVerifier(model=DummyModel(None))
    result = await verifier.verify_info("dummy", b"")
    assert result is None
