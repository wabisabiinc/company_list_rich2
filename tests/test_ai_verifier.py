# tests/test_ai_verifier.py
import pytest
import json
from src.ai_verifier import AIVerifier

class DummyResponse:
    def __init__(self, text):
        self.choices = [type("C", (), {"message": {"content": text}})]

class DummyClient:
    def __init__(self, api_key=None):
        # chat.completions.create が呼べる形だけ整える
        self.chat = type("Ch", (), {"completions": self})
    async def create(self, *, model, messages, images):
        # モックなので、入力された messages[0]["content"] を DummyResponse の text に流用
        return DummyResponse(messages[0]["content"])

@pytest.fixture(autouse=True)
def env_and_client(monkeypatch):
    # 環境変数をダミーで設定
    monkeypatch.setenv("GEMINI_API_KEY", "DUMMY_KEY")
    # Client クラスをモック化（AIVerifier.__init__ 時にこれが使われる）
    monkeypatch.setattr("src.ai_verifier.Client", lambda api_key: DummyClient(api_key))

def test_build_prompt():
    verifier = AIVerifier()
    sample_text = "電話番号: 03-0000-0000\n住所: 東京都中央区1-1-1"
    prompt = verifier._build_prompt(sample_text)
    assert "電話番号" in prompt
    assert sample_text in prompt
    # プロンプトが JSON フォーマットを示す終端で閉じているか
    assert prompt.strip().endswith("}")

@pytest.mark.asyncio
async def test_verify_info_success(monkeypatch):
    verifier = AIVerifier()
    # AIVerifier.verify_info 内で呼ばれる create() を置き換え
    fake_json = {"phone_number": "03-0000-0000", "address": "東京都中央区1-1-1"}
    fake_text = json.dumps(fake_json)
    class MockResp:
        choices = [type("C", (), {"message": {"content": fake_text}})]
    async def fake_create(**kwargs):
        return MockResp()
    # 直接 verifier.client.chat.completions.create をモック
    monkeypatch.setattr(verifier.client.chat.completions, "create", fake_create)

    result = await verifier.verify_info("dummy text", b"\x89PNG")
    assert isinstance(result, dict)
    assert result == fake_json

@pytest.mark.asyncio
async def test_verify_info_failure(monkeypatch):
    verifier = AIVerifier()
    # 不正 JSON を返すモック
    class MockResp:
        choices = [type("C", (), {"message": {"content": "not a json"}})]
    async def fake_create(**kwargs):
        return MockResp()
    monkeypatch.setattr(verifier.client.chat.completions, "create", fake_create)

    result = await verifier.verify_info("dummy", b"")
    assert result is None
