# src/ai_verifier.py
import os
import json
import logging
from typing import Optional, Dict, Any
import google.generativeai as generativeai
from google.api_core.exceptions import GoogleAPIError

# google-generativeai の Client が存在しない場合はプレースホルダを定義
Client = getattr(generativeai, "Client", None)
if Client is None:
    class Client:
        def __init__(self, api_key: str):
            pass
        class chat:
            class completions:
                @staticmethod
                async def create(**kwargs):
                    raise NotImplementedError("Multimodal AI client is not configured.")

class AIVerifier:
    """
    AI検証クラス
    - .env から GEMINI_API_KEY を読み込み
    - Google Generative AI (Gemini) マルチモーダルモデルで情報検証
    """

    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY is not set in environment")
        self.client = Client(api_key=api_key)
        self.model = "models/multimodal-gecko-alpha-1"

    def _build_prompt(self, text: str) -> str:
        """
        Sub-issue 6-3: プロンプト生成
        """
        prompt = (
            "あなたは企業の情報をウェブサイトから正確に抽出する専門家です。\n"
            "添付のスクリーンショットと以下のテキストから、問い合わせ電話番号と住所を抽出してください。\n\n"
            "# テキスト:\n"
            f"{text}\n\n"
            "# スクリーンショット: 添付画像を参照\n\n"
            "必ず以下のJSON形式で出力してください:\n"
            "{\n"
            "  \"phone_number\": \"抽出した電話番号\" または null,\n"
            "  \"address\": \"抽出した住所\" または null\n"
            "}"
        )
        return prompt

    async def verify_info(
        self,
        text: str,
        screenshot: bytes
    ) -> Optional[Dict[str, Any]]:
        """
        Sub-issue 6-4 & 6-5: マルチモーダルでAPIリクエストを送信し結果を解析
        """
        prompt = self._build_prompt(text)
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                images=[{"data": screenshot}],
            )
            raw = response.choices[0].message["content"]
            start = raw.find("{")
            end = raw.rfind("}") + 1
            json_str = raw[start:end]
            return json.loads(json_str)
        except (GoogleAPIError, json.JSONDecodeError, NotImplementedError) as e:
            logging.error(f"AIVerifier.verify_info failed: {e}")
            return None