# src/ai_verifier.py

import os
import json
import logging
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import google.generativeai as generativeai
from google.api_core.exceptions import GoogleAPIError

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY が .env に設定されていません。")
generativeai.configure(api_key=api_key)

class AIVerifier:
    """
    AI検証クラス（画像は辞書で渡す方式に統一!）
    """
    def __init__(self, model=None):
        self.model = model or generativeai.GenerativeModel("gemini-1.5-pro")

    def _build_prompt(self, text: str) -> str:
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

    async def verify_info(self, text: str, screenshot: bytes) -> Optional[Dict[str, Any]]:
        prompt = self._build_prompt(text)
        try:
            # 画像は公式仕様のdict形式で渡す
            image_dict = {"mime_type": "image/png", "data": screenshot}
            response = await self.model.generate_content_async([
                prompt,
                image_dict
            ])
            raw = getattr(response, "text", None) or str(response)
            start = raw.find("{")
            end = raw.rfind("}") + 1
            json_str = raw[start:end]
            return json.loads(json_str)
        except (GoogleAPIError, json.JSONDecodeError, Exception) as e:
            logging.error(f"AIVerifier.verify_info failed: {e}")
            return None
