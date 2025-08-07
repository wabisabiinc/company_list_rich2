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
    AI検証クラス（GeminiマルチモーダルAPIで企業情報を抽出）
    """
    def __init__(self, model=None):
        self.model = model or generativeai.GenerativeModel("gemini-1.5-pro")

    def _build_prompt(self, text: str) -> str:
        # 日本の法人に最適化したプロンプト
        prompt = (
            "あなたは日本企業情報抽出の専門家です。\n"
            "添付のスクリーンショット画像と、下記のテキストから、企業の代表電話番号と本社所在地住所を正確に抽出してください。\n"
            "抽出時は次のルールを厳守してください：\n"
            "・電話番号は企業のメイン/代表番号のみを抽出し、部署直通やFAX番号、英数字が混じるものは除外してください。\n"
            "・電話番号は必ず「XXX-XXXX-XXXX」の形式（市外局番-市内局番-番号、ハイフン区切り、半角数字）で出力してください。\n"
            "・住所は都道府県から番地まで正確に記載してください。郵便番号や建物名、階数もあれば含めてください。\n"
            "・テキストや画像内に複数候補がある場合は、もっとも代表的なもの（通常は最初に掲載されたもの）を抽出してください。\n"
            "・情報が存在しない場合は \"null\" を出力してください（空文字や他の値は禁止）。\n"
            "・FAX番号やE-mail、Webサイトなどは絶対に出力しないでください。\n"
            "・出力は説明や補足なしで、次のJSON形式「のみ」に厳密に従ってください。\n\n"
            "# テキスト:\n"
            f"{text}\n\n"
            "# スクリーンショット: 添付画像参照\n\n"
            "【出力サンプル】\n"
            "{\n"
            "  \"phone_number\": \"03-1234-5678\" または null,\n"
            "  \"address\": \"東京都新宿区西新宿1-1-1 ○○ビル3F\" または null\n"
            "}\n"
            "※JSON以外の説明や文字列、根拠等は一切出力しないでください。"
        )
        return prompt

    async def verify_info(self, text: str, screenshot: bytes) -> Optional[Dict[str, Any]]:
        """
        Geminiでテキスト＋スクリーンショット画像から電話番号・住所を抽出
        """
        prompt = self._build_prompt(text)
        try:
            # 画像は公式仕様（辞書）で渡す
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
