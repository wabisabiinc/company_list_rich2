import asyncio
import os
import logging
from dotenv import load_dotenv
from src.database_manager import DatabaseManager
from src.company_scraper import CompanyScraper
from src.ai_verifier import AIVerifier

# ロギング設定
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

load_dotenv()

async def process():
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    scraper  = CompanyScraper(headless=headless)
    verifier = AIVerifier()
    manager  = DatabaseManager()

    idx = 0
    while True:
        company = manager.get_next_company()
        if not company:
            logging.info("すべての企業データを処理しました。")
            break

        idx += 1
        name    = company.get("company_name", "")
        address = company.get("address", "")
        logging.info(f"[{idx}] {name} ({address}) の処理開始")

        try:
            urls = await scraper.search_company(name, address, num_results=3)
            logging.info(f"検索ヒットURL: {urls}")

            homepage = urls[0] if urls else ""
            phone, found_address = "", ""

            if homepage:
                logging.info(f"ページ取得中: {homepage}")
                info = await scraper.get_page_info(homepage)

                cands        = scraper.extract_candidates(info["text"])
                rule_phone   = cands["phone_numbers"][0] if cands["phone_numbers"] else ""
                rule_address = cands["addresses"][0]      if cands["addresses"]     else ""

                ai_result = await verifier.verify_info(info["text"], info["screenshot"])
                if ai_result:
                    phone         = ai_result.get("phone_number") or rule_phone
                    found_address = ai_result.get("address")      or rule_address
                else:
                    phone         = rule_phone
                    found_address = rule_address
            else:
                logging.info("有効なホームページが見つかりませんでした。")

            company.update({
                "homepage":      homepage,
                "phone":         phone,
                "found_address": found_address
            })

            logging.info(f"保存内容: {company}")
            manager.save_company_data(company)
            logging.info(f"保存完了: {name}")

        except Exception as e:
            logging.error(f"エラー発生: {name} ({address}) - {str(e)}", exc_info=True)
            # エラー時はstatusをerrorに
            manager.update_status(company['id'], 'error')

    manager.close()
    logging.info("全処理終了")

if __name__ == "__main__":
    asyncio.run(process())
