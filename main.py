# main.py
import asyncio
import os
from dotenv import load_dotenv
from src.csv_data_manager import CsvDataManager
from src.company_scraper import CompanyScraper
from src.ai_verifier import AIVerifier

load_dotenv()

INPUT_PATH  = "data/input_companies.csv"
OUTPUT_PATH = "data/output.csv"

async def process():
    headless = os.getenv("HEADLESS", "true").lower() == "true"

    scraper  = CompanyScraper(headless=headless)
    verifier = AIVerifier()

    with CsvDataManager(INPUT_PATH, OUTPUT_PATH) as manager:
        idx = 0
        while True:
            company = manager.get_next_company()
            if not company:
                print("すべての企業データを処理しました。")
                break

            idx += 1
            name    = company.get("company_name", "")
            address = company.get("address", "")
            print(f"\n[{idx}] {name} ({address}) を検索中...")

            urls = await scraper.search_company(name, address, num_results=3)
            print(f"  検索ヒットURL: {urls}")

            homepage = urls[0] if urls else ""
            phone, found_address = "", ""

            if homepage:
                print(f"  ページ取得中: {homepage}")
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
                print("  有効なホームページが見つかりませんでした。")

            company.update({
                "homepage":      homepage,
                "phone":         phone,
                "found_address": found_address
            })
            manager.save_company_data(company)
            print(f"  保存完了: {name}")

if __name__ == "__main__":
    asyncio.run(process())
