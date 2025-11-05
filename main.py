# main.py
import asyncio
import os
import csv
import logging
import re
import random
from dotenv import load_dotenv

from src.database_manager import DatabaseManager
from src.company_scraper import CompanyScraper
from src.ai_verifier import AIVerifier, DEFAULT_MODEL as AI_MODEL_NAME

# --------------------------------------------------
# ロギング設定
# --------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# .env 読み込み
load_dotenv()

# --------------------------------------------------
# 実行オプション（.env）
# --------------------------------------------------
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
USE_AI = os.getenv("USE_AI", "true").lower() == "true"
WORKER_ID = os.getenv("WORKER_ID", "w1")  # 並列識別子

MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
ID_MIN = int(os.getenv("ID_MIN", "0"))
ID_MAX = int(os.getenv("ID_MAX", "0"))
AI_COOLDOWN_SEC = float(os.getenv("AI_COOLDOWN_SEC", "0"))
SLEEP_BETWEEN_SEC = float(os.getenv("SLEEP_BETWEEN_SEC", "0"))
JITTER_RATIO = float(os.getenv("JITTER_RATIO", "0.30"))

MIRROR_TO_CSV = os.getenv("MIRROR_TO_CSV", "false").lower() == "true"
OUTPUT_CSV_PATH = os.getenv("OUTPUT_CSV_PATH", "data/output.csv")
CSV_FIELDNAMES = [
    "id", "company_name", "address", "employee_count",
    "homepage", "phone", "found_address", "rep_name", "description"
]

# --------------------------------------------------
# 正規化 & 一致判定
# --------------------------------------------------
def normalize_phone(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"[‐―－ー]+", "-", s)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def normalize_address(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip().replace("　", " ")
    s = re.sub(r"[‐―－ー]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    m = re.search(r"(\d{3}-\d{4})\s*(.*)", s)
    if m:
        body = m.group(2).strip()
        return f"〒{m.group(1)} {body}"
    return s if s else None

def addr_compatible(input_addr: str, found_addr: str) -> bool:
    input_addr = normalize_address(input_addr)
    found_addr = normalize_address(found_addr)
    if not input_addr or not found_addr:
        return False
    return input_addr[:8] in found_addr or found_addr[:8] in input_addr

# --------------------------------------------------
# 内部: 次ジョブ取得
# --------------------------------------------------
def claim_next(manager: DatabaseManager) -> dict | None:
    if hasattr(manager, "claim_next_company"):
        return manager.claim_next_company(WORKER_ID)
    return manager.get_next_company()

# --------------------------------------------------
# ユーティリティ：ジッター付きスリープ秒
# --------------------------------------------------
def jittered_seconds(base: float, ratio: float) -> float:
    if base <= 0 or ratio <= 0:
        return max(0.0, base)
    low = max(0.0, base * (1.0 - ratio))
    high = base * (1.0 + ratio)
    return random.uniform(low, high)

# --------------------------------------------------
# メイン処理（ワーカー）
# --------------------------------------------------
async def process():
    log.info(
        "=== Runner started (worker=%s) === HEADLESS=%s USE_AI=%s MAX_ROWS=%s "
        "ID_MIN=%s ID_MAX=%s AI_COOLDOWN_SEC=%s SLEEP_BETWEEN_SEC=%s JITTER_RATIO=%.2f "
        "MIRROR_TO_CSV=%s",
        WORKER_ID, HEADLESS, USE_AI, MAX_ROWS, ID_MIN, ID_MAX,
        AI_COOLDOWN_SEC, SLEEP_BETWEEN_SEC, JITTER_RATIO, MIRROR_TO_CSV
    )

    scraper = CompanyScraper(headless=HEADLESS)
    # CompanyScraper に start()/close() が無い実装でも動くように安全に呼ぶ
    if hasattr(scraper, "start") and callable(getattr(scraper, "start")):
        try:
            await scraper.start()
        except Exception:
            log.warning("scraper.start() はスキップ（未実装または失敗）", exc_info=True)

    verifier = AIVerifier() if USE_AI else None
    manager = DatabaseManager()

    csv_file = None
    csv_writer = None
    try:
        if MIRROR_TO_CSV:
            os.makedirs(os.path.dirname(OUTPUT_CSV_PATH) or ".", exist_ok=True)
            file_exists = os.path.exists(OUTPUT_CSV_PATH) and os.path.getsize(OUTPUT_CSV_PATH) > 0
            csv_file = open(OUTPUT_CSV_PATH, mode="a", newline="", encoding="utf-8")
            csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
            if not file_exists:
                csv_writer.writeheader()
                csv_file.flush()
            log.info("CSV mirror enabled -> %s", OUTPUT_CSV_PATH)

        processed = 0

        while True:
            if MAX_ROWS and processed >= MAX_ROWS:
                log.info("MAX_ROWS=%s に到達。", MAX_ROWS)
                break

            company = claim_next(manager)
            if not company:
                log.info("キューが空です。終了。")
                break

            cid = company.get("id")
            name = (company.get("company_name") or "").strip()
            addr = (company.get("address") or "").strip()

            if (ID_MIN and cid < ID_MIN) or (ID_MAX and cid > ID_MAX):
                log.info("[skip] id=%s はレンジ外 -> skipped (worker=%s)", cid, WORKER_ID)
                manager.update_status(cid, "skipped")
                continue

            log.info("[%s] %s の処理開始 (worker=%s)", cid, name, WORKER_ID)

            try:
                urls = await scraper.search_company(name, addr, num_results=5)
                homepage = ""
                info = None
                phone, found_address = "", ""

                for candidate in urls:
                    candidate_info = await scraper.get_page_info(candidate)
                    if scraper.is_likely_official_site(name, candidate, candidate_info.get("text", "") or ""):
                        homepage = candidate
                        info = candidate_info
                        break
                    log.info("[%s] 非公式と判断: %s", cid, candidate)

                phone = ""
                found_address = ""
                rep_name_val = (company.get("rep_name") or "").strip()
                description_val = (company.get("description") or "").strip()
                phone_source = "none"
                address_source = "none"
                ai_used = 0
                ai_model = ""
                company.setdefault("error_code", "")
                src_phone = ""
                src_addr = ""
                src_rep = ""
                verify_result = {"phone_ok": False, "address_ok": False}
                confidence = 0.0

                if homepage and info:
                    info_url = info.get("url") or homepage
                    cands = scraper.extract_candidates(info.get("text", "") or "")
                    rule_phone = normalize_phone(cands["phone_numbers"][0]) if cands.get("phone_numbers") else None
                    rule_address = normalize_address(cands["addresses"][0]) if cands.get("addresses") else None
                    rule_rep = (cands.get("rep_names") or [None])[0]

                    ai_result = None
                    ai_attempted = False
                    if USE_AI and verifier is not None:
                        ai_attempted = True
                        try:
                            ai_result = await verifier.verify_info(
                                info.get("text", "") or "", info.get("screenshot"),
                                name, addr
                            )
                        except Exception:
                            log.warning("[%s] AI検証失敗 -> ルールベースにフォールバック", cid, exc_info=True)
                            ai_result = None

                    ai_phone: str | None = None
                    ai_addr: str | None = None
                    ai_rep: str | None = None
                    if ai_result:
                        ai_used = 1
                        ai_model = AI_MODEL_NAME
                        ai_phone = normalize_phone(ai_result.get("phone_number"))
                        ai_addr = normalize_address(ai_result.get("address"))
                        ai_rep = ai_result.get("rep_name") or ai_result.get("representative")
                        if isinstance(ai_rep, str):
                            ai_rep = ai_rep.strip() or None
                        description = ai_result.get("description")
                        if isinstance(description, str) and description.strip():
                            description_val = description.strip()[:50]
                    else:
                        if ai_attempted and AI_COOLDOWN_SEC > 0:
                            await asyncio.sleep(jittered_seconds(AI_COOLDOWN_SEC, JITTER_RATIO))

                    if ai_phone:
                        phone = ai_phone
                        phone_source = "ai"
                        src_phone = info_url
                    elif rule_phone:
                        phone = rule_phone
                        phone_source = "rule"
                        src_phone = info_url
                    else:
                        phone = ""
                        phone_source = "none"

                    if ai_addr:
                        found_address = ai_addr
                        address_source = "ai"
                        src_addr = info_url
                    elif rule_address:
                        found_address = rule_address or ""
                        address_source = "rule" if rule_address else "none"
                        if rule_address:
                            src_addr = info_url
                    else:
                        found_address = ""
                        address_source = "none"

                    if ai_rep:
                        rep_name_val = ai_rep
                        src_rep = info_url
                    elif rule_rep:
                        rep_name_val = rule_rep.strip()
                        src_rep = info_url

                    # 欠落情報があれば浅く探索して補完
                    need_phone = not bool(phone)
                    need_addr = not bool(found_address)
                    need_rep = not bool(rep_name_val)
                    if need_phone or need_addr or need_rep:
                        try:
                            related = await scraper.crawl_related(
                                homepage,
                                need_phone,
                                need_addr,
                                need_rep,
                                max_pages=6,
                                max_hops=2,
                            )
                        except Exception:
                            related = {}
                        for url, data in related.items():
                            text = data.get("text", "") or ""
                            cc = scraper.extract_candidates(text)
                            if need_phone and cc.get("phone_numbers"):
                                cand = normalize_phone(cc["phone_numbers"][0])
                                if cand:
                                    phone = cand
                                    phone_source = "rule"
                                    src_phone = url
                                    need_phone = False
                            if need_addr and cc.get("addresses"):
                                cand_addr = normalize_address(cc["addresses"][0])
                                if cand_addr:
                                    found_address = cand_addr
                                    address_source = "rule"
                                    src_addr = url
                                    need_addr = False
                            if need_rep and cc.get("rep_names"):
                                cand_rep = (cc["rep_names"][0] or "").strip()
                                if cand_rep:
                                    rep_name_val = cand_rep
                                    src_rep = url
                                    need_rep = False
                            if not (need_phone or need_addr or need_rep):
                                break

                    try:
                        verify_result = await scraper.verify_on_site(homepage, phone or None, found_address or None)
                    except Exception:
                        log.warning("[%s] verify_on_site 失敗", cid, exc_info=True)
                        verify_result = {"phone_ok": False, "address_ok": False}

                    matches = int(bool(verify_result.get("phone_ok"))) + int(bool(verify_result.get("address_ok")))
                    if matches == 2:
                        confidence = 1.0
                    elif matches == 1:
                        confidence = 0.8
                    else:
                        confidence = 0.4
                else:
                    if urls:
                        log.info("[%s] 公式サイト候補を判別できず -> 未保存", cid)
                    else:
                        log.info("[%s] 有効なホームページ候補なし。", cid)
                    company["rep_name"] = company.get("rep_name", "") or ""
                    company["description"] = company.get("description", "") or ""
                    confidence = 0.4

                normalized_found_address = normalize_address(found_address) if found_address else ""
                rep_name_val = rep_name_val.strip()
                description_val = description_val.strip()[:50]
                company.update({
                    "homepage": homepage,
                    "phone": phone or "",
                    "found_address": normalized_found_address,
                    "rep_name": rep_name_val,
                    "description": description_val,
                    "phone_source": phone_source,
                    "address_source": address_source,
                    "ai_used": ai_used,
                    "ai_model": ai_model,
                    "extract_confidence": confidence,
                    "source_url_phone": src_phone,
                    "source_url_address": src_addr,
                    "source_url_rep": src_rep,
                })

                status = "done" if homepage else "error"
                if status == "done" and found_address and not addr_compatible(addr, found_address):
                    status = "review"
                if status == "done" and not verify_result.get("phone_ok") and not verify_result.get("address_ok"):
                    status = "review"

                company.setdefault("error_code", "")

                manager.save_company_data(company, status=status)
                log.info("[%s] 保存完了: status=%s (worker=%s)", cid, status, WORKER_ID)

                if csv_writer:
                    csv_writer.writerow({k: company.get(k, "") for k in CSV_FIELDNAMES})
                    csv_file.flush()

                processed += 1

            except Exception as e:
                log.error("[%s] エラー: %s (worker=%s)", cid, e, WORKER_ID, exc_info=True)
                manager.update_status(cid, "error")

            # 1社ごとのスリープ（±JITTERでレート制限/ドメイン集中回避）
            if SLEEP_BETWEEN_SEC > 0:
                await asyncio.sleep(jittered_seconds(SLEEP_BETWEEN_SEC, JITTER_RATIO))

    finally:
        if csv_file:
            csv_file.close()
        if hasattr(scraper, "close") and callable(getattr(scraper, "close")):
            try:
                await scraper.close()
            except Exception:
                log.warning("scraper.close() はスキップ（未実装または失敗）", exc_info=True)
        manager.close()
        log.info("全処理終了 (worker=%s)", WORKER_ID)

if __name__ == "__main__":
    asyncio.run(process())
