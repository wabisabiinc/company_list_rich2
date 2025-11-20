# scripts/report_kpis.py
import argparse, sqlite3, re, sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.import_csv_to_db import load_hubspot_data

LOG_PATH = Path("logs/app.log")

# -------------------------------
# 文字列正規化
# -------------------------------
def normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"[‐―－ー−]+", "-", s)  # いろんなハイフンを半角ハイフンに
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def normalize_address(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip().replace("　", " ")
    s = re.sub(r"[‐―－ー−]+", "-", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"^〒\s*", "〒", s)
    return s or None

def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    return u.rstrip("/")

# -------------------------------
# ログから直近N時間の保存件数
# -------------------------------
def recent_saves_from_log(hours: int) -> int:
    if not LOG_PATH.exists():
        return 0
    since = datetime.now() - timedelta(hours=hours)
    cnt = 0
    with open(LOG_PATH, encoding="utf-8") as f:
        for line in f:
            if "保存完了" not in line:
                continue
            try:
                ts = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
                if ts >= since:
                    cnt += 1
            except Exception:
                continue
    return cnt

# -------------------------------
# DBスキーマ検出
# -------------------------------
def pick_column(candidate_lists: List[List[str]], existing_cols: List[str]) -> List[Optional[str]]:
    picked = []
    low_cols = {c.lower(): c for c in existing_cols}
    for candidates in candidate_lists:
        found = None
        for c in candidates:
            if c.lower() in low_cols:
                found = low_cols[c.lower()]
                break
        if not found:
            for exist in existing_cols:
                for c in candidates:
                    if c.lower() in exist.lower():
                        found = exist
                        break
                if found:
                    break
        picked.append(found)
    return picked

# -------------------------------
# メイン
# -------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/companies.db")
    ap.add_argument("--hours", type=int, default=1, help="直近N時間の処理件数（ログ）")
    ap.add_argument("--hubspot", nargs="*", default=[], help="HubSpot CSV（複数可）")
    ap.add_argument("--done-value", default="done", help="完了を表すstatusの値（既定: done）")
    args = ap.parse_args()

    recent = recent_saves_from_log(args.hours)

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(companies)")
    cols = [r[1] for r in cur.fetchall()]

    (col_company_name,) = pick_column([["company_name", "name"]], cols)
    (col_input_addr,) = pick_column([["address", "input_address", "orig_address"]], cols)
    (col_found_addr,) = pick_column([["found_address", "extracted_address", "detected_address"]], cols)
    (col_phone,) = pick_column([["phone", "phone_number", "found_phone"]], cols)
    (col_homepage,) = pick_column([["homepage", "homepage_url", "found_homepage", "website", "url", "hp"]], cols)
    (col_status,) = pick_column([["status", "state", "result_status"]], cols)

    if not col_company_name:
        print("ERROR: 会社名の列が見つかりません（company_name/name）", file=sys.stderr)
        sys.exit(2)

    cur.execute("SELECT COUNT(*) FROM companies")
    all_rows = cur.fetchone()[0] or 0

    if col_status:
        cur.execute(f"SELECT COUNT(*) FROM companies WHERE {col_status}=?", (args.done_value,))
        done_rows = cur.fetchone()[0] or 0
    else:
        done_rows = all_rows

    if col_homepage:
        cur.execute(f"SELECT COUNT(*) FROM companies WHERE TRIM(IFNULL({col_homepage},''))<>''")
        hp_rows_all = cur.fetchone()[0] or 0
        if col_status:
            cur.execute(
                f"SELECT COUNT(*) FROM companies WHERE {col_status}=? AND TRIM(IFNULL({col_homepage},''))<>''",
                (args.done_value,),
            )
            hp_rows_done = cur.fetchone()[0] or 0
        else:
            hp_rows_done = hp_rows_all
    else:
        hp_rows_all = hp_rows_done = 0

    conn.commit()

    success_rate = (done_rows / all_rows * 100) if all_rows else 0.0
    hp_rate_all = (hp_rows_all / all_rows * 100) if all_rows else 0.0
    hp_rate_done = (hp_rows_done / done_rows * 100) if done_rows else 0.0

    print("=== KPI Report ===")
    print(f"直近{args.hours}時間の保存件数（ログ）: {recent} 件")
    print(f"全レコード数: {all_rows} / 完了({args.done_value}): {done_rows}（{success_rate:.1f}%）")

    if col_homepage:
        print(f"HP取得率（全体）: {hp_rows_all}/{all_rows} = {hp_rate_all:.1f}%")
        print(f"HP取得率（doneのみ）: {hp_rows_done}/{done_rows} = {hp_rate_done:.1f}%")
    else:
        print("HP取得率: 対応列が見つかりません（homepage/homepage_url/hp 等）")

    if args.hubspot:
        gt = load_hubspot_data(args.hubspot)
        addr_match = phone_match = hp_match = 0
        addr_total = phone_total = hp_total = 0

        query_cols = [c for c in [col_company_name, col_input_addr, col_found_addr, col_phone, col_homepage] if c]
        cur.execute(f"SELECT {', '.join(query_cols)} FROM companies")
        for row in cur.fetchall():
            idx = 0
            name = row[idx]; idx += 1
            in_addr = row[idx] if col_input_addr else None; idx += 1 if col_input_addr else 0
            found_addr = row[idx] if col_found_addr else None; idx += 1 if col_found_addr else 0
            phone = row[idx] if col_phone else None; idx += 1 if col_phone else 0
            hp = row[idx] if col_homepage else None

            name_key = (name or "").strip()
            addr_key = normalize_address(in_addr) or ""
            truth = gt.get((name_key, addr_key))
            if not truth and found_addr:
                truth = gt.get((name_key, normalize_address(found_addr) or ""))
            if not truth:
                continue

            truth_addr = normalize_address(truth.get("addr"))
            if truth_addr and found_addr:
                addr_total += 1
                if normalize_address(found_addr) == truth_addr:
                    addr_match += 1

            truth_phone = normalize_phone(truth.get("phone"))
            if truth_phone and phone:
                phone_total += 1
                if normalize_phone(phone) == truth_phone:
                    phone_match += 1

            truth_hp = normalize_url(truth.get("hp"))
            if truth_hp and hp:
                hp_total += 1
                if normalize_url(hp) == truth_hp:
                    hp_match += 1

        print("--- HubSpot Ground Truth (住所/電話/HP) ---")
        if addr_total:
            print(f"一致率（住所）: {addr_match}/{addr_total} = {addr_match/addr_total*100:.1f}%")
        else:
            print("一致率（住所）: -")
        if phone_total:
            print(f"一致率（電話）: {phone_match}/{phone_total} = {phone_match/phone_total*100:.1f}%")
        else:
            print("一致率（電話）: -")
        if hp_total:
            print(f"一致率（HP）: {hp_match}/{hp_total} = {hp_match/hp_total*100:.1f}%")
        else:
            print("一致率（HP）: -")

    conn.close()

if __name__ == "__main__":
    main()
