# scripts/make_single_csv.py
import argparse, csv, re, sqlite3
from typing import Optional, List

EXCLUDE_DOMAINS = [
    "ekiten.jp","tabelog.com","hotpepper.jp","itp.ne.jp",
    "mapion.co.jp","google.com/maps","r.gnavi.co.jp","houjin.info",
    "navitime.co.jp","yahoo.co.jp","map.yahoo.co.jp","jp-hp.com"
]

def normalize_phone(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = re.sub(r"[‐―－ー−]+", "-", s)
    m = re.search(r"(0\d{1,4})-?(\d{1,4})-?(\d{3,4})", s)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None

def normalize_address(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip().replace("　"," ")
    s = re.sub(r"[‐―－ー−]+","-", s)
    s = re.sub(r"\s+"," ", s)
    s = re.sub(r"^〒\s*","〒", s)
    return s or None

def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u: return None
    u = u.strip().rstrip("/")
    return u or None

def pick(cands: List[str], cols: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for k in cands:
        if k.lower() in low: return low[k.lower()]
    for ex in cols:
        for k in cands:
            if k.lower() in ex.lower(): return ex
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/companies.db")
    ap.add_argument("--out", default="data/output.csv", help="最終CSV（上書き）")
    ap.add_argument("--include-status", nargs="*", default=["done","review"],
                    help="含めるstatus（既定: done review）")
    ap.add_argument("--only-with-homepage", action="store_true",
                    help="HP空欄の行を除外")
    ap.add_argument("--strict-homepage", action="store_true",
                    help="ディレクトリ系URLを除外")
    ap.add_argument("--dedupe", choices=["homepage","name_addr","id"], default="homepage",
                    help="重複排除キー（既定: homepage）")
    ap.add_argument("--add-domain", action="store_true",
                    help="hp_domain列を追加")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(companies)")
    cols = [r[1] for r in cur.fetchall()]

    col_id         = pick(["id"], cols) or "id"
    col_name       = pick(["company_name","name"], cols)
    col_addr       = pick(["address","input_address"], cols)
    col_found_addr = pick(["found_address","extracted_address"], cols)
    col_phone      = pick(["phone","phone_number"], cols)
    col_homepage   = pick(["homepage","homepage_url","website","url","hp"], cols)
    col_status     = pick(["status","state"], cols)
    col_emp        = pick(["employee_count","employees"], cols)
    col_source     = pick(["source_url"], cols)
    col_updated    = pick(["updated_at"], cols)

    select_cols = [c for c in [
        col_id, col_name, col_addr, col_found_addr, col_phone, col_homepage,
        col_status, col_emp, col_source, col_updated
    ] if c]

    q = f"SELECT {', '.join(select_cols)} FROM companies"
    where = []
    params = []

    # ★ここを修正：include_status
    if col_status and args.include_status:
        placeholders = ",".join(["?"] * len(args.include_status))
        where.append(f"{col_status} IN ({placeholders})")
        params.extend(args.include_status)

    if col_homepage and args.only_with_homepage:
        where.append(f"TRIM(IFNULL({col_homepage},''))<>''")

    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY " + col_id

    cur.execute(q, params)

    header = ["id","company_name","address","found_address","phone","homepage",
              "status","employee_count","source_url","updated_at"]

    if args.add_domain: header.append("hp_domain")

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()

        seen = set()
        for row in cur.fetchall():
            rec = dict(zip(select_cols, row))

            name = rec.get(col_name, "") if col_name else ""
            addr = normalize_address(rec.get(col_addr)) if col_addr else ""
            fa   = normalize_address(rec.get(col_found_addr)) if col_found_addr else ""
            tel  = normalize_phone(rec.get(col_phone)) if col_phone else ""
            hp   = normalize_url(rec.get(col_homepage)) if col_homepage else ""
            st   = rec.get(col_status, "") if col_status else ""
            emp  = rec.get(col_emp, "") if col_emp else ""
            src  = rec.get(col_source, "") if col_source else ""
            upd  = rec.get(col_updated, "") if col_updated else ""
            cid  = rec.get(col_id, "")

            if args.strict_homepage and hp and any(d in hp for d in EXCLUDE_DOMAINS):
                continue

            if args.dedupe == "homepage":
                key = hp or f"hp|{cid}"
            elif args.dedupe == "name_addr":
                key = f"{name}|{addr}"
            else:
                key = f"id|{cid}"
            if key in seen: continue
            seen.add(key)

            out = {
                "id": cid,
                "company_name": name,
                "address": addr or "",
                "found_address": fa or "",
                "phone": tel or (rec.get(col_phone) if col_phone else ""),
                "homepage": hp or "",
                "status": st,
                "employee_count": emp,
                "source_url": src,
                "updated_at": upd,
            }
            if args.add_domain:
                out["hp_domain"] = hp.split("/")[2] if hp and "://" in hp else ""
            w.writerow(out)

    conn.close()
    print(f"Single CSV written -> {args.out}")

if __name__ == "__main__":
    main()
