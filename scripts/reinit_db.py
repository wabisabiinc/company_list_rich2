#!/usr/bin/env python3
import argparse, csv, os, sqlite3, time, sys
from typing import List, Dict, Any, Iterable, Tuple

DB_PATH_DEFAULT = "data/companies.db"

# --- 巨大セル対応: フィールド上限を最大化（最初に実行） ---
try:
    csv.field_size_limit(2_147_483_647)  # 2GB相当
except Exception:
    pass

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    company_name   TEXT,
    address        TEXT,
    employee_count INTEGER,
    homepage       TEXT,
    phone          TEXT,
    found_address  TEXT,
    status         TEXT DEFAULT 'pending',
    locked_by      TEXT,
    locked_at      TEXT,
    rep_name       TEXT,
    description    TEXT,
    listing        TEXT,
    revenue        TEXT,
    profit         TEXT,
    capital        TEXT,
    fiscal_month   TEXT,
    founded_year   TEXT,
    ai_used        INTEGER DEFAULT 0,
    ai_model       TEXT,
    phone_source   TEXT,
    address_source TEXT,
    extract_confidence REAL,
    last_checked_at TEXT,
    hubspot_id     TEXT,
    corporate_number     TEXT,
    corporate_number_norm TEXT,
    source_csv     TEXT,
    reference_homepage TEXT,
    reference_phone TEXT,
    reference_address TEXT,
    accuracy_homepage TEXT,
    accuracy_phone TEXT,
    accuracy_address TEXT,
    homepage_official_flag INTEGER,
    homepage_official_source TEXT,
    homepage_official_score REAL,
    official_homepage TEXT,
    alt_homepage TEXT,
    alt_homepage_type TEXT,
    source_url_phone TEXT,
    source_url_address TEXT,
    source_url_rep TEXT
);
"""
INDEX_SQL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_name_addr ON companies(company_name, address);",
    "CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);",
    "CREATE INDEX IF NOT EXISTS idx_companies_emp ON companies(employee_count);",
    "CREATE INDEX IF NOT EXISTS idx_companies_corporate_number_norm ON companies(corporate_number_norm);",
    "CREATE INDEX IF NOT EXISTS idx_companies_hubspot_id ON companies(hubspot_id);",
]

ALIASES = {
    "company_name": {"company_name","name","企業名","会社名","法人名","商号","社名","名称"},
    "address": {"address","住所","所在地","本社所在地","本店所在地"},
    "zipcode": {"郵便番号","zip","zipcode"},
    "pref": {"都道府県"},
    "city": {"市区町村","市町村","市区郡町村"},
    "employee_count": {"employee_count","従業員数","従業員","従業員数(単独)","従業員数（単独）"},
    "homepage": {"homepage","url","URL","ホームページ","サイト","公式サイト","出典URL"},
    "phone": {"phone","電話番号","TEL","tel"},
    "found_address": {"found_address"},
    "listing": {"上場区分","上場","市場"},
    "revenue": {"売上","売上高"},
    "profit": {"利益","営業利益","経常利益"},
    "capital": {"資本金"},
    "fiscal_month": {"決算","決算月","会計期"},
    "founded_year": {"設立","創業","創立"},
}
NOISE_KEYWORDS = ("リストスが提供しています", "お問い合わせフォーム送信代行", "ftj-g.co.jp", "listoss.com")

# ---------- SQLite ----------
def open_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    conn.execute("PRAGMA busy_timeout=15000;")
    conn.execute("PRAGMA wal_autocheckpoint=1000;")
    return conn

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    for sql in INDEX_SQL:
        conn.execute(sql)
    conn.commit()

def drop_db(db_path: str) -> None:
    for ext in ("", "-wal", "-shm"):
        p = db_path + ext
        if os.path.exists(p):
            os.remove(p)

# ---------- CSV ヘッダー検出 ----------
def _score_header(fields: List[str]) -> int:
    s = 0
    low = [f.strip().lower() for f in fields]
    hit = 0
    for aliases in ALIASES.values():
        if any(a.lower() in low for a in aliases):
            hit += 1
    s += hit * 3
    s += max(0, len(fields) - 3)      # 列数が多い候補を優先
    if not any(a.lower() in low for a in ALIASES["company_name"]):
        s -= 5                         # 会社名が無い行は減点
    return s

def detect_header(path: str, enc: str="utf-8-sig") -> Tuple[str, int, List[str]]:
    """
    先頭100行から本当のヘッダー行を見つける。
    - 区切り候補: , \t ; |
    - 注意書き/広告行は除外
    """
    cands = [",", "\t", ";", "|"]
    with open(path, "r", encoding=enc, errors="replace") as f:
        lines = [next(f, "") for _ in range(100)]
    best = ("", -1, [], -10)

    for d in cands:
        for idx, line in enumerate(lines):
            if not line or line.strip() == "":
                continue
            if any(k in line for k in NOISE_KEYWORDS):
                continue
            fields = [c.strip() for c in line.rstrip("\r\n").split(d)]
            if len(fields) < 3:
                continue
            score = _score_header(fields)
            if score > best[3]:
                best = (d, idx, fields, score)

    delim, idx, fns, _ = best
    if idx < 0:   # フォールバック
        for L in lines:
            if L and L.strip():
                return (",", 0, [c.strip() for c in L.split(",")])
        return (",", 0, ["company_name","address"])
    return (delim, idx, fns)

def open_dict_reader(path: str) -> Tuple[csv.DictReader, str, int, List[str]]:
    delim, header_idx, fieldnames = detect_header(path)
    f = open(path, "r", encoding="utf-8-sig", errors="replace", newline="")
    # ヘッダー行までスキップ
    for _ in range(header_idx):
        f.readline()
    reader = csv.DictReader(f, fieldnames=fieldnames, delimiter=delim)
    next(reader, None)  # ヘッダー自身を飛ばす
    print(f"[reinit] {os.path.basename(path)} delimiter='{delim}' header={fieldnames}", file=sys.stderr)
    return reader, delim, header_idx, fieldnames

# ---------- 行→レコード変換 ----------
def _pick(row: Dict[str, Any], key: str) -> Any:
    for cand in ALIASES[key]:
        if cand in row and str(row[cand]).strip() != "":
            return row[cand]
    return None

def _to_int(x: Any) -> int | None:
    try:
        if x is None: return None
        s = str(x).strip()
        if not s: return None
        return int(s)
    except Exception:
        return None

def _compose_address(r: Dict[str, Any]) -> str:
    # 住所カラムが無ければ「〒郵便番号 + 都道府県 + 市区町村 + 住所」を合成
    addr = str(_pick(r, "address") or "").strip()
    zipcode = str(_pick(r, "zipcode") or "").strip().replace("〒","")
    pref = str(_pick(r, "pref") or "").strip()
    city = str(_pick(r, "city") or "").strip()
    body = addr
    if not body:
        body = f"{pref}{city}".strip()
    if zipcode and body:
        return f"〒{zipcode} {body}"
    return body or addr

def row_from_csv(r: Dict[str, Any]) -> Dict[str, Any] | None:
    company_name = (str(_pick(r, "company_name") or "").strip())
    address = _compose_address(r).strip()
    if company_name == "" and address == "":
        return None
    return {
        "company_name": company_name,
        "address": address,
        "employee_count": _to_int(_pick(r, "employee_count")),
        "homepage": "",
        "phone": (str(_pick(r, "phone") or "").strip()),
        "found_address": (str(_pick(r, "found_address") or "").strip()),
        "status": "pending",
        "locked_by": None,
        "locked_at": None,
        "rep_name": None,
        "description": None,
        "listing": (str(_pick(r, "listing") or "").strip()),
        "revenue": (str(_pick(r, "revenue") or "").strip()),
        "profit": (str(_pick(r, "profit") or "").strip()),
        "capital": (str(_pick(r, "capital") or "").strip()),
        "fiscal_month": (str(_pick(r, "fiscal_month") or "").strip()),
        "founded_year": (str(_pick(r, "founded_year") or "").strip()),
    }

def iter_csv_rows(args) -> Iterable[Dict[str, Any]]:
    """
    listoss_merged_data_*.csv の先頭にある注意文・URL行などの“前置き”をスキップし、
    実データ（ヘッダー: 会社名, 郵便番号, ...）以降だけを返す。
    """
    files = getattr(args, "seed", []) or []
    for path in files:
        with open(path, "rb") as fb:
            head = fb.read(65536).decode("utf-8-sig", errors="replace")
        start = 0
        lines = head.splitlines()
        for i, ln in enumerate(lines[:100]):
            if ln.startswith("会社名") and ("郵便" in ln or "郵便番号" in ln):
                start = i
                break

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for _ in range(start):
                next(reader, None)
            header = next(reader, None)
            if not header:
                continue
            header = [h.strip() for h in header]
            for row in reader:
                if not any(cell.strip() for cell in row):
                    continue
                row_dict = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
                try:
                    item = row_from_csv(row_dict)
                    if item:
                        yield item
                except csv.Error:
                    continue

# ---------- バルク挿入 ----------
def bulk_insert(conn: sqlite3.Connection, rows: Iterable[Dict[str, Any]], chunk: int = 5000) -> Tuple[int,int]:
    cur = conn.cursor()
    cols = ("company_name","address","employee_count","homepage","phone","found_address",
            "status","locked_by","locked_at","rep_name","description",
            "listing","revenue","profit","capital","fiscal_month","founded_year")
    sql = f"INSERT OR IGNORE INTO companies ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"

    buf: list[Dict[str, Any]] = []
    read_total = 0
    added_total = 0

    def flush():
        nonlocal added_total
        if not buf: return
        cur.execute("BEGIN IMMEDIATE;")
        cur.executemany(sql, [tuple(b[c] for c in cols) for b in buf])
        added = cur.execute("SELECT changes()").fetchone()[0]
        conn.commit()
        added_total += int(added)
        buf.clear()

    for item in rows:
        read_total += 1
        buf.append(item)
        if len(buf) >= chunk:
            flush()
    flush()
    return read_total, added_total

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Re-init companies.db and seed from CSVs.")
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--drop", action="store_true")
    ap.add_argument("--seed", nargs="*")
    args = ap.parse_args()

    if args.drop:
        drop_db(args.db)
    conn = open_conn(args.db)
    ensure_schema(conn)

    before = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    read_n = added_n = 0
    if args.seed:
        st = time.time()
        read_n, added_n = bulk_insert(conn, iter_csv_rows(args))
        dt = time.time() - st
        print(f"seed processed: {read_n} rows, inserted(add): {added_n} rows in {dt:.2f}s")
    after = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    print(f"rows before={before}, after={after}, added={after-before}")
    conn.close()

if __name__ == "__main__":
    main()
