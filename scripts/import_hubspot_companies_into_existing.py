import argparse
import csv, sys, os, re, sqlite3, unicodedata
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from src.company_scraper import CompanyScraper  # rep_name の簡易クレンジングに再利用

DEFAULT_DB_PATH = os.environ.get("COMPANIES_DB_PATH", "data/companies.db")
DB_PATH = DEFAULT_DB_PATH

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
    address_confidence REAL,
    address_evidence TEXT,
    address_conflict_level TEXT,
    address_review_reason TEXT,
    homepage_official_flag INTEGER,
    homepage_official_source TEXT,
    homepage_official_score REAL,
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

DATASET_CONFIGS = {
    "hubspot": {
        "homepage_keys": ("ホームページ", "HP", "Webサイト", "website"),
        "rep_keys": ("代表者名", "代表取締役", "代表者"),
        "desc_keys": ("説明文", "会社説明", "備考"),
        "industry_keys": ("業種", "業界"),
        "phone_source": "hubspot",
        "address_source": "hubspot",
    },
    "logistics": {
        "homepage_keys": ("ホームページ", "HP", "Webサイト", "website"),
        "rep_keys": ("代表者名", "代表取締役", "代表者"),
        "desc_keys": ("説明文", "会社説明", "備考"),
        "industry_keys": ("業種_保険DB", "業種グループ", "業種", "業界"),
        "phone_source": "logistics_csv",
        "address_source": "logistics_csv",
    },
}

CURRENT_CONFIG = DATASET_CONFIGS["hubspot"]
HOMEPAGE_KEYS = CURRENT_CONFIG["homepage_keys"]
REP_KEYS = CURRENT_CONFIG["rep_keys"]
DESC_KEYS = CURRENT_CONFIG["desc_keys"]
INDUSTRY_KEYS = CURRENT_CONFIG["industry_keys"]
PHONE_SOURCE_VALUE = CURRENT_CONFIG["phone_source"]
ADDRESS_SOURCE_VALUE = CURRENT_CONFIG["address_source"]


def apply_dataset_config(name: str):
    global CURRENT_CONFIG, HOMEPAGE_KEYS, REP_KEYS, DESC_KEYS, INDUSTRY_KEYS, PHONE_SOURCE_VALUE, ADDRESS_SOURCE_VALUE
    if name not in DATASET_CONFIGS:
        raise ValueError(f"Unknown dataset: {name}")
    CURRENT_CONFIG = DATASET_CONFIGS[name]
    HOMEPAGE_KEYS = CURRENT_CONFIG["homepage_keys"]
    REP_KEYS = CURRENT_CONFIG["rep_keys"]
    DESC_KEYS = CURRENT_CONFIG["desc_keys"]
    INDUSTRY_KEYS = CURRENT_CONFIG["industry_keys"]
    PHONE_SOURCE_VALUE = CURRENT_CONFIG["phone_source"]
    ADDRESS_SOURCE_VALUE = CURRENT_CONFIG["address_source"]


def ensure_db_schema():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript(SCHEMA_SQL)
        for sql in INDEX_SQL:
            con.execute(sql)
        con.commit()
    finally:
        con.close()


def existing_cols():
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(companies)")
        return {row[1] for row in cur.fetchall()}
    finally:
        con.close()


def to_int(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    val = str(s).strip().replace(",", "")
    if not val:
        return None
    try:
        return int(float(val))
    except ValueError:
        digits = re.sub(r"[^0-9]", "", val)
        return int(digits) if digits else None


def first(*vals):
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None


def clean_description_text(val):
    if val is None:
        return None
    text = unicodedata.normalize("NFKC", str(val))
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if re.search(r"https?://|mailto:|@|＠|tel[:：]|電話|ＴＥＬ|ＴＥＬ：", text, flags=re.I):
        return None
    if any(term in text for term in ("お問い合わせ", "お問合せ", "アクセス", "採用", "求人", "予約")):
        return None
    return text


def clean(val):
    if val is None:
        return None
    val = unicodedata.normalize("NFKC", str(val))
    val = re.sub(r"\s+", " ", val).strip()
    return val or None


def resolve_employee_col(cols):
    if "employee_count" in cols:
        return "employee_count"
    for col in cols:
        if col and col.lower().startswith("employee"):
            return col
    return None


def is_blank(value):
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def determine_pk(data):
    pk = first(data.get("corporate_number_norm"), data.get("corporate_number"), data.get("hubspot_id"))
    if pk:
        return pk
    name = data.get("company_name")
    address = data.get("address")
    if name or address:
        return f"{name or ''}|{address or ''}"
    return None


def normalize_row(row):
    homepage = first(*(row.get(key) for key in HOMEPAGE_KEYS))
    rep_name = first(*(row.get(key) for key in REP_KEYS))
    rep_name = CompanyScraper.clean_rep_name(rep_name) if rep_name else None
    description = clean_description_text(first(*(row.get(key) for key in DESC_KEYS)))
    industry = first(*(row.get(key) for key in INDUSTRY_KEYS))
    data = {
        "hubspot_id": clean(row.get("レコードID")),
        "company_name": clean(row.get("会社名")),
        "address": clean(row.get("都道府県／地域")),
        "phone": clean(row.get("電話番号")),
        "homepage": homepage,
        "corporate_number": clean(row.get("法人番号")),
        "corporate_number_norm": clean(row.get("法人番号（名寄せ）")),
        "employee_count": to_int(row.get("従業員数")),
        "rep_name": rep_name,
        "description": description,
        "listing": clean(row.get("上場区分")),
        "industry_group": clean(row.get("業種グループ")),
        "industry": industry,
    }
    data["pk"] = determine_pk(data)
    return data


def fetch_existing(cur, pk):
    cur.execute("SELECT * FROM companies WHERE id=?", (pk,))
    return cur.fetchone()


def find_by_company_address(cur, name, address):
    cur.execute(
        "SELECT * FROM companies WHERE company_name=? AND address=? LIMIT 1",
        (name, address),
    )
    return cur.fetchone()

def find_by_corporate_number(cur, corporate_number_norm, corporate_number, cols):
    """
    既に別IDで登録されているレコードを、法人番号で名寄せして取り出す。
    - 取り込みデータの pk が法人番号であっても、DB側の id が別形式/別値のことがあるため。
    """
    if corporate_number_norm and "corporate_number_norm" in cols:
        cur.execute(
            "SELECT * FROM companies WHERE corporate_number_norm=? LIMIT 1",
            (corporate_number_norm,),
        )
        row = cur.fetchone()
        if row:
            return row
    if corporate_number and "corporate_number" in cols:
        cur.execute(
            "SELECT * FROM companies WHERE corporate_number=? LIMIT 1",
            (corporate_number,),
        )
        row = cur.fetchone()
        if row:
            return row
    return None


def update_existing(cur, target_id, existing, data, cols, employee_col, csv_name):
    updates = []
    params = []

    def set_text(column, value):
        if column not in cols or not value:
            return False
        if is_blank(existing[column]):
            updates.append(f"{column}=?")
            params.append(value)
            return True
        return False

    def set_identifier(column, value):
        if column not in cols or not value:
            return
        if is_blank(existing[column]):
            updates.append(f"{column}=?")
            params.append(value)

    def set_numeric(column, value):
        if column not in cols or value is None:
            return
        if existing[column] is None:
            updates.append(f"{column}=?")
            params.append(value)

    set_text("homepage", data.get("homepage"))
    phone_updated = set_text("phone", data.get("phone"))
    address_updated = set_text("address", data.get("address"))
    set_text("rep_name", data.get("rep_name"))
    set_text("description", data.get("description"))
    set_text("listing", data.get("listing"))
    set_text("industry_group", data.get("industry_group"))
    set_text("industry", data.get("industry"))
    set_identifier("hubspot_id", data.get("hubspot_id"))
    set_identifier("corporate_number", data.get("corporate_number"))
    set_identifier("corporate_number_norm", data.get("corporate_number_norm"))
    if employee_col:
        set_numeric(employee_col, data.get("employee_count"))
    if phone_updated and "phone_source" in cols:
        updates.append("phone_source=?")
        params.append(PHONE_SOURCE_VALUE)
    if address_updated and "address_source" in cols:
        updates.append("address_source=?")
        params.append(ADDRESS_SOURCE_VALUE)
    if "source_csv" in cols:
        updates.append("source_csv=?")
        params.append(csv_name)
    if updates:
        params.append(target_id)
        cur.execute(f"UPDATE companies SET {', '.join(updates)} WHERE id=?", params)


def insert_new(cur, target_id, data, cols, employee_col, csv_name):
    columns = ["id"]
    values = [target_id]
    placeholders = ["?"]

    def add(column, value, require_value=True):
        if column not in cols:
            return
        if require_value and value is None:
            return
        columns.append(column)
        values.append(value)
        placeholders.append("?")

    add("status", "pending", require_value=False)
    add("source_csv", csv_name, require_value=False)
    add("hubspot_id", data.get("hubspot_id"))
    add("corporate_number", data.get("corporate_number"))
    add("corporate_number_norm", data.get("corporate_number_norm"))
    add("company_name", data.get("company_name"))
    add("address", data.get("address"))
    add("homepage", data.get("homepage"))
    add("phone", data.get("phone"))
    add("rep_name", data.get("rep_name"))
    add("description", data.get("description"))
    add("listing", data.get("listing"))
    add("industry_group", data.get("industry_group"))
    add("industry", data.get("industry"))
    if employee_col and data.get("employee_count") is not None:
        add(employee_col, data.get("employee_count"), require_value=False)
    add("phone_source", PHONE_SOURCE_VALUE, require_value=False)
    add("address_source", ADDRESS_SOURCE_VALUE, require_value=False)
    cur.execute(
        f"INSERT INTO companies ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
        values,
    )


def upsert_row(cur, data, cols, employee_col, csv_name):
    existing = fetch_existing(cur, data["pk"])
    target_id = data["pk"]
    if not existing:
        match = find_by_corporate_number(cur, data.get("corporate_number_norm"), data.get("corporate_number"), cols)
        if match:
            existing = match
            target_id = match["id"]
    if not existing and data.get("company_name") and data.get("address") and {"company_name", "address"}.issubset(cols):
        match = find_by_company_address(cur, data["company_name"], data["address"])
        if match:
            existing = match
            target_id = match["id"]
    elif existing:
        target_id = existing["id"]
    if existing:
        update_existing(cur, target_id, existing, data, cols, employee_col, csv_name)
    else:
        insert_new(cur, target_id, data, cols, employee_col, csv_name)


def process_csv(csv_path, cols, employee_col):
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    processed = 0
    basename = os.path.basename(csv_path)
    with open(csv_path, newline="", encoding="utf-8-sig") as handle, sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        reader = csv.DictReader(handle)
        for row in reader:
            data = normalize_row(row)
            if not data.get("pk"):
                continue
            upsert_row(cur, data, cols, employee_col, basename)
            processed += 1
        con.commit()
    return processed


def main(args):
    parser = argparse.ArgumentParser(description="Import HubSpot/Logistics CSVs into the companies DB.")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to the SQLite DB (new files are created if missing)")
    parser.add_argument("--dataset", choices=DATASET_CONFIGS.keys(), default="hubspot", help="Column mapping preset")
    parser.add_argument("csvs", nargs="+", help="CSV files to import")
    parsed = parser.parse_args(args)

    global DB_PATH
    DB_PATH = parsed.db
    apply_dataset_config(parsed.dataset)
    ensure_db_schema()
    cols = existing_cols()
    if not cols:
        print("companies table not found in database.")
        sys.exit(3)
    employee_col = resolve_employee_col(cols)
    total = 0
    for csv_path in parsed.csvs:
        total += process_csv(csv_path, cols, employee_col)
    print(f"Imported/Merged {total} rows into companies (dataset={parsed.dataset}).")


if __name__ == "__main__":
    main(sys.argv[1:])
