import csv, sys, os, re, sqlite3

DB_PATH = os.environ.get("COMPANIES_DB_PATH", "data/companies.db")
HOMEPAGE_KEYS = ("ホームページ", "HP", "Webサイト", "website")
REP_KEYS = ("代表者名", "代表取締役", "代表者")
DESC_KEYS = ("説明文", "会社説明", "備考")
INDUSTRY_KEYS = ("業種", "業界")
PHONE_SOURCE_VALUE = "hubspot"
ADDRESS_SOURCE_VALUE = "hubspot"


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
    s = re.sub(r"[^0-9]", "", str(s))
    return int(s) if s else None


def first(*vals):
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None


def clean(val):
    if val is None:
        return None
    val = str(val).strip()
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
    description = first(*(row.get(key) for key in DESC_KEYS))
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
    if not args:
        print("Usage: python scripts/import_hubspot_companies_into_existing.py <csv> [<csv> ...]")
        sys.exit(1)
    cols = existing_cols()
    if not cols:
        print("companies table not found in database.")
        sys.exit(3)
    employee_col = resolve_employee_col(cols)
    total = 0
    for csv_path in args:
        total += process_csv(csv_path, cols, employee_col)
    print(f"Imported/Merged {total} rows into companies (safe).")


if __name__ == "__main__":
    main(sys.argv[1:])
