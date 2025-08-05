import csv
import sqlite3
import os

DB_PATH = "data/companies.db"
CSV_PATH = "data/input_companies.csv"

os.makedirs("data", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# companiesテーブルがなければ自動作成
cur.execute("""
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY,
    company_name TEXT,
    address TEXT,
    employee_count INTEGER,
    homepage TEXT,
    phone TEXT,
    found_address TEXT,
    status TEXT DEFAULT 'pending'
)
""")
conn.commit()

# CSV→DBインポート（id重複時はスキップ）
with open(CSV_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT OR IGNORE INTO companies (id, company_name, address, employee_count)
            VALUES (?, ?, ?, ?)
        """, (row['id'], row['company_name'], row['address'], row['employee_count']))

conn.commit()
conn.close()
print("CSVインポート完了")
