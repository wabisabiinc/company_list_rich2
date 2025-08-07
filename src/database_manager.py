import sqlite3
import csv
import os

class DatabaseManager:
    def __init__(self, db_path='data/companies.db', csv_path='data/output.csv'):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
        self.csv_path = csv_path

        # companiesテーブルがなければ自動作成
        self.cur.execute("""
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
        self.conn.commit()

        # CSVヘッダーの有無チェック
        self.csv_header_written = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    def get_next_company(self):
        self.cur.execute("SELECT * FROM companies WHERE status='pending' ORDER BY id LIMIT 1")
        row = self.cur.fetchone()
        if row:
            keys = [d[0] for d in self.cur.description]
            return dict(zip(keys, row))
        return None

    def save_company_data(self, company):
        # DB更新
        self.cur.execute("""
            UPDATE companies
            SET homepage=?, phone=?, found_address=?, status='done'
            WHERE id=?
        """, (company['homepage'], company['phone'], company['found_address'], company['id']))
        self.conn.commit()

        # CSVにも同時出力
        fieldnames = [
            "id", "company_name", "address", "employee_count", "homepage", "phone", "found_address"
        ]
        write_header = not os.path.exists(self.csv_path) or not self.csv_header_written
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
                self.csv_header_written = True
            # 必要なフィールドだけ出力
            writer.writerow({k: company.get(k, "") for k in fieldnames})

    def update_status(self, company_id, status):
        self.cur.execute("UPDATE companies SET status=? WHERE id=?", (status, company_id))
        self.conn.commit()

    def close(self):
        self.conn.close()
