import sqlite3

class DatabaseManager:
    def __init__(self, db_path='data/companies.db'):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()
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

    def get_next_company(self):
        self.cur.execute("SELECT * FROM companies WHERE status='pending' ORDER BY id LIMIT 1")
        row = self.cur.fetchone()
        if row:
            keys = [d[0] for d in self.cur.description]
            return dict(zip(keys, row))
        return None

    def save_company_data(self, company):
        self.cur.execute("""
            UPDATE companies
            SET homepage=?, phone=?, found_address=?, status='done'
            WHERE id=?
        """, (company['homepage'], company['phone'], company['found_address'], company['id']))
        self.conn.commit()

    def update_status(self, company_id, status):
        self.cur.execute("UPDATE companies SET status=? WHERE id=?", (status, company_id))
        self.conn.commit()

    def close(self):
        self.conn.close()
