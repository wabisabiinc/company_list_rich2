import sqlite3

from src.database_manager import DatabaseManager


def _fetch_one(db_path: str, sql: str, params=()):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def test_save_company_data_cleans_description_listing_and_amounts(tmp_path):
    db_path = str(tmp_path / "t.db")
    manager = DatabaseManager(db_path=db_path, worker_id=None)
    try:
        manager.conn.execute(
            "INSERT INTO companies (id, company_name, address, status) VALUES (?,?,?,?)",
            (1, "テスト株式会社", "東京都中央区1-1-1", "pending"),
        )
        manager.conn.commit()

        company = _fetch_one(db_path, "SELECT * FROM companies WHERE id=1")
        assert company is not None

        company.update(
            {
                "description": "運送・物流企業の総合データベースサイト、掲載企業詳細ページです。",
                "listing": "上場（東証）です。",  # 句読点混入は不正
                "revenue": "従業員200名",  # 金額ではない
                "profit": "1.2億円（2024年）",
                "capital": "3,000万円",
                "fiscal_month": "Q4",
                "founded_year": "創業: 1998年",
            }
        )

        manager.save_company_data(company, status="done")

        updated = _fetch_one(
            db_path,
            "SELECT description, listing, revenue, profit, capital, fiscal_month, founded_year FROM companies WHERE id=1",
        )
        assert updated is not None
        assert updated["description"] == ""
        assert updated["listing"] == ""
        assert updated["revenue"] == ""
        assert updated["profit"] == "1.2億円"
        assert updated["capital"] == "3,000万円"
        assert updated["fiscal_month"] == "12月"
        assert updated["founded_year"] == "1998"
    finally:
        manager.close()

