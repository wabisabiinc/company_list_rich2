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


def test_timeout_stage_column_is_saved(tmp_path):
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
        company.update({"timeout_stage": "deep"})
        manager.save_company_data(company, status="review")

        updated = _fetch_one(db_path, "SELECT timeout_stage FROM companies WHERE id=1")
        assert (updated or {}).get("timeout_stage") == "deep"
    finally:
        manager.close()

