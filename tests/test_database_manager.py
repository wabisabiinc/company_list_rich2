import os
import csv
from pathlib import Path
import pytest
from src.database_manager import DatabaseManager

# 追加：テスト用に初期データを挿入する
def _insert_company_rows(dbm: DatabaseManager):
    dbm.cur.executemany(
        """
        INSERT INTO companies (id, company_name, address, employee_count, status)
        VALUES (?, ?, ?, ?, 'pending')
        """,
        [
            (1, "小会社A", "東京都渋谷区", 50),
            (2, "大会社B", "東京都港区", 500),
            (3, "中会社C", "大阪府大阪市", 120),
        ],
    )
    dbm.conn.commit()

# テストのために一時的なデータベースを生成する
@pytest.fixture
def db_manager(tmp_path: Path):
    db_path = str(tmp_path / "test.db")
    csv_path = str(tmp_path / "out.csv")
    dbm = DatabaseManager(db_path=db_path, csv_path=csv_path)
    yield dbm
    dbm.close()  # テスト後に接続をクローズ

# 有効なデータの挿入テスト
def test_insert_company_valid_data(db_manager: DatabaseManager):
    valid_company = {
        "company_name": "有効会社",
        "address": "東京都渋谷区",
        "employee_count": 50,
        "homepage": "https://example.co.jp",
        "phone": "03-1234-5678",
        "found_address": "東京都港区",
    }
    
    invalid_company = {
        "company_name": "無効会社",
        "address": "東京都渋谷区",
        "employee_count": 50,
        "homepage": "https://listoss.com/",
        "phone": "03-1234-5678",
        "found_address": "東京都港区",
    }

    # 有効な会社データの挿入
    db_manager.insert_company(valid_company)
    valid_data = db_manager.cur.execute("SELECT * FROM companies WHERE company_name=?", ("有効会社",)).fetchone()
    assert valid_data is not None

    # 無効な会社データの挿入
    db_manager.insert_company(invalid_company)
    invalid_data = db_manager.cur.execute("SELECT * FROM companies WHERE company_name=?", ("無効会社",)).fetchone()
    assert invalid_data is None

# 次の会社を従業員数の多い順に取得するテスト
def test_get_next_company_picks_highest_employee(db_manager: DatabaseManager):
    _insert_company_rows(db_manager)
    c = db_manager.get_next_company()  # 従業員数が最も多い「大会社B」が返る
    assert c["company_name"] == "大会社B"
    
    db_manager.update_status(c["id"], "processing")
    c2 = db_manager.get_next_company()  # 次の「中会社C」または「小会社A」
    assert c2["company_name"] in ("中会社C", "小会社A")

# 会社データを更新し、CSVに出力されることを確認
def test_save_company_data_updates_row_and_writes_csv(db_manager: DatabaseManager, tmp_path: Path):
    _insert_company_rows(db_manager)
    
    c = db_manager.get_next_company()
    c.update({"homepage": "https://example.co.jp", "phone": "03-1234-5678", "found_address": "東京都港区"})
    db_manager.save_company_data(c, status="review")

    # DBが更新されていることを確認
    row = db_manager.cur.execute("SELECT homepage, phone, found_address, status FROM companies WHERE id=?", (c["id"],)).fetchone()
    assert row["homepage"] == "https://example.co.jp"
    assert row["phone"] == "03-1234-5678"
    assert row["found_address"].startswith("東京都")
    assert row["status"] == "review"

    # CSVファイルが生成され、1行が追記されていることを確認
    csv_path = str(tmp_path / "out.csv")
    assert os.path.exists(csv_path)
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1  # 1行が追加されていることを確認
        assert reader[0]["homepage"] == "https://example.co.jp"

    # 同じIDで再度saveしてもCSVが増えないことを確認
    db_manager.save_company_data(c, status="done")
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1  # CSVには再度書き込まれないことを確認

