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


def test_address_not_overwritten_on_pref_mismatch_without_strong_evidence(tmp_path):
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
                "homepage_official_flag": 1,
                "homepage_official_score": 5.0,
                "address_source": "ai",
                "found_address": "大阪府大阪市北区1-1-1",
                "phone": "06-1234-5678",
                "source_url_address": "https://example.co.jp/company/overview",
                "source_url_phone": "https://example.co.jp/company/overview",
                "address_confidence": 0.95,
                "address_evidence": "",  # HQ根拠がない
            }
        )

        manager.save_company_data(company, status="done")

        updated = _fetch_one(db_path, "SELECT address, found_address, status, address_conflict_level, address_review_reason FROM companies WHERE id=1")
        assert updated["address"] == "東京都中央区1-1-1"
        assert "大阪府" in (updated["found_address"] or "")
        assert updated["status"] == "review"
        assert updated["address_conflict_level"] == "pref_mismatch"
        assert updated["address_review_reason"] == "pref_mismatch_no_strong_hq_evidence"
    finally:
        manager.close()


def test_address_overwritten_on_pref_mismatch_with_strong_hq_evidence(tmp_path):
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
                "homepage_official_flag": 1,
                "homepage_official_score": 5.0,
                "address_source": "ai",
                "found_address": "大阪府大阪市北区1-1-1",
                "phone": "06-1234-5678",
                "source_url_address": "https://example.co.jp/company/overview",
                "source_url_phone": "https://example.co.jp/company/overview",
                "address_confidence": 0.95,
                "address_evidence": "本社所在地：大阪府大阪市北区1-1-1 / 代表電話：06-1234-5678",
            }
        )

        manager.save_company_data(company, status="done")

        updated = _fetch_one(db_path, "SELECT address, status, address_conflict_level, address_review_reason FROM companies WHERE id=1")
        assert "大阪府" in (updated["address"] or "")
        assert updated["status"] == "done"
        assert updated["address_conflict_level"] == "pref_mismatch_overwritten"
        assert updated["address_review_reason"] in ("", None)
    finally:
        manager.close()


def test_address_not_overwritten_when_found_missing_prefecture(tmp_path):
    db_path = str(tmp_path / "t.db")
    manager = DatabaseManager(db_path=db_path, worker_id=None)
    try:
        manager.conn.execute(
            "INSERT INTO companies (id, company_name, address, status) VALUES (?,?,?,?)",
            (1, "テスト株式会社", "神奈川県川崎市川崎区榎町5番14号", "pending"),
        )
        manager.conn.commit()

        company = _fetch_one(db_path, "SELECT * FROM companies WHERE id=1")
        assert company is not None
        company.update(
            {
                "homepage_official_flag": 1,
                "homepage_official_score": 5.0,
                "address_source": "rule",
                # 都道府県を欠いた住所（ZIP+市区町村のみ）
                "found_address": "〒210-0002 川崎市川崎区榎町5番14号",
            }
        )

        manager.save_company_data(company, status="done")

        updated = _fetch_one(db_path, "SELECT address, found_address, status FROM companies WHERE id=1")
        assert updated["address"] == "神奈川県川崎市川崎区榎町5番14号"
        assert "〒210-0002" in (updated["found_address"] or "")
        assert updated["status"] in ("done", "review")
    finally:
        manager.close()
