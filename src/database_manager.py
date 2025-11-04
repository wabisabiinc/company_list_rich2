# src/database_manager.py
import csv
import os
import sqlite3
import time
from typing import Optional, Dict, Any


class DatabaseManager:
    """
    - 並列安全なジョブ確保: claim_next_company(worker_id)
    - 互換性: csv_path をオプションでサポート（テスト/デバッグ用）
    - 安定化:
        * DB初期化（スキーマ作成/索引作成）時の "database is locked" をリトライで回避
        * PRAGMA busy_timeout を 60s に設定
        * WAL + synchronous=NORMAL
        * 古い running を TTL で自動回収（RUNNING_TTL_MIN, 既定30分）
    """
    def __init__(self, db_path: str = "data/companies.db", csv_path: Optional[str] = None):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        if csv_path:
            os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)

        # autocommit（isolation_level=None）+ 長めの timeout
        self.conn = sqlite3.connect(
            db_path,
            timeout=60,              # 以前: 30
            isolation_level=None,    # autocommit
            check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

        # PRAGMA は接続毎に適用
        self._configure_pragmas()

        self.csv_path = csv_path
        self.running_ttl_min = int(os.getenv("RUNNING_TTL_MIN", "30"))

        # ★ 初期化はロック競合が起きやすいので安全にリトライ
        self._ensure_schema_with_retry()

        # CSV の重複書き出し防止用キャッシュ
        self._init_csv_state()

    # ---------- PRAGMA ----------
    def _configure_pragmas(self) -> None:
        # WAL & 同期緩和（性能） / busy_timeout（ロック待ち）
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=60000;")  # 60秒

    # ---------- 初期化（ロックに強いリトライ付） ----------
    def _ensure_schema_with_retry(self, max_retry_sec: int = 20) -> None:
        start = time.time()
        while True:
            try:
                self._ensure_schema()
                return
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if ("locked" in msg or "busy" in msg) and (time.time() - start < max_retry_sec):
                    time.sleep(1.0)
                    continue
                raise

    def _ensure_schema(self) -> None:
        # companies テーブル（不足カラムは後で確認・追加）
        self.cur.execute(
            """
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
                locked_at      TEXT
            )
            """
        )

        # 不足カラムを安全に追加（存在確認してから）
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(companies)")}
        if "status" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN status TEXT DEFAULT 'pending';")
        if "locked_by" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN locked_by TEXT;")
        if "locked_at" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN locked_at TEXT;")

        # 索引
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_name_addr ON companies(company_name, address);"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_emp ON companies(employee_count);")

        self.conn.commit()

    # ---------- CSV 互換 ----------
    def _init_csv_state(self) -> None:
        self.csv_header_written = False
        self._csv_written_ids: set[int] = set()
        if not self.csv_path:
            return
        if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
            try:
                with open(self.csv_path, newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            cid = int((row.get("id") or "").strip())
                            self._csv_written_ids.add(cid)
                        except Exception:
                            continue
                self.csv_header_written = True
            except FileNotFoundError:
                self.csv_header_written = False
                self._csv_written_ids.clear()

    # ---------- 無効データ判定 ----------
    def _is_valid_company_data(self, data: Dict[str, Any]) -> bool:
        invalid_keywords = [
            "https://listoss.com/", "listoss.com",
            "ftj-g.co.jp/form",
            "本法人データはリストスが提供しています",
            "御社のテレアポ代行します",
            "1件10円!お問い合わせフォーム送信代行。"
        ]
        for v in data.values():
            if isinstance(v, str):
                low = v.lower()
                for kw in invalid_keywords:
                    if kw.lower() in low:
                        return False
        return True

    # ---------- 並列安全な1件確保 ----------
    def claim_next_company(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        1) 古い running を TTL で pending に戻す
        2) pending の先頭1件を running にし、locked_by/locked_at を付与
        3) そのレコードを返す
        """
        cur = self.conn.cursor()
        # IMMEDIATE: 直ちに RESERVED ロック（書込予約）を取り、競合を避ける
        cur.execute("BEGIN IMMEDIATE;")
        try:
            # ★ TTL 回収（locked_at が一定以上古い running を pending に戻す）
            cur.execute(
                "UPDATE companies SET status='pending', locked_by=NULL, locked_at=NULL "
                "WHERE status='running' AND locked_at IS NOT NULL AND "
                "locked_at < datetime('now', ?)",
                (f"-{self.running_ttl_min} minutes",),
            )

            # RETURNING が利用可能な SQLite ならこちらを使用
            try:
                row = cur.execute(
                    """
                    WITH picked AS (
                      SELECT id FROM companies
                       WHERE status='pending'
                       ORDER BY employee_count DESC, id ASC
                       LIMIT 1
                    )
                    UPDATE companies
                       SET status='running',
                           locked_by=?,
                           locked_at=datetime('now')
                     WHERE id=(SELECT id FROM picked)
                    RETURNING id, company_name, address, employee_count, homepage, phone, found_address, status
                    """,
                    (worker_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                # 古い SQLite 向けフォールバック（RETURNING 無し）
                cur.execute(
                    """
                    UPDATE companies
                       SET status='running',
                           locked_by=?,
                           locked_at=datetime('now')
                     WHERE id = (
                       SELECT id FROM companies
                        WHERE status='pending'
                        ORDER BY employee_count DESC, id ASC
                        LIMIT 1
                     ) AND status='pending'
                    """,
                    (worker_id,),
                )
                if cur.rowcount == 0:
                    self.conn.commit()
                    return None
                row = cur.execute(
                    """
                    SELECT id, company_name, address, employee_count, homepage, phone, found_address, status
                      FROM companies
                     WHERE locked_by=? AND status='running'
                     ORDER BY locked_at DESC, id DESC
                     LIMIT 1
                    """,
                    (worker_id,),
                ).fetchone()

            self.conn.commit()
            return dict(row) if row else None
        except Exception:
            self.conn.rollback()
            raise

    # ---------- 単発互換 ----------
    def get_next_company(self) -> Optional[Dict[str, Any]]:
        self.cur.execute(
            """
            SELECT * FROM companies
             WHERE status='pending'
             ORDER BY employee_count DESC, id ASC
             LIMIT 1
            """
        )
        row = self.cur.fetchone()
        return dict(row) if row else None

    # ---------- 書き込み ----------
    def save_company_data(self, company: Dict[str, Any], status: str = "done") -> None:
        self.cur.execute(
            """
            UPDATE companies
               SET homepage      = ?,
                   phone         = ?,
                   found_address = ?,
                   status        = ?,
                   locked_by     = NULL,
                   locked_at     = NULL
              WHERE id = ?
            """,
            (
                company.get("homepage", "") or "",
                company.get("phone", "") or "",
                company.get("found_address", "") or "",
                status,
                company["id"],
            ),
        )
        self.conn.commit()

        # 任意：CSVミラー（指定時のみ）
        if not self.csv_path:
            return
        cid = int(company["id"])
        if cid in self._csv_written_ids:
            return

        fieldnames = ["id", "company_name", "address", "employee_count", "homepage", "phone", "found_address"]
        write_header = not self.csv_header_written

        with open(self.csv_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
                self.csv_header_written = True
            writer.writerow(
                {
                    "id": company.get("id", ""),
                    "company_name": company.get("company_name", ""),
                    "address": company.get("address", ""),
                    "employee_count": company.get("employee_count", ""),
                    "homepage": company.get("homepage", ""),
                    "phone": company.get("phone", ""),
                    "found_address": company.get("found_address", ""),
                }
            )

        self._csv_written_ids.add(cid)

    def update_status(self, company_id: int, status: str) -> None:
        self.cur.execute(
            "UPDATE companies SET status=?, locked_by=NULL, locked_at=NULL WHERE id=?",
            (status, company_id),
        )
        self.conn.commit()

    def insert_company(self, company_data: Dict[str, Any]) -> None:
        if not self._is_valid_company_data(company_data):
            return
        self.cur.execute(
            """
            INSERT INTO companies (company_name, address, employee_count, homepage, phone, found_address)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                company_data["company_name"],
                company_data["address"],
                company_data.get("employee_count"),
                (company_data.get("homepage") or ""),
                (company_data.get("phone") or "").replace("-", "").replace(" ", ""),
                company_data.get("found_address", ""),
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.cur.close()
        finally:
            self.conn.close()
