# src/database_manager.py
import csv
import os
import sqlite3
import time
import urllib.parse
import re
import logging
from typing import Iterable, Optional, Dict, Any

log = logging.getLogger(__name__)


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
    def __init__(self, db_path: str = "data/companies.db", csv_path: Optional[str] = None, claim_order: Optional[str] = None, worker_id: Optional[str] = None):
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
        self.worker_id = worker_id

        # PRAGMA は接続毎に適用
        self._configure_pragmas()

        self.csv_path = csv_path
        self.running_ttl_min = int(os.getenv("RUNNING_TTL_MIN", "30"))
        # 優先度: 引数 > 環境変数 > デフォルト
        self.claim_order = (claim_order or os.getenv("CLAIM_ORDER") or "employee_desc_id_asc").lower()
        self._claim_order_clause = self._build_claim_order_clause()
        self.wal_checkpoint_interval = max(0, int(os.getenv("WAL_CHECKPOINT_INTERVAL", "200")))
        self._writes_since_checkpoint = 0
        retry_statuses_env = os.getenv("RETRY_STATUSES", "review,no_homepage")
        self.retry_statuses: list[str] = [s.strip() for s in retry_statuses_env.split(",") if s.strip()]
        self._schema_columns: set[str] = set()
        self.lock_mismatch_count = 0

        # ★ 初期化はロック競合が起きやすいので安全にリトライ
        self._ensure_schema_with_retry()
        self._ensure_indexes()
        self._refresh_schema_columns()

        # CSV の重複書き出し防止用キャッシュ
        self._init_csv_state()

    def _build_claim_order_clause(self) -> str:
        """
        取得順序を環境変数CLAIM_ORDERで切り替える。
        - employee_desc_id_asc (default)
        - id_asc
        - id_desc
        - random
        """
        mapping = {
            "employee_desc_id_asc": "ORDER BY COALESCE(employee_count, 0) DESC, id ASC",
            "id_asc": "ORDER BY id ASC",
            "id_desc": "ORDER BY id DESC",
            "random": "ORDER BY RANDOM()",
        }
        return mapping.get(self.claim_order, mapping["employee_desc_id_asc"])

    def _commit_with_checkpoint(self) -> None:
        self.conn.commit()
        self._maybe_checkpoint()

    def _maybe_checkpoint(self) -> None:
        if self.wal_checkpoint_interval <= 0:
            return
        self._writes_since_checkpoint += 1
        if self._writes_since_checkpoint >= self.wal_checkpoint_interval:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            except sqlite3.DatabaseError:
                pass
            else:
                self._writes_since_checkpoint = 0

    # ---------- PRAGMA ----------
    def _configure_pragmas(self) -> None:
        # WAL & 同期緩和（性能） / busy_timeout（ロック待ち）
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA busy_timeout=60000;")  # 60秒

    def _get_table_columns(self) -> set[str]:
        return {r[1] for r in self.conn.execute("PRAGMA table_info(companies)")}

    def _refresh_schema_columns(self) -> None:
        self._schema_columns = self._get_table_columns()

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
                error_code     TEXT
            )
            """
        )

        cols = self._get_table_columns()
        # 既存データベースとの互換を保つため、後から増えた列を順に追加する
        if "status" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN status TEXT DEFAULT 'pending';")
            cols.add("status")
        if "locked_by" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN locked_by TEXT;")
            cols.add("locked_by")
        if "locked_at" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN locked_at TEXT;")
            cols.add("locked_at")
        if "rep_name" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN rep_name TEXT;")
            cols.add("rep_name")
        if "description" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN description TEXT;")
            cols.add("description")
        if "listing" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN listing TEXT;")
            cols.add("listing")
        if "revenue" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN revenue TEXT;")
            cols.add("revenue")
        if "profit" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN profit TEXT;")
            cols.add("profit")
        if "capital" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN capital TEXT;")
            cols.add("capital")
        if "fiscal_month" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN fiscal_month TEXT;")
            cols.add("fiscal_month")
        if "founded_year" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN founded_year TEXT;")
            cols.add("founded_year")
        if "ai_used" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN ai_used INTEGER DEFAULT 0;")
            cols.add("ai_used")
        if "ai_model" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN ai_model TEXT;")
            cols.add("ai_model")
        if "hubspot_id" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN hubspot_id TEXT;")
            cols.add("hubspot_id")
        if "corporate_number" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN corporate_number TEXT;")
            cols.add("corporate_number")
        if "corporate_number_norm" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN corporate_number_norm TEXT;")
            cols.add("corporate_number_norm")
        if "source_csv" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN source_csv TEXT;")
            cols.add("source_csv")
        if "phone_source" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN phone_source TEXT;")
            cols.add("phone_source")
        if "address_source" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN address_source TEXT;")
            cols.add("address_source")
        if "extract_confidence" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN extract_confidence REAL;")
            cols.add("extract_confidence")
        if "last_checked_at" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN last_checked_at TEXT;")
            cols.add("last_checked_at")
        if "reference_homepage" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN reference_homepage TEXT;")
            cols.add("reference_homepage")
        if "reference_phone" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN reference_phone TEXT;")
            cols.add("reference_phone")
        if "reference_address" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN reference_address TEXT;")
            cols.add("reference_address")
        if "accuracy_homepage" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN accuracy_homepage TEXT;")
            cols.add("accuracy_homepage")
        if "accuracy_phone" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN accuracy_phone TEXT;")
            cols.add("accuracy_phone")
        if "accuracy_address" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN accuracy_address TEXT;")
            cols.add("accuracy_address")

    def _ensure_indexes(self) -> None:
        cols = self._get_table_columns()
        try:
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_status_locked_at ON companies(status, locked_at);"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_companies_locked_by ON companies(locked_by);"
            )
        except sqlite3.DatabaseError:
            # 古いSQLiteなどで失敗しても致命ではない
            pass
        if "homepage_official_flag" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN homepage_official_flag INTEGER;")
            cols.add("homepage_official_flag")
        if "homepage_official_source" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN homepage_official_source TEXT;")
            cols.add("homepage_official_source")
        if "homepage_official_score" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN homepage_official_score REAL;")
            cols.add("homepage_official_score")
        if "source_url_phone" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN source_url_phone TEXT;")
            cols.add("source_url_phone")
        if "source_url_address" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN source_url_address TEXT;")
            cols.add("source_url_address")
        if "source_url_rep" not in cols:
            self.conn.execute("ALTER TABLE companies ADD COLUMN source_url_rep TEXT;")
            cols.add("source_url_rep")

        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_name_addr ON companies(company_name, address);"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_emp ON companies(employee_count);")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_companies_corporate_number_norm ON companies(corporate_number_norm);"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_hubspot_id ON companies(hubspot_id);")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS url_flags (
                normalized_value TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                is_official INTEGER NOT NULL,
                host TEXT,
                judge_source TEXT,
                reason TEXT,
                confidence REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_url_flags_scope_host ON url_flags(scope, host)"
        )

        self._schema_columns = cols

        self._commit_with_checkpoint()

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

    @staticmethod
    def _normalize_flag_target(url: str) -> tuple[str, str]:
        if not url:
            return "", ""
        try:
            parsed = urllib.parse.urlparse(url.strip())
        except Exception:
            return "", ""
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if ":" in host:
            host = host.split(":")[0]
        if not host:
            return "", ""
        scheme = parsed.scheme or "https"
        path = parsed.path or "/"
        # index.* -> /
        if path.lower().endswith(("/index.html", "/index.htm", "/index.php", "/index.asp")):
            path = "/"
        normalized_path = path if path == "/" else path.rstrip("/")
        normalized_url = urllib.parse.urlunparse((scheme, host, normalized_path or "/", "", "", ""))
        return normalized_url, host

    def _query_url_flags(self, scope: str, values: list[str]) -> Dict[str, Dict[str, Any]]:
        if not values:
            return {}
        placeholders = ",".join("?" for _ in values)
        rows = self.conn.execute(
            f"SELECT normalized_value, scope, is_official, judge_source, reason, confidence, host "
            f"FROM url_flags WHERE scope=? AND normalized_value IN ({placeholders})",
            (scope, *values),
        ).fetchall()
        return {row["normalized_value"]: dict(row) for row in rows}

    def get_url_flags_batch(self, urls: Iterable[str]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        normalized_urls: list[str] = []
        hosts: list[str] = []
        seen_urls: set[str] = set()
        seen_hosts: set[str] = set()
        for url in urls:
            normalized, host = self._normalize_flag_target(url)
            if normalized and normalized not in seen_urls:
                seen_urls.add(normalized)
                normalized_urls.append(normalized)
            if host and host not in seen_hosts:
                seen_hosts.add(host)
                hosts.append(host)
        url_flags = self._query_url_flags("url", normalized_urls)
        host_flags = self._query_url_flags("host", hosts)
        return url_flags, host_flags

    def get_url_flag(self, url: str) -> Optional[Dict[str, Any]]:
        normalized_url, host = self._normalize_flag_target(url)
        if not host:
            return None
        row = self.conn.execute(
            "SELECT normalized_value, scope, is_official, judge_source, reason, confidence FROM url_flags WHERE scope='url' AND normalized_value=?",
            (normalized_url,)
        ).fetchone()
        if row:
            return dict(row)
        row = self.conn.execute(
            "SELECT normalized_value, scope, is_official, judge_source, reason, confidence FROM url_flags WHERE scope='host' AND normalized_value=?",
            (host,)
        ).fetchone()
        return dict(row) if row else None

    def upsert_url_flag(
        self,
        url: str,
        *,
        is_official: bool,
        source: str,
        reason: str = "",
        confidence: Optional[float] = None,
        scope: str = "url",
    ) -> None:
        normalized_url, host = self._normalize_flag_target(url)
        if not host:
            return
        scope_value = scope if scope in {"url", "host"} else "url"
        normalized_value = host if scope_value == "host" else normalized_url
        if not normalized_value:
            return
        self.conn.execute(
            """
            INSERT INTO url_flags (normalized_value, scope, is_official, host, judge_source, reason, confidence, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(normalized_value) DO UPDATE SET
                scope=excluded.scope,
                is_official=excluded.is_official,
                host=excluded.host,
                judge_source=excluded.judge_source,
                reason=excluded.reason,
                confidence=excluded.confidence,
                updated_at=datetime('now')
            """,
            (normalized_value, scope_value, int(bool(is_official)), host, source, reason or "", confidence),
        )
        self._commit_with_checkpoint()

    # ---------- 並列安全な1件確保 ----------
    def claim_next_company(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """
        1) 古い running を TTL で pending に戻す
        2) pending を優先して1件確保。pending が無ければ retry_statuses（例: review,no_homepage）から順に1件確保
        3) running にし、locked_by/locked_at を付与して返す
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
                row = None
                statuses = ["pending"] + self.retry_statuses
                for st in statuses:
                    row = cur.execute(
                        f"""
                        WITH picked AS (
                          SELECT id FROM companies
                           WHERE status=?
                           {self._claim_order_clause}
                           LIMIT 1
                        )
                        UPDATE companies
                           SET status='running',
                               locked_by=?,
                               locked_at=datetime('now')
                         WHERE id=(SELECT id FROM picked)
                        RETURNING id, company_name, address, employee_count, homepage, phone, found_address, status, corporate_number, corporate_number_norm
                        """,
                        (st, worker_id),
                    ).fetchone()
                    if row:
                        break
            except sqlite3.OperationalError:
                # 古い SQLite 向けフォールバック（RETURNING 無し）
                row = None
                statuses = ["pending"] + self.retry_statuses
                for st in statuses:
                    cur.execute(
                        f"""
                        UPDATE companies
                           SET status='running',
                               locked_by=?,
                               locked_at=datetime('now')
                         WHERE id = (
                           SELECT id FROM companies
                            WHERE status=?
                            {self._claim_order_clause}
                            LIMIT 1
                         ) AND status=?
                        """,
                        (worker_id, st, st),
                    )
                    if cur.rowcount > 0:
                        row = cur.execute(
                            """
                            SELECT id, company_name, address, employee_count, homepage, phone, found_address, status, corporate_number, corporate_number_norm
                              FROM companies
                             WHERE locked_by=? AND status='running'
                             ORDER BY locked_at DESC, id DESC
                             LIMIT 1
                            """,
                            (worker_id,),
                        ).fetchone()
                        break

            self._commit_with_checkpoint()
            return dict(row) if row else None
        except Exception:
            self.conn.rollback()
            raise

    # ---------- 単発互換 ----------
    def get_next_company(self) -> Optional[Dict[str, Any]]:
        self.cur.execute(
            f"""
            SELECT * FROM companies
             WHERE status='pending'
             {self._claim_order_clause}
             LIMIT 1
            """
        )
        row = self.cur.fetchone()
        return dict(row) if row else None

    # ---------- 書き込み ----------
    def save_company_data(self, company: Dict[str, Any], status: str = "done") -> None:
        cols = self._schema_columns
        updates: list[str] = []
        params: list[Any] = []

        def _clean_phone(raw: Any) -> str:
            s = (raw or "").strip()
            hyphen_match = re.search(
                r"(0\d{1,4})\D+?(\d{1,4})\D+?(\d{3,4})",
                s,
            )
            if hyphen_match:
                return f"{hyphen_match.group(1)}-{hyphen_match.group(2)}-{hyphen_match.group(3)}"
            digits = re.sub(r"\D", "", s)
            if digits.startswith("81") and len(digits) >= 11:
                digits = "0" + digits[2:]
            if digits in {"81112345678", "0123456789"}:
                return ""
            if not digits.startswith("0") or len(digits) not in (10, 11):
                return ""
            m = re.search(r"^(0\d{1,4})(\d{2,4})(\d{3,4})$", digits)
            if not m:
                return ""
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        def _clean_address(raw: Any) -> str:
            s = (raw or "").strip()
            if not s:
                return ""
            s = s.replace("　", " ")
            s = re.sub(r"<[^>]+>", " ", s)
            s = re.sub(r"[\x00-\x1f\x7f]", " ", s)
            # 住所入力フォームのラベル/候補一覧が混入した場合は破棄
            if re.search(r"(住所検索|都道府県|市区町村|マンション・?ビル名|郵便番号\\s*[（(]?\s*半角)", s, flags=re.I):
                return ""
            if ("郵便番号" in s) and not re.search(r"\d{3}-\d{4}", s):
                return ""
            contact_pattern = re.compile(r"(TEL|電話|☎|℡|FAX|ファックス|メール|E[-\s]?mail)", re.IGNORECASE)
            contact_match = contact_pattern.search(s)
            if contact_match:
                s = s[: contact_match.start()]
            map_match = re.search(r"(地図アプリ|地図で見る|マップ|Google\s*マップ|アクセス|ルート|Route|Directions|行き方)", s, flags=re.IGNORECASE)
            if map_match:
                s = s[: map_match.start()]
            arrow_idx = min([idx for idx in (s.find("→"), s.find("⇒")) if idx >= 0], default=-1)
            if arrow_idx >= 0:
                s = s[:arrow_idx]
            s = s.translate(str.maketrans("０１２３４５６７８９－ー―‐／", "0123456789----/"))
            s = re.sub(r"\s+", " ", s)
            s = s.replace("〒 ", "〒").replace("〒", "〒")
            if re.search(r"[ãÂ�]{2,}", s):
                return ""
            return s.strip()

        def set_value(column: str, value: Any) -> None:
            if column in cols:
                updates.append(f"{column} = ?")
                params.append(value)

        raw_address = (company.get("address") or "").strip()
        set_value("homepage", company.get("homepage", "") or "")
        cleaned_phone = _clean_phone(company.get("phone"))
        set_value("phone", cleaned_phone)
        found_addr_clean = _clean_address(company.get("found_address"))
        set_value("found_address", found_addr_clean)
        final_address = raw_address
        found_addr = found_addr_clean or ""
        if found_addr:
            strong_official = bool(
                (company.get("homepage_official_flag") == 1)
                and float(company.get("homepage_official_score") or 0.0) >= 4.0
            )
            addr_source = (company.get("address_source") or "").lower()
            if strong_official or addr_source in {"official", "rule"}:
                has_zip = bool(re.search(r"\d{3}-\d{4}", found_addr))
                has_city = bool(re.search(r"(\u5e02|\u533a|\u753a|\u6751|\u90e1)", found_addr))
                raw_has_zip = bool(re.search(r"\d{3}-\d{4}", raw_address))
                raw_has_city = bool(re.search(r"(\u5e02|\u533a|\u753a|\u6751|\u90e1)", raw_address))
                if (
                    not raw_address
                    or len(found_addr) > len(raw_address)
                    or (has_zip and not raw_has_zip)
                    or (has_city and not raw_has_city)
                ):
                    final_address = found_addr
                    company["address"] = final_address
        set_value("address", final_address)
        set_value("rep_name", company.get("rep_name", "") or "")
        set_value("description", company.get("description", "") or "")
        set_value("ai_used", int(company.get("ai_used", 0) or 0))
        set_value("ai_model", company.get("ai_model", "") or "")
        set_value("phone_source", company.get("phone_source", "") or "")
        set_value("address_source", company.get("address_source", "") or "")
        set_value("extract_confidence", company.get("extract_confidence"))
        set_value("source_url_phone", company.get("source_url_phone", "") or "")
        set_value("source_url_address", company.get("source_url_address", "") or "")
        set_value("source_url_rep", company.get("source_url_rep", "") or "")
        set_value("error_code", company.get("error_code", "") or "")
        set_value("listing", company.get("listing", "") or "")
        set_value("revenue", company.get("revenue", "") or "")
        set_value("profit", company.get("profit", "") or "")
        set_value("capital", company.get("capital", "") or "")
        set_value("fiscal_month", company.get("fiscal_month", "") or "")
        set_value("founded_year", company.get("founded_year", "") or "")
        set_value("reference_homepage", company.get("reference_homepage", "") or "")
        set_value("reference_phone", company.get("reference_phone", "") or "")
        set_value("reference_address", company.get("reference_address", "") or "")
        set_value("accuracy_homepage", company.get("accuracy_homepage", "") or "")
        set_value("accuracy_phone", company.get("accuracy_phone", "") or "")
        set_value("accuracy_address", company.get("accuracy_address", "") or "")
        set_value("homepage_official_flag", company.get("homepage_official_flag"))
        set_value("homepage_official_source", company.get("homepage_official_source", "") or "")
        set_value("homepage_official_score", company.get("homepage_official_score"))

        if "last_checked_at" in cols:
            updates.append("last_checked_at = datetime('now')")
        updates.append("status = ?")
        params.append(status)
        if "locked_by" in cols:
            updates.append("locked_by = NULL")
        if "locked_at" in cols:
            updates.append("locked_at = NULL")

        where_clause = "id = ?"
        where_params: list[Any] = [company["id"]]
        if self.worker_id:
            where_clause += " AND (locked_by IS NULL OR locked_by = ?)"
            where_params.append(self.worker_id)

        sql = f"UPDATE companies SET {', '.join(updates)} WHERE {where_clause}"
        params.extend(where_params)
        self.cur.execute(sql, params)
        if self.cur.rowcount == 0:
            self.lock_mismatch_count += 1
            log.warning("DB update skipped (lock mismatch?) id=%s worker=%s", company["id"], self.worker_id)
            try:
                self.conn.execute(
                    "UPDATE companies SET status='review', locked_by=NULL, locked_at=NULL WHERE id=?",
                    (company["id"],),
                )
                self._commit_with_checkpoint()
            except Exception:
                pass
            return
        self._commit_with_checkpoint()
        import logging as _l; _l.info(f"DB_WRITE_OK id={company['id']} status={status}")

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
        sql = "UPDATE companies SET status=?, locked_by=NULL, locked_at=NULL WHERE id=?"
        params: list[Any] = [status, company_id]
        if self.worker_id:
            sql += " AND (locked_by IS NULL OR locked_by = ?)"
            params.append(self.worker_id)
        self.cur.execute(sql, params)
        if self.cur.rowcount == 0:
            self.lock_mismatch_count += 1
            log.warning("update_status skipped (lock mismatch?) id=%s worker=%s", company_id, self.worker_id)
            try:
                self.conn.execute(
                    "UPDATE companies SET status='review', locked_by=NULL, locked_at=NULL WHERE id=?",
                    (company_id,),
                )
                self._commit_with_checkpoint()
            except Exception:
                pass
            return
        self._commit_with_checkpoint()

    def mark_error(self, company_id: int, error_code: str = "") -> None:
        """
        status を error に更新するヘルパー（既存呼び出し互換のため任意利用）。
        """
        sql = "UPDATE companies SET status='error', error_code=?, locked_by=NULL, locked_at=NULL WHERE id=?"
        params: list[Any] = [error_code, company_id]
        if self.worker_id:
            sql += " AND (locked_by IS NULL OR locked_by = ?)"
            params.append(self.worker_id)
        self.cur.execute(sql, params)
        if self.cur.rowcount == 0:
            self.lock_mismatch_count += 1
            log.warning("mark_error skipped (lock mismatch?) id=%s worker=%s", company_id, self.worker_id)
            try:
                self.conn.execute(
                    "UPDATE companies SET status='review', error_code=?, locked_by=NULL, locked_at=NULL WHERE id=?",
                    (error_code, company_id),
                )
                self._commit_with_checkpoint()
            except Exception:
                pass
            return
        self._commit_with_checkpoint()

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
        self._commit_with_checkpoint()

    def close(self) -> None:
        try:
            if self.wal_checkpoint_interval > 0:
                try:
                    self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                except sqlite3.DatabaseError:
                    pass
            self.cur.close()
        finally:
            self.conn.close()
        if self.lock_mismatch_count > 0:
            log.info("lock_mismatch_count=%s", self.lock_mismatch_count)
