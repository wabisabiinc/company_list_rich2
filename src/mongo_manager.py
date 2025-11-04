# src/mongo_manager.py
from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument

class MongoManager:
    """
    SQLite版 DatabaseManager と互換のインターフェイス。
    コレクション: companies
    主キー: id (int) をそのまま保持
    フィールド: company_name, address, employee_count, homepage, phone, found_address,
               status, locked_by, locked_at
    """

    def __init__(self):
        uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        dbname = os.getenv("MONGO_DB", "companydb")
        coll = os.getenv("MONGO_COLLECTION", "companies")

        self.client = MongoClient(uri)
        self.db = self.client[dbname]
        self.col = self.db[coll]

        # インデックス（性能 & 一意制約）
        # 待ち行列取得: status, employee_count desc, id asc
        self.col.create_index([("status", ASCENDING),
                               ("employee_count", DESCENDING),
                               ("id", ASCENDING)])
        # 一意：会社名+住所（既存SQLiteの一意制約互換）
        self.col.create_index([("company_name", ASCENDING),
                               ("address", ASCENDING)],
                               unique=True, name="uniq_name_addr")
        # id で更新する想定のため
        self.col.create_index([("id", ASCENDING)], unique=True)

    # ---- CSV/初期投入で使用 ----
    def insert_company(self, company_data: Dict[str, Any]) -> None:
        doc = dict(company_data)
        # 既定
        doc.setdefault("status", "pending")
        doc.setdefault("homepage", "")
        doc.setdefault("phone", "")
        doc.setdefault("found_address", "")
        # id は既に採番済み前提（CSV -> import時に付与）
        self.col.update_one({"id": doc["id"]}, {"$setOnInsert": doc}, upsert=True)

    # ---- 並列処理の核：1件の“取り出し & ロック”を原子的に行う ----
    def claim_next_company(self, worker_id: str) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        # RUNNING_TTL_MIN の回収（stuck回復）
        ttl_min = int(os.getenv("RUNNING_TTL_MIN", "20"))
        expire_before = now - timedelta(minutes=ttl_min)
        # 古いrunningをpendingに戻す（ロック回収）
        self.col.update_many(
            {"status": "running", "locked_at": {"$lt": expire_before}},
            {"$set": {"status": "pending"},
             "$unset": {"locked_by": "", "locked_at": ""}}
        )

        # pendingの中から、employee_count desc, id asc で1件をrunningへ
        doc = self.col.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "running",
                      "locked_by": worker_id,
                      "locked_at": now}},
            sort=[("employee_count", -1), ("id", 1)],
            return_document=ReturnDocument.AFTER
        )
        return dict(doc) if doc else None

    # SQLite版の get_next_company 互換（ロック無し）—使わないが互換で用意
    def get_next_company(self) -> Optional[Dict[str, Any]]:
        doc = self.col.find_one(
            {"status": "pending"},
            sort=[("employee_count", -1), ("id", 1)]
        )
        return dict(doc) if doc else None

    # ---- 処理結果の保存 ----
    def save_company_data(self, company: Dict[str, Any], status: str = "done") -> None:
        update = {
            "$set": {
                "homepage": company.get("homepage", "") or "",
                "phone": company.get("phone", "") or "",
                "found_address": company.get("found_address", "") or "",
                "status": status
            },
            "$unset": {"locked_by": "", "locked_at": ""}  # 終了時はロック解除
        }
        self.col.update_one({"id": company["id"]}, update)

    def update_status(self, company_id: int, status: str) -> None:
        update = {"$set": {"status": status}}
        if status != "running":
            update["$unset"] = {"locked_by": "", "locked_at": ""}
        self.col.update_one({"id": company_id}, update)

    def close(self) -> None:
        self.client.close()
