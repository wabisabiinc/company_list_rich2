import asyncio
import json
import os
import sqlite3
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3


ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from src.database_manager import DatabaseManager


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        print(f"missing env: {name}")
        sys.exit(1)
    return value


def normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    prefix = prefix.lstrip("/")
    if not prefix.endswith("/"):
        prefix += "/"
    return prefix


def first_non_empty(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row and row[k] is not None:
            s = str(row[k]).strip()
            if s != "":
                return s
    return ""


def to_int(value: str) -> int | None:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        digits = "".join(ch for ch in s if ch.isdigit())
        return int(digits) if digits else None


def build_address(row: dict) -> str:
    pref = first_non_empty(row, "都道府県／地域", "都道府県/地域", "都道府県", "prefecture")
    city = first_non_empty(row, "市区町村", "city")
    addr1 = first_non_empty(row, "番地", "address1")
    addr2 = first_non_empty(row, "番地2", "address2")
    parts = [p for p in (pref, city, addr1, addr2) if p]
    return "".join(parts)


def row_to_company(row: dict, source_label: str) -> dict:
    company_name = first_non_empty(row, "会社名", "company_name")
    if not company_name:
        raise ValueError("missing company_name")

    address = build_address(row) or first_non_empty(row, "住所", "address")
    if not address:
        raise ValueError("missing address")

    employee_count = to_int(first_non_empty(row, "従業員数", "employee_count"))
    homepage = first_non_empty(row, "ウェブサイトURL", "URL_保険DB", "homepage", "URL", "url")
    phone = first_non_empty(row, "電話番号", "電話番号(2)", "phone")
    hubspot_id = first_non_empty(row, "レコードID", "hubspot_id", "id")
    corporate_number = first_non_empty(row, "法人番号（名寄せ）", "法人番号", "corporate_number")
    industry = first_non_empty(row, "業種", "業種_保険DB", "業種グループ", "industry")
    rep_name = first_non_empty(row, "代表者名", "rep_name")

    return {
        "company_name": company_name,
        "address": address,
        "csv_address": address,
        "employee_count": employee_count,
        "homepage": homepage,
        "phone": phone,
        "hubspot_id": hubspot_id,
        "corporate_number": corporate_number,
        "industry": industry,
        "rep_name": rep_name,
        "source_csv": source_label,
    }


def insert_company(db_path: str, worker_id: str, company: dict) -> int:
    manager = DatabaseManager(db_path=db_path, worker_id=worker_id)
    cols = [
        k for k, v in company.items()
        if v is not None and v != "" and k in manager._schema_columns
    ]
    if not cols:
        manager.close()
        raise ValueError("no insertable columns")
    placeholders = ",".join("?" for _ in cols)
    values = [company[c] for c in cols]
    manager.cur.execute(
        f"INSERT INTO companies ({', '.join(cols)}) VALUES ({placeholders})",
        values,
    )
    manager._commit_with_checkpoint()
    row_id = int(manager.cur.lastrowid)
    manager.close()
    return row_id


def fetch_company(db_path: str, row_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM companies WHERE id=?", (row_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def delete_company(db_path: str, row_id: int) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM companies WHERE id=?", (row_id,))
        conn.commit()
    finally:
        conn.close()


RESULT_FIELDS = [
    "id",
    "company_name",
    "address",
    "employee_count",
    "employees",
    "final_homepage",
    "phone",
    "found_address",
    "rep_name",
    "description",
    "industry",
    "industry_major",
    "industry_middle",
    "industry_minor",
    "industry_major_code",
    "industry_middle_code",
    "industry_minor_code",
    "industry_minor_item_code",
    "industry_minor_item",
    "contact_url",
    "listing",
    "revenue",
    "profit",
    "capital",
    "fiscal_month",
    "founded_year",
    "corporate_number",
]


def pick_result_fields(result_row: dict, fallback_company: dict) -> dict:
    out: dict = {}
    for key in RESULT_FIELDS:
        value = result_row.get(key)
        if (value is None or value == "") and key == "corporate_number":
            value = fallback_company.get("corporate_number")
        out[key] = value
    return out


def main() -> int:
    worker_id = os.getenv("WORKER_ID") or f"worker-{os.getpid()}"
    db_path = os.getenv("COMPANIES_DB_PATH") or str(ROOT_DIR / "data" / f"companies_{worker_id}.db")
    os.environ.setdefault("WORKER_ID", worker_id)
    os.environ.setdefault("COMPANIES_DB_PATH", db_path)
    os.environ.setdefault("MAX_ROWS", "1")

    region = require_env("AWS_REGION")
    queue_url = require_env("SQS_QUEUE_URL")
    bucket = require_env("S3_BUCKET")
    prefix = normalize_prefix(require_env("S3_PREFIX"))
    source_label = os.getenv("SOURCE_LABEL", "")

    from main import process as scrape_process

    sqs = boto3.client("sqs", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    def log(msg: str) -> None:
        print(f"[{worker_id}] {msg}")

    while True:
        try:
            log("receiving message from SQS...")
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )
            messages = resp.get("Messages", [])
            if not messages:
                log("no messages, sleeping 5s")
                time.sleep(5)
                continue

            msg = messages[0]
            receipt_handle = msg.get("ReceiptHandle")
            message_id = msg.get("MessageId", str(uuid.uuid4()))
            body = msg.get("Body", "")

            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                log("invalid JSON body; skipping")
                continue

            company_row = row_to_company(payload, source_label)
            row_id = insert_company(db_path, worker_id, company_row)

            asyncio.run(scrape_process())

            result_row = fetch_company(db_path, row_id)
            filtered_result = pick_result_fields(result_row, company_row)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            key = f"{prefix}{timestamp}_{message_id}.json"
            result = {
                "worker_id": worker_id,
                "message_id": message_id,
                "received_at": timestamp,
                "result": filtered_result,
            }

            log(f"putting to S3: s3://{bucket}/{key}")
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(result, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )

            if receipt_handle:
                log("deleting message from SQS...")
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

            delete_company(db_path, row_id)
            log("done")

        except Exception:
            traceback.print_exc()
            time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
