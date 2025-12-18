#!/usr/bin/env python3
import sqlite3
import re

DB_PATH = "data/companies.db"

MAP_NOISE_RE = re.compile(r"(地図アプリ|マップ|Google\s*マップ|地図|map)", re.I)

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT id, found_address FROM companies WHERE found_address LIKE '%地図%' OR found_address LIKE '%map%'"
    )
    rows = cur.fetchall()
    cleaned = 0
    for cid, addr in rows:
        if not addr:
            continue
        text = str(addr)
        stripped = re.sub(r"\s+", " ", text).strip()
        # 短いマップ誘導だけ、または末尾が地図誘導のみのものを空にする
        if len(stripped) <= 30 and MAP_NOISE_RE.search(stripped):
            cur.execute("UPDATE companies SET found_address='' WHERE id=?", (cid,))
            cleaned += 1
            continue
        m = MAP_NOISE_RE.search(stripped)
        if m:
            trimmed = stripped[: m.start()].strip()
            if len(trimmed) >= 6:
                cur.execute("UPDATE companies SET found_address=? WHERE id=?", (trimmed, cid))
                cleaned += 1
    con.commit()
    print(f"cleaned {cleaned} rows")

if __name__ == "__main__":
    main()
