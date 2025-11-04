# scripts/fix_ddg_redirects.py
import sqlite3
from urllib.parse import urlparse, parse_qs, unquote

def decode_uddg(url: str) -> str:
    if not url: return url
    try:
        p = urlparse(url)
        if (p.netloc.endswith("duckduckgo.com") or p.netloc.endswith("html.duckduckgo.com")) and p.path.startswith("/l"):
            qs = parse_qs(p.query)
            if "uddg" in qs and qs["uddg"]:
                return unquote(qs["uddg"][0])
    except Exception:
        pass
    return url

def main(db_path="data/companies.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    rows = cur.execute("SELECT id, homepage FROM companies WHERE homepage LIKE '%duckduckgo.com/l/%'").fetchall()
    fixed = 0
    for cid, hp in rows:
        new = decode_uddg(hp or "")
        if new and new != hp:
            cur.execute("UPDATE companies SET homepage=? WHERE id=?", (new, cid))
            fixed += 1
    conn.commit()
    conn.close()
    print(f"fixed {fixed} rows")

if __name__ == "__main__":
    main()
