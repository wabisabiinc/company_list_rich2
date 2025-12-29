import argparse
import os
import sqlite3
import sys
from urllib.parse import urlparse


def _host(url: str) -> str:
    try:
        h = (urlparse(url).netloc or "").lower().split(":")[0]
    except Exception:
        return ""
    if h.startswith("www."):
        h = h[4:]
    return h


def _ensure_columns(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(companies)").fetchall()}
    for name, ddl in (
        ("official_homepage", "ALTER TABLE companies ADD COLUMN official_homepage TEXT;"),
        ("alt_homepage", "ALTER TABLE companies ADD COLUMN alt_homepage TEXT;"),
        ("alt_homepage_type", "ALTER TABLE companies ADD COLUMN alt_homepage_type TEXT;"),
    ):
        if name not in cols:
            con.execute(ddl)
            cols.add(name)


def main() -> int:
    sys.dont_write_bytecode = True
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    ap = argparse.ArgumentParser(description="Migrate DB to homepage model v2 (official_homepage + alt_homepage).")
    ap.add_argument("--db", default="data/companies.db")
    ap.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--set-pending", action="store_true", help="If applied, set affected rows to status='pending'")
    args = ap.parse_args()

    from src.company_scraper import CompanyScraper  # type: ignore

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=15000;")
    _ensure_columns(con)

    rows = list(
        con.execute(
            """
            select id, status, company_name, homepage, homepage_official_flag, provisional_homepage
              from companies
             where homepage is not null and trim(homepage)!=''
            """
        ).fetchall()
    )

    affected = []
    for r in rows:
        homepage = (r["homepage"] or "").strip()
        if not homepage:
            continue
        if int(r["homepage_official_flag"] or 0) == 1 and CompanyScraper.is_disallowed_official_host(homepage):
            affected.append(r)

    print(f"db={args.db}")
    print(f"official_disallowed={len(affected)}")
    for r in affected[: max(0, int(args.limit or 0))]:
        print(f"{r['id']}\t{r['status']}\t{r['company_name']}\t{r['homepage']}\t{_host(r['homepage'])}")

    if not args.apply:
        print("(dry-run) use --apply to write changes")
        return 0

    con.execute("BEGIN IMMEDIATE;")
    try:
        for r in affected:
            cid = int(r["id"])
            homepage = (r["homepage"] or "").strip()
            provisional = (r["provisional_homepage"] or "").strip()
            new_provisional = provisional or homepage
            new_status = "pending" if args.set_pending else (r["status"] or "review")
            con.execute(
                """
                update companies
                   set homepage='',
                       homepage_official_flag=0,
                       homepage_official_source='',
                       homepage_official_score=0.0,
                       official_homepage='',
                       alt_homepage=?,
                       alt_homepage_type='platform',
                       provisional_homepage=?,
                       provisional_reason=coalesce(nullif(provisional_reason,''), 'migrate_v2_disallowed_official'),
                       status=?
                 where id=?
                """,
                (homepage, new_provisional, new_status, cid),
            )
        con.commit()
    except Exception:
        con.rollback()
        raise

    print(f"applied={len(affected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

