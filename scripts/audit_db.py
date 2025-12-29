import argparse
import os
import sqlite3
import sys
from urllib.parse import urlparse


def _host(url: str) -> str:
    try:
        h = (urlparse(url).netloc or "").lower()
    except Exception:
        return ""
    if h.startswith("www."):
        h = h[4:]
    return h


def main() -> int:
    sys.dont_write_bytecode = True
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    ap = argparse.ArgumentParser(description="Audit companies DB for data quality and official-homepage issues.")
    ap.add_argument("--db", default="data/companies.db", help="Path to SQLite DB (default: data/companies.db)")
    ap.add_argument("--limit", type=int, default=30, help="Max rows to show in each section")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    def q(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return list(con.execute(sql, params).fetchall())

    companies = q("select count(*) as n from companies")[0]["n"]
    url_flags = q("select count(*) as n from url_flags")[0]["n"] if q("select name from sqlite_master where type='table' and name='url_flags'") else 0
    print(f"db={args.db}")
    print(f"companies={companies} url_flags={url_flags}")

    print("\n[status]")
    for r in q("select status, count(*) c from companies group by status order by c desc"):
        print(f"{r['status']}\t{r['c']}")

    print("\n[homepage]")
    r = q(
        """
        select
          sum(case when homepage is null or trim(homepage)='' then 1 else 0 end) as empty_homepage,
          sum(case when homepage is not null and trim(homepage)!='' then 1 else 0 end) as has_homepage,
          sum(case when homepage_official_flag=1 then 1 else 0 end) as official_flag_1,
          sum(case when homepage_official_flag=0 then 1 else 0 end) as official_flag_0,
          sum(case when homepage_official_flag is null then 1 else 0 end) as official_flag_null
        from companies
        """
    )[0]
    print(dict(r))

    print("\n[suspicious_official_hosts]")
    try:
        from src.company_scraper import CompanyScraper  # type: ignore

        hard_exclude = set(getattr(CompanyScraper, "HARD_EXCLUDE_HOSTS", set()) or set())
        suspect = set(getattr(CompanyScraper, "SUSPECT_HOSTS", set()) or set())
    except Exception:
        hard_exclude = set()
        suspect = set()

    rows = q(
        """
        select id, company_name, homepage, homepage_official_source
          from companies
         where homepage_official_flag=1 and homepage is not null and trim(homepage)!=''
        """
    )
    bad: list[sqlite3.Row] = []
    for row in rows:
        host = _host(row["homepage"] or "")
        if not host:
            continue
        if any(host == d or host.endswith("." + d) for d in hard_exclude) or any(host == d or host.endswith("." + d) for d in suspect):
            bad.append(row)
    for row in bad[: max(0, int(args.limit or 0))]:
        print(f"{row['id']}\t{row['company_name']}\t{row['homepage']}\t{row['homepage_official_source']}")
    if bad:
        print(f"(total suspicious official={len(bad)})")
    else:
        print("(none)")

    print("\n[address_input_noise]")
    noisy = q(
        """
        select id, company_name, address
          from companies
         where address like '%Google%' or address like '% 代表%'
         limit ?
        """,
        (int(args.limit or 0),),
    )
    for row in noisy:
        print(f"{row['id']}\t{row['company_name']}\t{row['address']}")
    if not noisy:
        print("(none)")

    print("\n[running_rows]")
    running = q("select id, company_name, locked_by, locked_at from companies where status='running' order by locked_at asc limit ?", (int(args.limit or 0),))
    for row in running:
        print(f"{row['id']}\t{row['company_name']}\t{row['locked_by']}\t{row['locked_at']}")
    if not running:
        print("(none)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
