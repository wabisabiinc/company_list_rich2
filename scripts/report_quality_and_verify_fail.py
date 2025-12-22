import os
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict


PREFECTURE_NAMES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県",
)

ZIP_RE = re.compile(r"\d{3}[-\s]?\d{4}")
CITY_RE = re.compile(r"((?:東京都|北海道|大阪府|京都府)?[^ 　\t,，]+?(?:市|区|町|村))")


def is_prefecture_only_address(text: str | None) -> bool:
    if not text:
        return False
    s = unicodedata.normalize("NFKC", str(text))
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"^〒?\d{3}[-\s]?\d{4}", "", s)
    s = s.strip(" 　\t,，;；。．|｜/／・-‐―－ー:：")
    return bool(s) and s in PREFECTURE_NAMES


def is_address_verifiable(text: str | None) -> bool:
    if not text:
        return False
    s = unicodedata.normalize("NFKC", str(text)).strip()
    if not s:
        return False
    if is_prefecture_only_address(s):
        return False
    if ZIP_RE.search(s):
        return True
    if CITY_RE.search(s):
        return True
    if re.search(r"\d", s) and re.search(r"(丁目|番地|番|号)", s):
        return True
    return False


def main() -> None:
    db_path = os.getenv("COMPANIES_DB_PATH", "data/companies.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
          id, company_name, address, homepage, phone,
          status, address_source, homepage_official_source, homepage_official_flag
        FROM companies
        WHERE status IN ('done', 'review', 'no_homepage')
        """
    ).fetchall()

    print(f"db={db_path} processed={len(rows)}")
    status_counts = Counter(r["status"] for r in rows)
    print("status:", dict(status_counts))

    pref_only = [r for r in rows if is_prefecture_only_address(r["address"])]
    print(f"input_addr_pref_only: {len(pref_only)} ({(len(pref_only)/len(rows)*100):.1f}%)" if rows else "input_addr_pref_only: 0")

    review_rows = [r for r in rows if r["status"] == "review"]
    src_counts = Counter((r["homepage_official_source"] or "").strip() for r in review_rows)
    print("review.homepage_official_source top10:", dict(src_counts.most_common(10)))

    verify_fail = [
        r for r in review_rows
        if (r["homepage_official_source"] or "").strip() == "verify_fail"
    ]
    if not verify_fail:
        print("verify_fail: 0")
        return

    groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for r in verify_fail:
        phone = (r["phone"] or "").strip()
        addr = (r["address"] or "").strip()
        addr_source = (r["address_source"] or "").strip().lower()

        if not is_address_verifiable(addr) or (addr_source == "none" and is_prefecture_only_address(addr)):
            groups["A_addr_low_quality_or_csv"].append(r)
        elif not phone:
            groups["B_phone_empty"].append(r)
        else:
            groups["C_both_present_but_unverified"].append(r)

    print("verify_fail groups:", {k: len(v) for k, v in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))})

    for key, items in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        print(f"\n[{key}] sample up to 20")
        for r in items[:20]:
            print(
                f"- id={r['id']} name={r['company_name']} phone={(r['phone'] or '').strip()} "
                f"addr={(r['address'] or '').strip()} addr_source={(r['address_source'] or '').strip()} "
                f"homepage={(r['homepage'] or '').strip()}"
            )


if __name__ == "__main__":
    main()

