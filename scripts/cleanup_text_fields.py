#!/usr/bin/env python3
import argparse
import re
import sqlite3
import unicodedata
import html as html_mod


DESCRIPTION_MAX_LEN_DEFAULT = 200
DESCRIPTION_MIN_LEN_DEFAULT = 10

_DESCRIPTION_BIZ_HINTS = (
    "事業", "製造", "開発", "販売", "提供", "サービス", "運営", "支援", "施工", "設計", "製作",
    "物流", "運送", "建設", "工事", "コンサル", "consulting", "solution", "ソリューション",
    "製品", "プロダクト", "システム", "加工", "レンタル", "IT", "デジタル", "クラウド", "SaaS", "DX", "AI",
    "データ分析", "セキュリティ", "インフラ", "研究", "技術",
    "人材", "教育", "医療", "ヘルスケア", "介護", "福祉", "食品", "エネルギー", "不動産", "金融", "EC", "通販",
)

_LISTING_ALLOWED_KEYWORDS = (
    "上場", "未上場", "非上場", "東証", "名証", "札証", "福証", "JASDAQ",
    "TOKYO PRO", "マザーズ", "グロース", "スタンダード", "プライム",
    "Nasdaq", "NYSE",
)

_AMOUNT_ALLOWED_UNITS = ("億円", "万円", "千円", "円")


def _looks_mojibake(text: str) -> bool:
    if not text:
        return False
    if "\ufffd" in text:
        return True
    if re.search(r"[ぁ-んァ-ン一-龥]", text):
        return False
    latin_count = sum(1 for ch in text if "\u00c0" <= ch <= "\u00ff")
    if latin_count >= 3 and latin_count / max(len(text), 1) >= 0.15:
        return True
    return bool(re.search(r"[ÃÂãâæçïðñöøûüÿ]", text) and latin_count >= 2)


def _truncate_description(text: str, max_len: int, min_len: int) -> str:
    if len(text) <= max_len:
        return text
    truncated = text[:max_len]
    truncated = re.sub(r"[、。．,;]+$", "", truncated)
    trimmed = re.sub(r"\s+\S*$", "", truncated).strip()
    return trimmed if len(trimmed) >= min_len else truncated.rstrip()


def clean_description(raw: str | None, *, max_len: int, min_len: int) -> str:
    if raw is None:
        return ""
    text = unicodedata.normalize("NFKC", str(raw))
    text = html_mod.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text or _looks_mojibake(text):
        return ""
    if re.search(r"https?://|mailto:|@|＠|tel[:：]|電話|ＴＥＬ|TEL|ＦＡＸ|FAX|住所|所在地", text, flags=re.I):
        return ""
    if any(term in text for term in ("お問い合わせ", "お問合せ", "お問合わせ", "アクセス", "採用", "求人", "予約", "営業時間", "受付時間")):
        return ""
    if ("サイト" in text or "ページ" in text) and any(
        w in text for w in ("データベース", "登録企業", "掲載", "企業詳細", "会社情報を掲載", "企業情報を掲載", "口コミ", "評判", "ランキング")
    ):
        return ""
    if any(w in text for w in ("理念", "ビジョン", "ご挨拶", "メッセージ", "ポリシー", "方針", "コンプライアンス", "情報セキュリティ")):
        return ""

    candidates = [text]
    if "。" in text or "．" in text:
        parts = [p.strip() for p in re.split(r"[。．]", text) if p.strip()]
        if parts:
            candidates = parts
    for cand in candidates:
        if len(cand) < min_len:
            continue
        if not any(h in cand for h in _DESCRIPTION_BIZ_HINTS):
            continue
        return _truncate_description(cand, max_len=max_len, min_len=min_len)
    return ""


def clean_listing(raw: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(raw or "")).strip().replace("　", " ")
    if not text:
        return ""
    if re.search(r"[。！？!?\n]", text):
        return ""
    text = re.sub(r"\s+", "", text)
    if len(text) > 15:
        return ""
    low = text.lower()
    if any(k.lower() in low for k in _LISTING_ALLOWED_KEYWORDS):
        return text
    if re.fullmatch(r"(?:上場|未上場|非上場)", text):
        return text
    if re.fullmatch(r"[0-9]{4}", text):
        return text
    return ""


def clean_amount(raw: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(raw or "")).strip()
    if not text:
        return ""
    if re.search(r"(従業員|社員|職員|スタッフ)\s*[0-9]+", text):
        return ""
    if re.search(r"[0-9]+\s*(名|人)\b", text):
        return ""
    text = re.sub(r"[（(][^）)]*[）)]", "", text)
    m = re.search(r"([0-9,\.]+(?:兆円|億円|万円|千円|円))", text)
    text = m.group(1) if m else text
    if not re.search(r"[0-9]", text):
        return ""
    if not any(u in text for u in _AMOUNT_ALLOWED_UNITS):
        return ""
    text = re.sub(r"\s+", "", text)
    return text[:40]


def clean_fiscal_month(raw: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(raw or "")).strip().replace("　", " ")
    if not text:
        return ""
    text = text.replace("期", "月").replace("末", "月")
    if re.fullmatch(r"[Qq][1-4]", text):
        qmap = {"Q1": "3月", "Q2": "6月", "Q3": "9月", "Q4": "12月"}
        return qmap.get(text.upper(), "")
    m = re.search(r"(1[0-2]|0?[1-9])\s*月", text)
    if m:
        return f"{int(m.group(1))}月"
    m = re.search(r"(1[0-2]|0?[1-9])", text)
    if m:
        return f"{int(m.group(1))}月"
    return ""


def clean_founded_year(raw: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(raw or "")).strip()
    if not text:
        return ""
    m = re.search(r"(18|19|20)\\d{2}", text)
    return m.group(0) if m else ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean noisy text fields in companies.db (in-place).")
    ap.add_argument("--db", default="data/companies.db", help="SQLite DB path")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0=all)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write; only report counts")
    ap.add_argument("--where", default="", help="Optional WHERE clause without 'WHERE' (e.g. \"status='review'\")")
    ap.add_argument("--desc-min", type=int, default=DESCRIPTION_MIN_LEN_DEFAULT)
    ap.add_argument("--desc-max", type=int, default=DESCRIPTION_MAX_LEN_DEFAULT)
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        where = f" WHERE {args.where} " if args.where.strip() else ""
        limit_sql = f" LIMIT {int(args.limit)}" if int(args.limit or 0) > 0 else ""
        rows = con.execute(
            f"SELECT id, description, listing, revenue, profit, capital, fiscal_month, founded_year FROM companies{where}{limit_sql}"
        ).fetchall()

        changed = 0
        for r in rows:
            new_desc = clean_description(r["description"], max_len=int(args.desc_max), min_len=int(args.desc_min))
            new_listing = clean_listing(r["listing"])
            new_revenue = clean_amount(r["revenue"])
            new_profit = clean_amount(r["profit"])
            new_capital = clean_amount(r["capital"])
            new_fiscal = clean_fiscal_month(r["fiscal_month"])
            new_founded = clean_founded_year(r["founded_year"])

            updates = {}
            for key, new_val in (
                ("description", new_desc),
                ("listing", new_listing),
                ("revenue", new_revenue),
                ("profit", new_profit),
                ("capital", new_capital),
                ("fiscal_month", new_fiscal),
                ("founded_year", new_founded),
            ):
                old_val = r[key] or ""
                if old_val != new_val:
                    updates[key] = new_val

            if not updates:
                continue

            changed += 1
            if not args.dry_run:
                sets = ", ".join([f"{k}=?" for k in updates.keys()])
                params = list(updates.values()) + [r["id"]]
                con.execute(f"UPDATE companies SET {sets} WHERE id=?", params)

        if not args.dry_run:
            con.commit()
        print(f"[DONE] rows={len(rows)} changed={changed} dry_run={bool(args.dry_run)}")
    finally:
        con.close()


if __name__ == "__main__":
    main()

