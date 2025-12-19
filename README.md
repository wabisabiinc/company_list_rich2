# company_list_rich

## HubSpot CSV 取り込み手順

1. 事前に `data/companies.db` をバックアップするか、SQLite のスナップショットを取ります。
2. HubSpot から出力した CSV を `data/` に配置し、以下の順で実行します。

```bash
# サンプル(小規模) CSV
python3 scripts/import_hubspot_companies_into_existing.py --dataset hubspot --db data/companies.db data/hubspot_sample.csv

# 本番 CSV (分割2ファイルの例)
python3 scripts/import_hubspot_companies_into_existing.py --dataset hubspot --db data/companies.db data/hubspot_export_part1.csv
python3 scripts/import_hubspot_companies_into_existing.py --dataset hubspot --db data/companies.db data/hubspot_export_part2.csv
```

`--dataset` は既定で `hubspot` なので省略可能です。`--db` は指定したパスに DB を作成／接続します（省略時は `data/companies.db`）。

スクリプトは空欄のみを補完し、既存の `done/review` ステータスや住所/電話/代表者などの値は上書きしません。新規レコードは `status='pending'` として投入され、`python main.py` を従来通り走らせるだけでスクレイプ処理を継続できます。

## 物流 CSV（別DB）取り込み手順

物流データは既存 HubSpot データと混ざらないように専用 DB を用意して取り込みます。

```bash
# 物流CSVを新規DBに投入
python3 scripts/import_hubspot_companies_into_existing.py \
  --dataset logistics \
  --db data/companies_logistics.db \
  data/物流.csv

# 物流DBを対象にスクレイプを実行
COMPANIES_DB_PATH=data/companies_logistics.db python3 main.py
```

インポート時に DB が存在しなければ自動作成されます。`main.py` を走らせる際は `COMPANIES_DB_PATH` 環境変数を設定し、対象の DB を切り替えてから実行してください。

## 進捗確認用 SQL

```sql
-- 総件数
SELECT COUNT(*) FROM companies;

-- ステータス別件数
SELECT status, COUNT(*) FROM companies GROUP BY status ORDER BY COUNT(*) DESC;

-- HubSpot 由来のURL/電話を抜き取りチェック
SELECT id, company_name, homepage, phone, source_csv
  FROM companies
 WHERE status='pending' AND source_csv LIKE '%hubspot%'
 ORDER BY id DESC
 LIMIT 20;
```

## データ保全ポリシー

- `homepage/phone/address/rep_name/description/listing…` など既存の列は**空欄のみ**を埋め、値が入っている場合は触りません。
- 公式スクレイプの進捗を壊さないため、既存の `done/review/error` は維持し、新規行のみ `pending` で挿入します。
- 取り込みスクリプトは `hubspot_id/corporate_number` などの追加カラムを使って名寄せし、同一企業を重複登録しないようにしています。
