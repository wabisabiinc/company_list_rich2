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
- `homepage` は「公式と確定したURLのみ」を保存し、公式でない候補URL（求人/企業DB等）は `provisional_homepage` や `alt_homepage/alt_homepage_type` 側に退避します。
- 公式スクレイプの進捗を壊さないため、既存の `done/review/error` は維持し、新規行のみ `pending` で挿入します。
- 取り込みスクリプトは `hubspot_id/corporate_number` などの追加カラムを使って名寄せし、同一企業を重複登録しないようにしています。
- DB保存時に各テキスト列（`description/listing/revenue/...`）はクレンジングされ、列に不適切なノイズ（URL/連絡先/定型文など）が混入した場合は空欄化されます。

## スクレイプ実行

```bash
# 既定DBで実行
python3 main.py

# 対象DBを切り替える
COMPANIES_DB_PATH=data/companies_logistics.db python3 main.py
```

主な環境変数（抜粋）:
- `USE_AI=true/false`（AI補助のON/OFF）
- `AI_VERIFY_MIN_CONFIDENCE`（AI抽出の最低信頼度。低い場合は採用せず深掘りで再抽出。デフォルト `0.65`）
- `AI_ADDRESS_ENABLED`（AIが返した住所を取り込むか。デフォルト `true`）
- `USE_AI_DESCRIPTION`（現在未使用。description専用の追加AI呼び出しは `AI_DESCRIPTION_FALLBACK_CALL` で制御）
- `REGENERATE_DESCRIPTION`（retry/requeue時に既存descriptionを破棄して再生成する。既定 `false`）
- `AI_DESCRIPTION_ALWAYS`（description を毎回AI由来で埋める。既定 `true`）
- `AI_DESCRIPTION_ALWAYS_CALL`（description 生成AIを毎回呼ぶ。既定 `true`）
- `AI_DESCRIPTION_FALLBACK_CALL`（AI由来descriptionが取れない場合に、追加AI呼び出しでdescriptionだけ生成する。既定 `true`）
- `PRIORITY_DOCS_MAX_LINKS_CAP` / `PROFILE_DISCOVERY_MAX_LINKS_CAP`（会社概要系の優先docs取得リンク数の上限。時間爆発防止）
- `AI_FINAL_ALWAYS`（USE_AI_OFFICIAL=true でも最終AI(select_company_fields)を許可する。既定 `false` / 追加コスト）
- `AI_DESCRIPTION_VERIFY_MIN_LEN` / `AI_DESCRIPTION_VERIFY_MAX_LEN`（AIが返すdescriptionの受理条件を調整）
- `AI_SCREENSHOT_POLICY=auto/always/never`（AIに渡すスクショ方針。`auto` は本文が十分ならスクショ無しで高速化）
- `OFFICIAL_AI_SCREENSHOT_POLICY=always/never/auto`（公式判定AIのスクショ方針。デフォルト `always`）
- `VERIFY_AI_SCREENSHOT_POLICY`（抽出AI（verify/最終選択）のスクショ方針。未指定時は `AI_SCREENSHOT_POLICY`）
- `REQUIRE_OFFICIAL_HOMEPAGE`（official確定できないURLを `homepage` に保存しない。既定 `true`）
- `SAVE_PROVISIONAL_HOMEPAGE`（暫定URLを `homepage` にも保存する。既定 `false`。`final_homepage/provisional_homepage` には常に記録）
- `APPLY_PROVISIONAL_HOMEPAGE_POLICY`（弱い暫定URLを自動で落とす。既定 `true`）
- `DIRECTORY_HARD_REJECT_SCORE`（企業DB/ディレクトリ疑いのハード拒否閾値。既定 `9`）
- `SEARCH_CANDIDATE_LIMIT`（検索候補の最大数）
- `RELATED_BASE_PAGES`, `RELATED_MAX_HOPS_BASE`（深掘りのページ数/ホップ上限。内部でも最大3にクランプ）

既存DBのノイズを一括除去したい場合:
```bash
python3 scripts/cleanup_text_fields.py --db data/companies.db --dry-run
python3 scripts/cleanup_text_fields.py --db data/companies.db
```

## テスト

```bash
python3 -m pytest
```
