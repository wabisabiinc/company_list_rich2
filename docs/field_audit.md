# フィールド監査（代表者名 / 住所）

DBで取得できた `rep_name`（代表者名）や `address`（住所）に、誤って別種の情報（電話・URL・問い合わせ文など）が混入することがあります。

本リポジトリでは、まず「安い（ルール）検査」で疑わしい行を絞り込み、必要ならAIで最終判定するのが費用対効果が高いです。

## 全カラム監査（推奨）

住所/代表者だけでなく、**DBの全カラム**に対して「欄違い・形式違い（URL/電話/HTML混入など）」を検出できます。
この監査は **ネットワークを使わず**、値のパターンだけで疑わしい箇所を列挙します。

### `python main.py` 的な実行（短いコマンド）

```bash
.venv/bin/python field_audit.py --db data/companies_logistics_p1.db --min-severity medium
```

出力は既定で `reports/field_audit_<db>_YYYYmmdd_HHMMSS.csv` です（`reports/` は `.gitignore` 済み）。

## 1) ルール検査（無料・高速）

疑わしい行だけをCSVに書き出します。

```bash
python3 scripts/audit_company_fields.py --db data/companies_logistics_p1.db
```

- 出力: `reports/field_audit_YYYYmmdd_HHMMSS.csv`
- 既定: `total_score >= 4` の行のみ出力

任意で絞り込み:

```bash
# status が done/review のみ
python3 scripts/audit_company_fields.py --db data/companies_logistics_p1.db --status done,review

# まずは1000件だけ見る
python3 scripts/audit_company_fields.py --db data/companies_logistics_p1.db --limit 1000
```

## 2) AI判定（コストあり・高精度）

ルール検査で引っかかった行のうち、最大 `--ai-max` 件だけを Gemini で判定します。

```bash
python3 scripts/audit_company_fields.py --db data/companies_logistics_p1.db --ai --ai-max 200
```

- `GEMINI_API_KEY` が必要です（`.env` を読み込みます）
- 送信件数は `--ai-max` で上限がかかります（コスト制御）

## 監査の見方（CSV列）

- `rep_issues` / `addr_issues`: ルールで検出した疑いポイント（この監査は `address` を検査対象にします）
- `total_score` / `risk`: 疑いの強さ
- `source_url_rep` / `source_url_address`: その値の根拠URL（ある場合）
- `ai_rep_ok` / `ai_addr_ok`: AI判定（`1=true`, `0=false`, 空=判断不能）

## 3) 自動クリーニング（住所:本社優先 / 代表者:名前以外は空）

AI監査の結果を見て方針が固まったら、DBの値を自動で整形できます。

住所（`address`）:
- 電話番号/URL/メール/メニュー文/著作権/緯度経度などの混入を除去
- 複数住所が混在している場合は「本社/本店/本部」などの手掛かりを優先し、無ければ先頭の住所を採用

代表者（`rep_name`）:
- 「名前らしい部分」だけを抽出して残す
- 名前が取れない/役職や文章のみの場合は空欄にする（名だけも許容）

### まずはドライラン（DBは変更しない）

```bash
python3 scripts/clean_company_fields.py --db data/companies_logistics_p1.db
```

変更案のレポートが `reports/field_clean_*.csv` に出ます。

### 適用（バックアップを取ってDB更新）

```bash
python3 scripts/clean_company_fields.py --db data/companies_logistics_p1.db --apply
```

`backups/` に `*.bak` が作られ、トランザクションで更新されます。

## 4) 全カラムのファクトチェック

証拠URLの本文と照合して「混入/取り違え」を検出する仕組みは `docs/fact_check.md` を参照してください。
