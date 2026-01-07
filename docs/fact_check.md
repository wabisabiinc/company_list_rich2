# 全カラムのファクトチェック（証拠ベース）

ここでの「ファクトチェック」は、**DBに保存されている値が「根拠URLのページ本文で確認できるか」**を検証するものです。

- 目的: 取り違え/混入/誤抽出（電話やメニュー文、別拠点住所、ラベル等）の検出
- 注意: Webページ自体が誤っている可能性は否定できないため、**絶対的な真偽証明ではありません**

## できること / できないこと

できる（比較的正確）:
- `phone` / `address` / `rep_name` が根拠ページに存在するか（厳しめに判定）
- `description_evidence` の snippet がURL本文に存在するか（整合性チェック）
- `corporate_number` の形式チェック（13桁）

できない（別の仕組みが必要）:
- 売上/利益/資本金などの「真偽」そのもの（公的ソース・IR・登記等との照合が必要）

## 実行方法

例: 物流P1のDBを全件チェック（証拠URLを取得して照合）

```bash
.venv/bin/python scripts/fact_check_db.py --db data/companies_logistics_p1.db
```

出力: `reports/fact_check_companies_logistics_p1_YYYYmmdd_HHMMSS.csv`

## もっと分かりやすいコマンド（推奨）

`python main.py` のように短く実行したい場合は、ルートの `fact_check.py` を使えます。
既定で「キャッシュは一時ディレクトリ（終了後削除）」「出力は /tmp」にします。

```bash
.venv/bin/python fact_check.py --db data/companies_logistics_p1.db --limit 100
```

出力例: `reports/fact_check_companies_logistics_p1_YYYYmmdd_HHMMSS.csv`

### コスト/時間を抑える

```bash
# まずは100件だけ
.venv/bin/python scripts/fact_check_db.py --db data/companies_logistics_p1.db --limit 100

# fetch間隔を入れる（BAN/負荷回避）
.venv/bin/python scripts/fact_check_db.py --db data/companies_logistics_p1.db --sleep-sec 0.5

# 取得回数の上限（安全弁）
.venv/bin/python scripts/fact_check_db.py --db data/companies_logistics_p1.db --max-fetch 500

# キャッシュを作らない
.venv/bin/python scripts/fact_check_db.py --db data/companies_logistics_p1.db --no-cache
```

## レポートの見方（CSVはlong形式）

列:
- `field`: 検証対象のカラム名
- `value`: DBの値
- `verdict`: `verified` / `not_found` / `blank` / `unknown_no_evidence` / `fetch_failed`
- `evidence_url` / `evidence_snippet`: 根拠（見つかった場合）

## 次の改善（おすすめ）

より「正確」なファクトチェックを目指す場合は、次の追加が有効です。
- 値の根拠URLを全項目で保存（今は `source_url_phone/address/rep` が中心）
- 公的/一次ソース（法人番号・gBizINFO等）との照合ステップを追加
- `not_found` を再抽出キューに戻すワークフロー（再クロール or 手確認）
