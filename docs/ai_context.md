# AI コンテキスト / 運用ガイド

このシステムは日本の企業情報を収集し、公式サイト判定・基本属性抽出・精度評価を自動化するワーカーです。Gemini などの LLM は本ドキュメントを毎回読み込んで、揺らぎなく同じポリシーで応答してください。  
対象ファイル: `main.py`, `src/company_scraper.py`, `src/ai_verifier.py`, `src/database_manager.py`

## 目的・出力
- 入力: DB の `companies` テーブル行（`company_name`, `address` など）
- 出力: 公式ホームページと各種属性（電話・所在地・代表者・説明・上場区分・資本金・売上・利益・決算月・設立年）を `done` または `review/no_homepage/error` ステータスで保存
- LLM への期待: 公式サイト判定補助と属性抽出補助。別会社の情報を混ぜない。わからない場合は空文字/空欄を返す。

## DB スキーマ主要カラム
- `id`, `company_name`, `address`, `employee_count`
- `homepage`, `phone`, `found_address`, `rep_name`, `description`
- `listing`, `revenue`, `profit`, `capital`, `fiscal_month`, `founded_year`
- `status` (pending/running/done/review/error/no_homepage)
- `phone_source`, `address_source`, `ai_used`, `ai_model`, `extract_confidence`
- `homepage_official_flag/source/score`
- `error_code`, `last_checked_at`

## ステータス運用
- `pending`: 未処理
- `running`: 処理中（TTL で回収）
- `done`: 公式サイト決定＋主要項目 OK
- `review`: 公式はあるが検証弱い/時間切れ/住所不一致など
- `no_homepage`: 公式サイト見つからず早期終了
- `error`: 例外発生

## 環境パラメータ（.env）
- 範囲・順序: `ID_MIN`, `ID_MAX`, `MAX_ROWS`, `CLAIM_ORDER`
- 取得幅: `SEARCH_CANDIDATE_LIMIT` (初期 6), `FETCH_CONCURRENCY`, `PROFILE_FETCH_CONCURRENCY`
- レート・タイムアウト: `TIME_LIMIT_SEC` (初期 45), `AI_COOLDOWN_SEC`, `SLEEP_BETWEEN_SEC`, `JITTER_RATIO`
- フラグ: `HEADLESS`, `USE_AI`, `REFERENCE_CSVS`

## ワークフロー概要
1) 1件クレーム取得 (`pending`→`running`)
2) 会社名・住所で検索 (`SEARCH_CANDIDATE_LIMIT` 上限)。除外ドメインをフィルタ。
3) 各候補を Playwright でフェッチし、テキスト/HTML/スクショを取得（キャッシュ利用）。
4) 公式サイト判定: ルールベース + 住所一致 + ドメインスコア + AI 補助  
   - ドメインスコア低＆住所一致なしは公式採用しない（誤公式防止）。  
   - `no_homepage` な場合は早期終了。
5) 公式サイトから候補抽出（電話/住所/代表者/財務/説明）。足りなければ優先リンク巡回、さらに必要に応じて関連クロール。`TIME_LIMIT_SEC` 超過時は深掘り・2回目 AI をスキップ。
6) AI 検証: 公式ページテキスト＋スクショ、または優先リンク統合テキストを与え、欠損を補完。わからない項目は空で返す。
7) `verify_on_site` で電話/住所の簡易検証（時間切れ時はスキップ）。
8) 正規化・クレンジングして保存。`done/review/no_homepage/error` に振り分け。

## 公式サイト判定ポリシー（重要）
- 強い要素: ドメインスコア（会社名トークンとの一致）、`.co.jp/.or.jp/.go.jp` など、住所一致、社名が本文/meta/URL に含まれること
- 弱い要素のみ（ドメインスコア低 & 住所一致なし）は公式にしない
- 除外: 口コミ/求人/地図/ポータル/ディレクトリ等のブロックドメイン
- AI 判定結果も上記ルールを満たさない場合は却下

## 抽出ルール
- 電話: `0X-XXXX-XXXX` に正規化。求人/問い合わせ専用なども拾うが、複数あれば本文優先。
- 住所: 郵便番号付き「〒123-4567 住所」。入力住所と 8 桁キー・郵便番号・漢字トークンで類似度を確認。
- 代表者: 敬称/役職/括弧/冗長語を除去。汎用語（氏名/名前/役職/担当/選任/概要など）は除外。会社名・地名・長文は禁止。
- 説明: 見出しだけ（会社概要/事業概要/法人概要/沿革/会社案内/企業情報）は除外。ニュース/採用/日付行は除外。本文から 80 文字以内の要約。
- 上場区分: 許可キーワード（上場/未上場/非上場/市場名/Nasdaq/NYSE など）のみ採用。
- 金額系: 単位（億円/万円/千円/円）を含まないものは除外。長さは 40 文字以内。
- 決算月: `1-12月` を抽出し正規化。
- 設立年: 西暦 4 桁。

## パフォーマンス/スキップ
- `TIME_LIMIT_SEC` を超えたら深掘り・2回目AI・verifyをスキップし `review` で保存（`error_code=timeout`）。
- `no_homepage`: 検索候補が全滅/スコア低の場合は早期終了し後日再処理。
- 公式決定＋主要項目充足時は追加クロールを行わない。

## 期待する LLM 出力例（verify_info）
```
{
  "phone_number": "03-1234-5678",
  "address": "〒123-4567 東京都渋谷区○○1-2-3",
  "rep_name": "山田太郎",
  "description": "〇〇事業を展開する企業。主要サービスは…",
  "listing": "未上場",
  "capital": "5,000万円",
  "revenue": "12億円",
  "profit": "1億円",
  "fiscal_month": "3月",
  "founded_year": "1999"
}
```
- 不明な項目は空文字または `null` で返す。別会社の値を推測で埋めない。

## Gemini へのプロンプト雛形（system）
```
あなたは企業公式サイトの情報抽出アシスタントです。
- 別会社の情報を混ぜない。わからない項目は空で返す。
- 見出しだけの文（会社概要/事業概要/法人概要/沿革/会社案内/企業情報）はdescriptionに入れない。
- 汎用語（氏名/名前/役職/担当/選任/概要など）はrep_nameにしない。
- 金額は単位を伴う表記のみ許容。決算月は1-12月で表記。
出力はJSONのみ。説明文や推測は禁止。
```

## Gemini へのプロンプト雛形（user/ツール呼び出し）
```
会社名: {company_name}
住所: {address}
本文: {text_snippet_or_full}
（必要に応じてスクリーンショット参照あり）
上の本文から公式サイトの情報を抽出し、指定のJSONキーで返してください。
```

## 運用メモ
- スキップ/後回し方針: `no_homepage` と `timeout` は別バッチで再処理。`review` は手動確認または緩和設定で再実行。
- ログ: 1社ごとに `elapsed=xx.xs` を出力。異常な遅延や誤公式はIDとURLを記録してブロックリストに反映。
- ブロックワード/ルールは誤りが見つかり次第このドキュメントに追記し、AIにも反映する。
