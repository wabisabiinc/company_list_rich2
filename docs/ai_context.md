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
- 取得幅: `SEARCH_CANDIDATE_LIMIT` (デフォルト 5)、`FETCH_CONCURRENCY`, `PROFILE_FETCH_CONCURRENCY`
- レート・タイムアウト: `TIME_LIMIT_SEC` (デフォルト 0: 無効)、`TIME_LIMIT_FETCH_ONLY` (デフォルト 30s), `TIME_LIMIT_WITH_OFFICIAL` (デフォルト 45s), `AI_COOLDOWN_SEC`, `SLEEP_BETWEEN_SEC`, `JITTER_RATIO`
- 単ページ: `PAGE_TIMEOUT_MS` (デフォルト 9000)、`SLOW_PAGE_THRESHOLD_MS` (デフォルト 9000: 閾値超はログ＋ホストスキップ。スキップ無効化は `SKIP_SLOW_HOSTS=false`)、`SKIP_SLOW_HOSTS` (デフォルト true)
- メトリクス: `PHASE_METRICS_PATH` (デフォルト logs/phase_metrics.csv。空文字で無効)
- フラグ: `HEADLESS`, `USE_AI`, `REFERENCE_CSVS`, `RETRY_STATUSES`（再処理順）

## ワークフロー概要
1) 1件クレーム取得 (`pending`→`running`)
2) 会社名・住所で検索 (`SEARCH_CANDIDATE_LIMIT` 上限、曖昧名は+1件)。除外ドメインをフィルタ。会社概要/アクセス系のキーワードを含むクエリを追加し、検索時に会社情報ページも直接集めて候補に統合。
3) 各候補を Playwright でフェッチし、テキスト/HTML/スクショを取得（キャッシュ利用）。公式判定AIは上位候補を最大2並列で投げて判定を待つ間に他処理を進める。
4) 公式サイト判定: ルールベース + 住所一致 + ドメインスコア + AI 補助  
   - ドメインスコア低＆住所一致なし（<6）の候補は公式採用しない。AIがYesでもreview扱い。  
   - `no_homepage`: 公式候補が無い場合は直ちにno_homepageで保存。
5) 公式サイトから候補抽出（電話/住所/代表者/財務/説明）。足りなければ優先リンクを最大10件以上巡回し、必要に応じて関連クロールを広げる。クロールは不足項目に応じてリンク優先度を変え、同一URLの重複フェッチを避ける。`TIME_LIMIT_SEC` 超過時は深掘り・2回目 AI をスキップ。
6) AI 検証: 公式ページテキスト＋スクショ（不足時）、または優先リンク統合テキストを与え、欠損を補完。わからない項目は空で返す。AI呼び出しは非同期で他処理と並行する。
7) `verify_on_site` で電話/住所の簡易検証（時間切れ時はスキップ）。
8) 正規化・クレンジングして保存。`done/review/no_homepage/error` に振り分け。

## 公式サイト判定ポリシー（重要）
- 強い要素: ドメインスコア（会社名トークンとの一致）、`.co.jp/.or.jp/.go.jp` など、住所一致、社名が本文/meta/URL に含まれること
- 住所は都道府県 or 郵便番号一致 + 類似が取れない候補は公式にしない（pref/zip が無い場合は review 送り）
- 弱い要素のみ（ドメインスコア低 & 住所一致なし、目安 <6）は公式にしない。AI が Yes でも不採用で review。
- 除外: 口コミ/求人/地図/ポータル/ディレクトリ等のブロックドメイン
- AI 判定結果も上記ルールを満たさない場合は却下

## 抽出ルール
- 電話: `0X-XXXX-XXXX` に正規化。求人/問い合わせ専用なども拾うが、複数あれば本文優先。
- 住所: 郵便番号付き「〒123-4567 住所」。入力住所と 8 桁キー・郵便番号・漢字トークンで類似度を確認。
- 代表者: 敬称/役職/括弧/冗長語を除去。汎用語（氏名/名前/役職/担当/選任/概要など）は除外。会社名・地名・長文は禁止。
- 説明: AIが毎回生成する。本文・優先リンク等から「何をしている会社か」「提供サービス/事業内容」がわかる60〜100文字の文章を1本作る。お問い合わせ/採用/アクセス/予約や住所・電話は含めない。見出しだけ（会社概要/事業概要/法人概要/沿革/会社案内/企業情報）は除外。ニュース/採用/日付行は除外。事業・製造・開発・販売・提供・サービス・運営・支援・施工・設計・製作などの事業キーワードが入る文を優先。
- 上場区分: 許可キーワード（上場/未上場/非上場/市場名/Nasdaq/NYSE など）のみ採用。文章・長文は除外し、15文字超は除外。
- 金額系: 単位（億円/万円/千円/円）を含まないものは除外。長さは 40 文字以内。
- 決算月: `1-12月` を抽出し正規化。
- 設立年: 西暦 4 桁。

## パフォーマンス/スキップ
- `TIME_LIMIT_SEC` を超えたら深掘り・2回目AI・verifyをスキップし `review` で保存（`error_code=timeout`）。
- `no_homepage`: 検索候補が全滅/スコア低の場合は早期終了し後日再処理。
- 公式決定＋主要項目充足時は追加クロールを行わない。
- `RETRY_STATUSES`（デフォルト: review,no_homepage）で再処理順序を制御。pendingが無ければ順にクレーム。

## スクリーンショットの扱い
- 公式判定（judge_official_homepage）はスクショ付き。
- 属性抽出（verify_info）は、電話/住所/代表者が欠けている場合はスクショも添付して精度を優先。優先リンク統合テキストでも不足を補う。
- 通常取得/深掘りはテキスト/HTML主体で、`need_screenshot=True` のときだけ撮影。

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

### 具体的なOK/NG例（AIが判断に迷いやすい項目）
- 住所 OK: `〒123-4567 東京都渋谷区〇〇1-2-3` / `東京都渋谷区〇〇1-2-3`（都道府県から始まる）  
  NG: `アクセスはこちら`, `本社はこちら`, `会社概要`, 郵便番号も都道府県も無い断片。
- 代表者 OK: `山田太郎`, `代表取締役 山田太郎`（役職は前置きでも良い）  
  NG: `氏名`, `名前`, `担当`, `代表者`, `代表者名`, `採用担当`, `スタッフ紹介`, `ニュース`, `会社概要` 等の汎用語やセクション名。
- 説明 OK: `〇〇事業を展開し、△△向けに□□サービスを提供する企業。`（事業内容が1文でわかる）  
  NG: `会社概要`, `法人概要`, `事業概要`, `お問い合わせはこちら`, `採用情報`, `アクセス`, `ニュース`, 日付列のみ、途中で途切れた文。

### 説明(description)の指針
- 1文 60-120文字。句点で終える。業種・事業内容・提供サービスだけを書く。
- 含めない: 問い合わせ/採用/ニュース/挨拶/アクセス/予約、住所・電話・メール、URL。
- 見出しだけの文（会社概要/事業概要/法人概要/沿革/会社案内/企業情報）は description にしない。
```

## Gemini へのプロンプト雛形（user/ツール呼び出し）
```
会社名: {company_name}
住所: {address}
本文: {text_snippet_or_full}  ※ナビ/お問い合わせ/採用/アクセス等は除いてあります
（必要に応じてスクリーンショット参照あり）
上の本文から公式サイトの情報を抽出し、指定のJSONキーで返してください。
```

## 運用メモ
- スキップ/後回し方針: `no_homepage` と `timeout` は別バッチで再処理。`review` は手動確認または緩和設定で再実行。
- ログ: 1社ごとに `elapsed=xx.xs` + フェーズ別 (search/official/deep/ai) を出力し、`PHASE_METRICS_PATH` にも書き出し。`scripts/aggregate_phase_metrics.py` で平均/標準偏差/p95を集計可能。異常な遅延や誤公式はIDとURLを記録してブロックリストに反映。
- ページ取得が `SLOW_PAGE_THRESHOLD_MS` （デフォルト約9s）を超えたホストはログを残して次回スキップ。必要に応じて `SKIP_SLOW_HOSTS=false` でスキップを無効化。
- ブロックワード/ルールは誤りが見つかり次第このドキュメントに追記し、AIにも反映する。
- プロンプトは短く禁止事項を冒頭に置き、few-shotは必要最小限にとどめて揺らぎを抑える。
- 公式判定後に主要項目が埋まれば深掘りを即スキップし、時間を節約する。
- done で電話/住所/代表者が欠けている行だけ後追いしたい場合は `python scripts/requeue_missing_fields.py --fields phone,address,rep --statuses done` で pending に戻す。
- メトリクスは `PHASE_METRICS_PATH` に保存し、`scripts/aggregate_phase_metrics.py` で平均/標準偏差/p95を集計。遅延や誤公式の検出に活用する。
