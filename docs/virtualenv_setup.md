# 仮想環境の自動化セットアップ

このプロジェクトを `python main.py` で問題なく起動するには、リポジトリ直下に仮想環境を作成して依存を入れるのが最も手軽です。以下の手順を VS Code などで一度だけ済ませれば、以後ターミナルを開けば `python` コマンドが `.venv` に切り替わるはずです。

## 1. 仮想環境を作る（WSL でも同じ）

```bash
cd ~/projects/company_list_rich
python3 -m venv .venv
```

## 2. 仮想環境を有効化して依存をインストール

```bash
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium
```

この状態で `python main.py` を実行すれば `ModuleNotFoundError: dotenv` のようなエラーは発生しません。
`playwright install` を省略すると `Executable doesn't exist ... Please run: playwright install` のエラーになります。

## デバッグ（あるのに取れない原因調査）

- `EXTRACT_DEBUG_JSONL_PATH` を設定すると、各社の抽出/採用/除外理由をJSONLで出力できます（例: `.env` に `EXTRACT_DEBUG_JSONL_PATH=logs/extract_debug.jsonl`）
- `VERIFY_DOCS_REQUIRED`（デフォルト `true`）は、AI公式採用時に「会社概要/連絡先」ページを最小限だけ事前取得して `verify=docs` の精度と速度を上げます
- `REP_STRICT_SOURCES`（デフォルト `true`）は、代表者名を構造化ソース（table/dl/label/JSON-LD等）中心に制限して誤抽出を減らします
- description を毎回AIで作りたい場合は `AI_DESCRIPTION_ALWAYS=true`（既定true）と `AI_DESCRIPTION_FALLBACK_CALL=true` を使います（既存値は保持しません）
- 公式サイトが無い/不明なケースで誤ったURLを `homepage` に保存したくない場合は `REQUIRE_OFFICIAL_HOMEPAGE=true`（既定true）を維持し、暫定URLは `final_homepage/provisional_homepage` で確認します

## 3. VS Code と連携して毎回自動で有効化

VS Code のターミナルから自動的に `.venv` を読み込むには、プロジェクト内に `.vscode/settings.json` を作成し、以下を追加します。もし `settings.json` が既にある場合は該当キーだけを追記してください。

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.terminal.activateEnvironment": true
}
```

この設定によって VS Code のターミナル起動時や実行時に `.venv/bin/python` が選択され、`python` コマンドが仮想環境版になります。

## 4. 以降の運用

- 仮想環境を有効化した状態で `python main.py` を実行すると `.env` を読み込んでスクレイピングが進む状態になります。
- 仮想環境を破棄したくなったら `.venv` フォルダを削除し、再度 `python3 -m venv .venv` からやり直してください。

## 5. 複数台PCで並列に処理する（推奨: DB をコピーして ID 範囲を固定）

SQLite は「同一ファイルを複数PCからネットワーク共有で同時更新」するとロック/破損リスクがあるため、基本は **各PCで `data/companies.db` をローカルに持って処理**し、最後に **成果だけをマージ**する運用を推奨します。

### 5-1. 各PC共通: リポジトリを用意して起動できる状態にする

1) リポジトリを取得（例）

```bash
git clone <このリポジトリのURL> company_list_rich
cd company_list_rich
```

2) 仮想環境と依存関係（初回のみ）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium
```

3) `.env` を作る（APIキーは各PCに安全に配布）

```bash
cp .env.example .env
```

`.env` の `GEMINI_API_KEY` を設定してください（運用上は「PC/ユーザーごとに別キー」を推奨。共有すると漏洩時の影響が大きくなります）。必要に応じて `DATABASE_URL` なども調整します。

### 5-2. マスターPC: DB を分配する

1) マスター側で最新の `data/companies.db` を確定（取り込み・整形がある場合は先に済ませる）
2) 各ワーカーPCへ `data/companies.db` をコピーします（例: USB / 社内共有 / `scp` など）

ワーカー側は `company_list_rich/data/companies.db` が存在する状態にしてください。

### 5-3. 分担範囲を決める（ID_MIN / ID_MAX）

マスターPCで `companies` の最大IDを確認します。

```bash
sqlite3 data/companies.db "select max(id) from companies;"
```

例えば「4台で分担」なら、ざっくり等分して各PCに割り当てます（例）:
- PC1: `ID_MIN=1` `ID_MAX=25000`
- PC2: `ID_MIN=25001` `ID_MAX=50000`
- PC3: `ID_MIN=50001` `ID_MAX=75000`
- PC4: `ID_MIN=75001` `ID_MAX=100000`

※ 実際の `max(id)` に合わせて調整してください。

### 5-4. ワーカーPC: 自分の範囲だけ処理して走らせる

1) venv を有効化

```bash
source .venv/bin/activate
```

Windows（PowerShell）の場合:

```powershell
.\.venv\Scripts\Activate.ps1
```

2) そのPC用の `WORKER_ID` と ID 範囲を指定して実行（例）

```bash
WORKER_ID=pc1 ID_MIN=1 ID_MAX=25000 FETCH_CONCURRENCY=4 python main.py
```

Windows（PowerShell）の場合:

```powershell
$env:WORKER_ID="pc1"
$env:ID_MIN="1"
$env:ID_MAX="25000"
$env:FETCH_CONCURRENCY="4"
python .\main.py
```

- `WORKER_ID` はログ/ロック識別子なので各PCでユニークにしてください（例: `pc1`, `pc2`）。
- 速度調整は `FETCH_CONCURRENCY`（既定3）で行います。APIや回線が詰まるなら下げてください。
- GUIを出したくない場合は `HEADLESS=true`（既定true）です。

バックグラウンド実行したい場合（Linux/WSL例）:

```bash
mkdir -p logs
WORKER_ID=pc1 ID_MIN=1 ID_MAX=25000 FETCH_CONCURRENCY=4 \
  nohup python main.py > logs/pc1.log 2>&1 &
tail -f logs/pc1.log
```

### 5-5. マスターPC: ワーカーの DB をマージする（処理完了後）

前提: 各PCが **自分の割り当て範囲だけ**を処理し、その範囲が他PCと重複していないこと。

1) マスター側でバックアップを取ります。

```bash
cp data/companies.db data/companies.db.bak
```

2) ワーカーDBをマスターへ集めます（例: `backups/worker_pc1.db` などに配置）
3) 範囲ごとに `companies` を上書きマージします（例: PC1の成果を反映）

```bash
sqlite3 data/companies.db <<'SQL'
ATTACH 'backups/worker_pc1.db' AS w;
INSERT OR REPLACE INTO companies
  SELECT * FROM w.companies WHERE id BETWEEN 1 AND 25000;
INSERT OR REPLACE INTO url_flags
  SELECT * FROM w.url_flags;
DETACH w;
SQL
```

PC2/PC3…も同様に実行し、それぞれ `id BETWEEN ...` の範囲だけ変えてください。

## 6. 1台のPC内で並列化したい場合（run_sharded）

同一PCでの並列起動は、Linux/WSL/macOS は `run_sharded.sh`、Windows は `run_sharded.ps1` を使うのが簡単です。

```bash
# 例: 4プロセス / 各プロセスの並列4（合計16並列）
bash run_sharded.sh 4 4
```

Windows（PowerShell）の例:

```powershell
# 例: 4ワーカー / 各ワーカーの並列4
.\run_sharded.ps1 -Workers 4 -PerWorkerConcurrency 4
```
