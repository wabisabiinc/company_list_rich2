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
```

この状態で `python main.py` を実行すれば `ModuleNotFoundError: dotenv` のようなエラーは発生しません。

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
