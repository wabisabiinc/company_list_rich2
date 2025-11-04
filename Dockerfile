# syntax=docker/dockerfile:1

# Playwright の公式 Python イメージ（ブラウザと依存は同梱）
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# --- OS パッケージ ---
# sqlite3 CLI を追加（DB中身を直接確認できるように）
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
 && rm -rf /var/lib/apt/lists/*

# --- Python 依存 ---
# 依存を先に入れてビルドキャッシュを効かせる
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install -r requirements.txt \
 # Playwright の Python パッケージ版にブラウザを合わせる
 && playwright install --with-deps

# --- アプリ本体 ---
COPY . .

# 生成物の置き場を用意（ログ/DB をホストへマウントする前提でも安全）
RUN mkdir -p /app/logs /app/data

# デフォルトはシェルで起動（必要に応じて python main.py など実行）
CMD ["bash"]
