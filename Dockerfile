# Playwright公式のPython＋全Linux依存入りイメージを利用
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

WORKDIR /app

# Python依存インストール
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 必要な全ソースコードをコピー（src/ tests/ main.py など全部）
COPY . .

# bashで起動（他のCMDにしたい場合はここを変える）
CMD ["bash"]
