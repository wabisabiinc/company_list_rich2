# conftest.py
import sys
import os

# このファイルが置いてあるディレクトリ（プロジェクトルート）を起点に
# src フォルダへの絶対パスを作成し、sys.path の先頭に追加
ROOT = os.path.dirname(__file__)
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)
