# tests/test_csv_data_manager.py
import sys
import os
import csv
import pytest

# srcフォルダをimportパスに追加
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from csv_data_manager import CsvDataManager

@pytest.fixture
def sample_csv(tmp_path):
    data = tmp_path / "in.csv"
    data.write_text("id,company_name,address\n1,AAA,Tokyo\n")
    return str(data)

def test_csv_roundtrip(sample_csv, tmp_path):
    out = tmp_path / "out.csv"
    mgr = CsvDataManager(sample_csv, str(out))
    rec = mgr.get_next_company()
    assert rec["company_name"] == "AAA"
    rec.update({"homepage": "/", "phone": "", "found_address": ""})
    mgr.save_company_data(rec)
    with open(out, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert rows[0]["company_name"] == "AAA"
    assert "homepage" in reader.fieldnames
