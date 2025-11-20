import csv
from pathlib import Path

import pytest

from src.reference_checker import ReferenceChecker


def write_reference_csv(tmp_path: Path) -> Path:
    path = tmp_path / "reference.csv"
    fieldnames = ["法人番号", "ホームページ", "電話番号", "住所"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "法人番号": "1010001001458",
            "ホームページ": "https://sites.google.com/view/otakizai-n/",
            "電話番号": "03-1111-2222",
            "住所": "〒113-0033 東京都文京区本郷3-35",
        })
    return path


def test_reference_checker_match(tmp_path: Path):
    csv_path = write_reference_csv(tmp_path)
    checker = ReferenceChecker.from_csvs([str(csv_path)])

    company = {
        "corporate_number": "1010001001458",
        "homepage": "https://sites.google.com/view/otakizai-n/",
        "phone": "03-1111-2222",
        "found_address": "〒113-0033 東京都文京区本郷3-35",
    }
    result = checker.evaluate(company)
    assert result["accuracy_homepage"] == "match"
    assert result["accuracy_phone"] == "match"
    assert result["accuracy_address"] == "match"


def test_reference_checker_missing_values(tmp_path: Path):
    csv_path = write_reference_csv(tmp_path)
    checker = ReferenceChecker.from_csvs([str(csv_path)])
    company = {
        "corporate_number_norm": "1010001001458",
        "homepage": "",
        "phone": "",
        "found_address": "",
    }
    result = checker.evaluate(company)
    assert result["accuracy_homepage"] == "missing_new"
    assert result["accuracy_phone"] == "missing_new"
    assert result["accuracy_address"] == "missing_new"


def test_reference_checker_returns_empty_when_not_found(tmp_path: Path):
    csv_path = write_reference_csv(tmp_path)
    checker = ReferenceChecker.from_csvs([str(csv_path)])
    company = {
        "corporate_number": "9999999999999",
        "homepage": "https://example.com",
    }
    assert checker.evaluate(company) == {}
