import csv
from typing import Dict, Tuple

def load_listoss_data(csv_paths: list) -> Dict[Tuple[str, str], dict]:
    """
    CSVファイルからリストスのデータをロードし、会社名と住所をキーとする辞書形式で返す
    """
    listoss_data = {}
    
    for csv_path in csv_paths:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                company_name = row["会社名"].strip()
                address = row["住所"].strip()
                phone = row.get("電話番号", "").strip()
                homepage = row.get("ホームページ", "").strip()
                
                # 会社名と住所をキーにして情報を格納
                key = (company_name, address)
                listoss_data[key] = {
                    "addr": address,
                    "phone": phone,
                    "hp": homepage
                }

    return listoss_data
