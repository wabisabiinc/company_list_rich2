import csv
from typing import Optional, Dict

class CsvDataManager:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.input_file = open(self.input_path, newline='', encoding='utf-8')
        self.output_file = open(self.output_path, mode='w', newline='', encoding='utf-8')
        self.reader = csv.DictReader(self.input_file)
        self.fieldnames = self.reader.fieldnames + ['homepage', 'phone', 'found_address']
        self.writer = csv.DictWriter(self.output_file, fieldnames=self.fieldnames)
        self.writer.writeheader()
        self.rows = list(self.reader)
        self.index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.input_file.close()
        self.output_file.close()

    def get_next_company(self) -> Optional[Dict[str, str]]:
        if self.index < len(self.rows):
            company = self.rows[self.index]
            self.index += 1
            return company
        else:
            return None

    def save_company_data(self, company: Dict[str, str]):
        self.writer.writerow(company)
        self.output_file.flush()
