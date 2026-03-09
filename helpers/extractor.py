from pathlib import Path
import pandas as pd


class DataExtractor:

    def load_excel(self, path):
        return pd.read_excel(path)

    def load_all_results(self, bronze_path):
        bronze_path = Path(bronze_path)

        results = {}

        for file in bronze_path.glob("*.xlsx"):
            if "results" in file.name.lower():
                results[file.name] = pd.read_excel(file)

        return results