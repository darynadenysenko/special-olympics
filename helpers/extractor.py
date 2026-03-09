from pathlib import Path
import pandas as pd


class DataExtractor:

    def load_excel(self, path):
        return pd.read_excel(path)

    def load_all_results(self, bronze_path):
        bronze_path = Path(bronze_path)

        results = {}

        for file in bronze_path.glob("*.xlsx"):
            file_name = file.name.lower()

            if "result" in file_name:
                df = pd.read_excel(file)

                year = "".join(char for char in file.name if char.isdigit())
                df["Year"] = year

                results[file.name] = df

        return results