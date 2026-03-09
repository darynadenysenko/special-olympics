from helpers.extractor import DataExtractor
import pandas as pd

extractor = DataExtractor()

clubs = extractor.load_excel("data/bronze/Thomas More Data Clubs.xlsx")
print(clubs.shape)

certifications = extractor.load_excel("data/bronze/Thomas More Data Certifications.xlsx")
print(certifications.shape)

results = extractor.load_all_results("data/bronze")
print(len(results))

all_results = pd.concat(results.values(), ignore_index=True)

print(all_results.shape)
print(all_results.columns)
print(all_results[["Code", "Club", "Sport", "Event", "Year"]].head())