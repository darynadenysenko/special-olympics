from helpers.extractor import DataExtractor

extractor = DataExtractor()

clubs = extractor.load_excel("data/bronze/Thomas More Data Clubs.xlsx")
print(clubs.shape)

certifications = extractor.load_excel("data/bronze/Thomas More Data Certifications.xlsx")
print(certifications.shape)

results = extractor.load_all_results("data/bronze")
print(len(results))