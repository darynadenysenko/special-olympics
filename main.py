from helpers.extractor import DataExtractor
from helpers.transformer import DataTransformer
from helpers.loader import DataLoader
import pandas as pd

extractor = DataExtractor()
transformer = DataTransformer()
loader = DataLoader()

#load raw data
clubs = extractor.load_excel("data/bronze/Thomas More Data Clubs.xlsx")
certifications = extractor.load_excel("data/bronze/Thomas More Data Certifications.xlsx")
results = extractor.load_all_results("data/bronze")

#combine all results into one dataframe
all_results = pd.concat(results.values(), ignore_index=True)

#clean datasets
clubs_clean = transformer.clean_clubs(clubs)
certifications_clean = transformer.clean_certifications(certifications)
results_clean = transformer.clean_results(all_results)

 
#save cleaned datasets to silver layer
loader.save_csv(clubs_clean, "data/silver/clubs_cleaned.csv")
loader.save_csv(certifications_clean, "data/silver/certifications_cleaned.csv")
loader.save_csv(results_clean, "data/silver/results_cleaned.csv")



# build dim_person
dim_person = transformer.build_dim_person(certifications_clean)

# save to gold layer
loader.save_csv(dim_person, "data/gold/dim_person.csv")


# build dim_club
dim_club = transformer.build_dim_club(clubs_clean)

print("DimClub shape:", dim_club.shape)
print(dim_club.head())

# save to gold layer
loader.save_csv(dim_club, "data/gold/dim_club.csv")
