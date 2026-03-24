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



# ------dim_person------

# build 
dim_person = transformer.build_dim_person(certifications_clean)
# save to gold layer
loader.save_csv(dim_person, "data/gold/dim_person.csv")


# ------dim_club------

# build 
dim_club = transformer.build_dim_club(clubs_clean)
# save to gold layer
loader.save_csv(dim_club, "data/gold/dim_club.csv")


# ------dim_sport------

# build
dim_sport = transformer.build_dim_sport(results_clean)
# save to gold layer
loader.save_csv(dim_sport, "data/gold/dim_sport.csv")


# ------dim_role------

# build 
dim_role = transformer.build_dim_role(results_clean)
# save to gold layer
loader.save_csv(dim_role, "data/gold/dim_role.csv")


# ------dim_year------

# build
dim_year = transformer.build_dim_year(results_clean)
# save to gold layer
loader.save_csv(dim_year, "data/gold/dim_year.csv")


#------dim_event------
# build
dim_event = transformer.build_dim_event(results_clean, dim_sport)
# save to gold layer
loader.save_csv(dim_event, "data/gold/dim_event.csv")


#------dim_person_type------
# build
dim_person_type = transformer.build_dim_person_type(certifications_clean)
# save to gold layer
loader.save_csv(dim_person_type, "data/gold/dim_person_type.csv")


#------dim_certification_type------

# build 
dim_certification_type = transformer.build_dim_certification_type()
# save to gold layer
loader.save_csv(dim_certification_type, "data/gold/dim_certification_type.csv")



#------fact_person_certification------

# build 
fact_person_certification = transformer.build_fact_person_certification(
    certifications_clean,
    dim_person,
    dim_club,
    dim_person_type,
    dim_certification_type
)
# save to gold layer
loader.save_csv(fact_person_certification, "data/gold/fact_person_certification.csv")


#------fact_results------
# build
fact_results = transformer.build_fact_results(
    results_clean,
    dim_person,
    dim_club,
    dim_sport,
    dim_event,
    dim_role,
    dim_year
)

# save to gold layer
loader.save_csv(fact_results, "data/gold/fact_results.csv")


#------fact_club_participation------
# build 
fact_club_participation = transformer.build_fact_club_participation(
    clubs_clean,
    dim_club,
    dim_year
)

# save to gold
loader.save_csv(fact_club_participation, "data/gold/fact_club_participation.csv")


