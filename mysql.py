import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")

# list of all gold tables
tables = [
    "dim_person",
    "dim_club",
    "dim_sport",
    "dim_role",
    "dim_year",
    "dim_event",
    "dim_person_type",
    "dim_certification_type",
    "fact_person_certification",
    "fact_results",
    "fact_club_participation"
]

# load each csv into MySQL
for table_name in tables:
    file_path = f"data/gold/{table_name}.csv"
    df = pd.read_csv(file_path)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

print("All tables loaded into MySQL successfully!")