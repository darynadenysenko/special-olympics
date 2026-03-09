import pandas as pd
from pathlib import Path

# Path to bronze folder
bronze_path = Path("data/bronze")

# Load clubs
clubs = pd.read_excel(bronze_path / "Thomas More Data Clubs.xlsx")
print("CLUBS")
print(clubs.shape)
print(clubs.columns)

# Load certifications
certifications = pd.read_excel(bronze_path / "Thomas More Data Certifications.xlsx")
print("\nCERTIFICATIONS")
print(certifications.shape)
print(certifications.columns)

# Load results file
results_2015 = pd.read_excel(bronze_path / "Thomas More Results 2015.xlsx")
print("\nRESULTS 2015")
print(results_2015.shape)
print(results_2015.columns)