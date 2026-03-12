#This class is used to clean the data after it is loaded from the raw files.
import pandas as pd

class DataTransformer:

    #clean the clubs dataframe
    def clean_clubs(self, df):
        
        df = df.copy()
        df.columns = df.columns.str.strip() #remove spaces in column names

        text_columns = [
            "Name",
            "Primary language",
            "Address (Street and Number)",
            "City",
            "Province",
            "Country"
        ]

        for col in text_columns:
            df[col] = df[col].fillna("").astype(str).str.strip() #remove spaces (if value is missing then first replace it with an empty string)
            

        #clean zipcode
        zipcode = df["Zipcode"]
        zipcode = zipcode.fillna("") # replace missing values with empty string
        zipcode = zipcode.astype(str).str.strip() #convert to string & remove spaces
        df["Zipcode"] = zipcode

        #remove duplicate rows
        df = df.drop_duplicates()

        #reset index after cleaning
        df = df.reset_index(drop=True)

        return df
    

    #clean the certifications dataframe
    def clean_certifications(self, df):

        df = df.copy()
        df.columns = df.columns.str.strip() #remove spaces in column names

        text_columns = [
            "Club",
            "Code",
            "Person type",
            "Gender",
            "Mental Handicap (SOB has this certificate)",
            "Parents Consent (SOB has this certificate)",
            "HAP (SOB has this certificate)",
            "Unified Partner (SOB has this certificate)"
        ]

        for col in text_columns:
            df[col] = df[col].fillna("").astype(str).str.strip()


        df["DOB"] = pd.to_datetime(df["DOB"], dayfirst=True, errors="coerce") #convert DOB to datetime

        #age 0 --> missing value
        df["Age"] = df["Age"].replace(0, pd.NA)

        #remove duplicate rows 
        df = df.drop_duplicates()

        #reset index after cleaning
        df = df.reset_index(drop=True)
        return df