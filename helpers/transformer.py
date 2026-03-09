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
            

        # clean zipcode
        zipcode = df["Zipcode"]
        zipcode = zipcode.fillna("") # replace missing values with empty string
        zipcode = zipcode.astype(str).str.strip() #convert to string & remove spaces
        df["Zipcode"] = zipcode

        # remove duplicate rows
        df = df.drop_duplicates()

        # reset the index after cleaning
        df = df.reset_index(drop=True)

        return df