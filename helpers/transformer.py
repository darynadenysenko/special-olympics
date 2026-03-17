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



    #clean the results dataframe
    def clean_results(self, df):

        df = df.copy()
        df.columns = df.columns.str.strip() #remove spaces in column names

        text_columns = [
            "Code",
            "Club",
            "Sport",
            "Role",
            "Gender",
            "Event"
        ]

        for col in text_columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

        
        df["DOB"] = pd.to_datetime(df["DOB"], dayfirst=True) #convert DOB to datetime

        df["Score"] = df["Score"].astype(str).str.replace("points", "").str.strip() #clean score column

        df["Score"] = pd.to_numeric(df["Score"], errors="coerce") # convert score to numeric

        # clean place column 
        df["Place"] = df["Place"].astype(str).str.extract(r"(\d+)")

        # convert place to numeric
        df["Place"] = pd.to_numeric(df["Place"], errors="coerce")

        # remove duplicate rows
        #df = df.drop_duplicates()
       
        df = df.reset_index(drop=True)  # reset index after cleaning

        return df


    # build dim_person table

    def build_dim_person(self, df):

        df = df.copy()
        dim_person = df[["Code", "Gender", "DOB"]]

        dim_person = dim_person.drop_duplicates()

        dim_person = dim_person.reset_index(drop=True) #reset index after dropping duplicates

        dim_person["person_key"] = dim_person.index + 1 #surrogate key 

        dim_person = dim_person.rename(columns={
            "Code": "person_code"
        })

        # reorder columns
        dim_person = dim_person[
            ["person_key", "person_code", "DOB", "Gender"]
        ]

        return dim_person
    


    #build dim_club table
    def build_dim_club(self, df):

        df = df.copy()

        dim_club = df[[
            "Group number",
            "Name",
            "Primary language",
            "Address (Street and Number)",
            "Zipcode",
            "City",
            "Province",
            "Country"
        ]]

        dim_club = dim_club.drop_duplicates()

        dim_club = dim_club.reset_index(drop=True)

        dim_club["club_key"] = dim_club.index + 1 #surrogate key

        #rename columns
        dim_club = dim_club.rename(columns={
            "Group number": "group_number",
            "Name": "club_name",
            "Primary language": "primary_language",
            "Address (Street and Number)": "address",
            "Zipcode": "zipcode",
            "City": "city",
            "Province": "province",
            "Country": "country"
        })

        #reorder 
        dim_club = dim_club[
            [
                "club_key",
                "group_number",
                "club_name",
                "primary_language",
                "address",
                "zipcode",
                "city",
                "province",
                "country"
            ]
        ]

        return dim_club

    

    #build dim_sport table
    def build_dim_sport(self, df):

        df = df.copy()

        dim_sport = df[["Sport"]]
        dim_sport = dim_sport.drop_duplicates()

        dim_sport = dim_sport.reset_index(drop=True)

        dim_sport["sport_key"] = dim_sport.index + 1 #surrogate key

        #rename column
        dim_sport = dim_sport.rename(columns={
            "Sport": "sport_name"
        })

        #reorder
        dim_sport = dim_sport[["sport_key", "sport_name"]]

        return dim_sport
    

    # build dim_role table
    def build_dim_role(self, df):

        df = df.copy()

        dim_role = df[["Role"]]
        dim_role = dim_role.drop_duplicates()
        dim_role = dim_role[dim_role["Role"] != ""]
        dim_role = dim_role.reset_index(drop=True)

        dim_role["role_key"] = dim_role.index + 1 #surrogate key

        # rename column
        dim_role = dim_role.rename(columns={
            "Role": "role_name"
        })

        # reorder 
        dim_role = dim_role[["role_key", "role_name"]]

        return dim_role



    # build dim_year table
    def build_dim_year(self, df):

        df = df.copy()

        dim_year = df[["Year"]]
        dim_year = dim_year.drop_duplicates()
        dim_year = dim_year.reset_index(drop=True)

        dim_year["year_key"] = dim_year.index + 1 #surrogate key

        # rename column
        dim_year = dim_year.rename(columns={
            "Year": "year"
        })

        # reorder 
        dim_year = dim_year[["year_key", "year"]]

        return dim_year

    
    # build dim_event table

    def build_dim_event(self, df, dim_sport):

        df = df.copy()
        dim_sport = dim_sport.copy()

        dim_event = df[["Event", "Sport"]]

        dim_event = dim_event[dim_event["Event"] != ""]
        dim_event = dim_event.drop_duplicates()

        # join with dim_sport to get sport_key
        dim_event = dim_event.merge(
            dim_sport,
            left_on="Sport",
            right_on="sport_name",
            how="left"
        )

        dim_event = dim_event[["Event", "sport_key"]]


        dim_event = dim_event.reset_index(drop=True)

        dim_event["event_key"] = dim_event.index + 1 #surrogate key

        # rename 
        dim_event = dim_event.rename(columns={
            "Event": "event_name"
        })

        # reorder 
        dim_event = dim_event[["event_key", "event_name", "sport_key"]]

        return dim_event