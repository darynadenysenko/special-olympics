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
        df.columns = df.columns.str.strip()

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

        df["DOB"] = pd.to_datetime(df["DOB"], dayfirst=True)

        df["Score"] = df["Score"].astype(str).str.strip()
        df["Score_Numeric"] = pd.to_numeric(df["Score"].str.replace("points", ""), errors="coerce")

        df["Place"] = df["Place"].astype(str).str.strip()
        df["Place_Numeric"] = pd.to_numeric(df["Place"].str.extract(r"(\d+)")[0], errors="coerce")

        df = df.reset_index(drop=True)

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

    

    # build dim_person_type table
    def build_dim_person_type(self, df):

        df = df.copy()

        dim_person_type = df[["Person type"]]
        dim_person_type = dim_person_type[dim_person_type["Person type"] != ""]

        dim_person_type = dim_person_type.drop_duplicates()

        dim_person_type = dim_person_type.reset_index(drop=True)

        dim_person_type["person_type_key"] = dim_person_type.index + 1 #surrogate key

        dim_person_type = dim_person_type.rename(columns={
            "Person type": "person_type_name"
        })

        dim_person_type = dim_person_type[
            ["person_type_key", "person_type_name"]
        ]

        return dim_person_type

    

    # build dim_certification_type table
    def build_dim_certification_type(self):

        certification_types = [
            "Mental Handicap",
            "Parents Consent",
            "HAP",
            "Unified Partner"
        ]

        dim_certification_type = pd.DataFrame({
            "certification_type_name": certification_types
        })

        #surrogate key
        dim_certification_type["certification_type_key"] = dim_certification_type.index + 1

        # reorder columns
        dim_certification_type = dim_certification_type[
            ["certification_type_key", "certification_type_name"]
        ]

        return dim_certification_type

    

    # build fact_person_certification table
    def build_fact_person_certification(self,certifications_df,dim_person,dim_club,dim_person_type,dim_certification_type):

        df = certifications_df.copy()

        df = df[[
            "Club",
            "Code",
            "Person type",
            "Mental Handicap (SOB has this certificate)",
            "Parents Consent (SOB has this certificate)",
            "HAP (SOB has this certificate)",
            "Unified Partner (SOB has this certificate)"
        ]]

        # rename certificate columns
        df = df.rename(columns={
            "Mental Handicap (SOB has this certificate)": "Mental Handicap",
            "Parents Consent (SOB has this certificate)": "Parents Consent",
            "HAP (SOB has this certificate)": "HAP",
            "Unified Partner (SOB has this certificate)": "Unified Partner"
        })

        # turn certificate columns into rows
        fact_person_certification = df.melt(
            id_vars=["Club", "Code", "Person type"],
            value_vars=["Mental Handicap", "Parents Consent", "HAP", "Unified Partner"],
            var_name="certification_type_name",
            value_name="has_certificate"
        )

        fact_person_certification["has_certificate"] = pd.to_numeric(
        fact_person_certification["has_certificate"], errors="coerce")


        fact_person_certification = fact_person_certification[fact_person_certification["has_certificate"] == 1.0]
        
        # join with dim_person
        fact_person_certification = fact_person_certification.merge(
            dim_person,
            left_on="Code",
            right_on="person_code",
            how="left"
        )

        # join with dim_club
        fact_person_certification = fact_person_certification.merge(
            dim_club,
            left_on="Club",
            right_on="club_name",
            how="left"
        )

        # join with dim_person_type
        fact_person_certification = fact_person_certification.merge(
            dim_person_type,
            left_on="Person type",
            right_on="person_type_name",
            how="left"
        )

        # join with dim_certification_type
        fact_person_certification = fact_person_certification.merge(
            dim_certification_type,
            on="certification_type_name",
            how="left"
        )

        fact_person_certification["person_key"] = fact_person_certification["person_key"].astype("Int64")
        fact_person_certification["club_key"] = fact_person_certification["club_key"].astype("Int64")
        fact_person_certification["certification_type_key"] = fact_person_certification["certification_type_key"].astype("Int64")
        fact_person_certification["person_type_key"] = fact_person_certification["person_type_key"].astype("Int64")
        
        # final columns
        fact_person_certification = fact_person_certification[[
            "person_key",
            "club_key",
            "certification_type_key",
            "person_type_key"
        ]]


        fact_person_certification["has_certificate"] = 1

        # remove duplicates
        fact_person_certification = fact_person_certification.drop_duplicates()

        # reset index
        fact_person_certification = fact_person_certification.reset_index(drop=True)

        # create surrogate key
        fact_person_certification["athlete_certification_key"] = fact_person_certification.index + 1

        # reorder columns
        fact_person_certification = fact_person_certification[[
            "athlete_certification_key",
            "person_key",
            "club_key",
            "certification_type_key",
            "person_type_key",
            "has_certificate"
        ]]

        return fact_person_certification


    # build fact_results table
    def build_fact_results(
        self,
        results_df,
        dim_person,
        dim_club,
        dim_sport,
        dim_event,
        dim_role,
        dim_year
    ):

        df = results_df.copy()

        # keep needed columns
        df = df[[
            "Code",
            "Club",
            "Sport",
            "Event",
            "Role",
            "Year",
            "Place",
            "Place_Numeric",
            "Score",
            "Score_Numeric",
            "Age"
        ]]

        # join with dim_person
        df = df.merge(
            dim_person[["person_key", "person_code"]],
            left_on="Code",
            right_on="person_code",
            how="left"
        )

        # join with dim_club
        df = df.merge(
            dim_club[["club_key", "club_name"]],
            left_on="Club",
            right_on="club_name",
            how="left"
        )

        # join with dim_sport
        df = df.merge(
            dim_sport[["sport_key", "sport_name"]],
            left_on="Sport",
            right_on="sport_name",
            how="left"
        )

        # join with dim_event
        df = df.merge(
            dim_event[["event_key", "event_name"]],
            left_on="Event",
            right_on="event_name",
            how="left"
        )

        # join with dim_role
        df = df.merge(
            dim_role[["role_key", "role_name"]],
            left_on="Role",
            right_on="role_name",
            how="left"
        )

        # join with dim_year
        df = df.merge(
            dim_year[["year_key", "year"]],
            left_on="Year",
            right_on="year",
            how="left"
        )

        # convert keys to integers
        df["person_key"] = df["person_key"].astype("Int64")
        df["club_key"] = df["club_key"].astype("Int64")
        df["sport_key"] = df["sport_key"].astype("Int64")
        df["event_key"] = df["event_key"].astype("Int64")
        df["role_key"] = df["role_key"].astype("Int64")
        df["year_key"] = df["year_key"].astype("Int64")

        # keep final columns
        fact_results = df[[
            "person_key",
            "club_key",
            "sport_key",
            "event_key",
            "role_key",
            "year_key",
            "Place",
            "Place_Numeric",
            "Score",
            "Score_Numeric",
            "Age"
        ]]

        # rename measures
        fact_results = fact_results.rename(columns={
            "Place": "place",
            "Place_Numeric": "place_numeric",
            "Score": "score",
            "Score_Numeric": "score_numeric",
            "Age": "age"
        })

        # remove duplicates
        fact_results = fact_results.drop_duplicates()

        # reset index
        fact_results = fact_results.reset_index(drop=True)

        # create surrogate key
        fact_results["result_key"] = fact_results.index + 1

        # reorder columns
        fact_results = fact_results[[
            "result_key",
            "person_key",
            "club_key",
            "sport_key",
            "event_key",
            "role_key",
            "year_key",
            "place",
            "place_numeric",
            "score",
            "score_numeric",
            "age"
        ]]

        return fact_results


    #build fact_club_participation table
    def build_fact_club_participation(self, clubs_df, dim_club, dim_year):

        df = clubs_df.copy()
        dim_club = dim_club.copy()
        dim_year = dim_year.copy()

        df = df[[
            "Name",
            "Participation Games 2015",
            "Participation Games 2016",
            "Participation Games 2017",
            "Participation Games 2018",
            "Participation Games 2019",
            "Participation Games 2022",
            "Participation Games 2023",
            "Participation Games 2024",
            "Participation Games 2025"
        ]]

        # rename
        df = df.rename(columns={
            "Name": "club_name",
            "Participation Games 2015": "2015",
            "Participation Games 2016": "2016",
            "Participation Games 2017": "2017",
            "Participation Games 2018": "2018",
            "Participation Games 2019": "2019",
            "Participation Games 2022": "2022",
            "Participation Games 2023": "2023",
            "Participation Games 2024": "2024",
            "Participation Games 2025": "2025"
        })


        fact_club_participation = df.melt(
            id_vars=["club_name"],
            var_name="year",
            value_name="participated"
        )

        fact_club_participation["participated"] = pd.to_numeric(
            fact_club_participation["participated"],
            errors="coerce"
        )

        fact_club_participation = fact_club_participation[
            fact_club_participation["participated"] == 1
        ]

        fact_club_participation["year"] = fact_club_participation["year"].astype(int)
        dim_year["year"] = dim_year["year"].astype(int)

        # join with dim_club
        fact_club_participation = fact_club_participation.merge(
            dim_club[["club_key", "club_name"]],
            on="club_name",
            how="left"
        )

        # join with dim_year
        fact_club_participation = fact_club_participation.merge(
            dim_year[["year_key", "year"]],
            on="year",
            how="left"
        )

        # convert keys to integer type
        fact_club_participation["club_key"] = fact_club_participation["club_key"].astype("Int64")
        fact_club_participation["year_key"] = fact_club_participation["year_key"].astype("Int64")

        #final columns
        fact_club_participation = fact_club_participation[[
            "club_key",
            "year_key"
        ]]

        fact_club_participation["participated"] = 1
        fact_club_participation = fact_club_participation.drop_duplicates()
        fact_club_participation = fact_club_participation.reset_index(drop=True)

        # surrogate key
        fact_club_participation["club_participation_key"] = fact_club_participation.index + 1

        # reorder
        fact_club_participation = fact_club_participation[[
            "club_participation_key",
            "club_key",
            "year_key",
            "participated"
        ]]


        return fact_club_participation