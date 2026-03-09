#This class saves cleaned data to files in the silver layer

from pathlib import Path

class DataLoader:

    #save a dataframe as csv
    def save_csv(self, df, path):
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True) #create folder if it doesn't exist
        df.to_csv(output_path, index=False) #save