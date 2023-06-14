import pandas as pd
import os 
from ETL_psql.Transform.Rename_col_df import column_dict

# ---------------------------------- Parent class ---------------------------------- #

class Transform_df : # Parent class for transformation of dataframe
    def __init__(self, Name, filePath = "") :
        self.root_dir = "/opt/airflow/Input_data";
        self.write_dir = "/opt/airflow/Transformed_data";
        
        try : 
            path = os.path.join(self.root_dir, filePath);
            self.df = pd.read_csv(path);
        except :
            self.df = pd.DataFrame();
        
        self.name = Name;
       
        self.clean();
        self.extra_var();
        self.transform();
        self.rename_col_df();
        self.write_csv();

    def extra_var(self) : # Additional variables and setting
        pass

    def get_primary_column(self) :  # Primary key columns mostly contain "ID" in their name
        for col in self.df.columns :
            if ("ID" in col) : return col;

    def clean(self) : # Drop duplicate values in primary key columns (still keep one)
        self.df.drop_duplicates(subset = [self.get_primary_column()], keep = 'first', inplace = True);

    def transform(self) : # Transform data
        pass

    def rename_col_df(self) :
        # This function renames all columns to fit postgreSQL database format
        # details can be found in file `Rename_col_df.py`
        self.df.rename(columns = column_dict[self.name], inplace = True);

    def write_csv(self) :
        write_path = os.path.join(self.write_dir, self.name + ".csv");
        self.df.to_csv(write_path, index = False);