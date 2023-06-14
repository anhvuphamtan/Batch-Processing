from ETL_psql.Transform.Transform import Transform_df

# ---------------------------------- Child class Customer ---------------------------------- #
 
class Transform_customer_df(Transform_df) : # Transfrom customers dataframe class    
    def transform(self) : 
        # Drop columns below since these information are stored in location_df
        self.df.drop(columns = ['City', 'State', 'Country'], inplace = True);


def Transform_customers(Name, filePath) :
    customer = Transform_customer_df(Name, filePath);

