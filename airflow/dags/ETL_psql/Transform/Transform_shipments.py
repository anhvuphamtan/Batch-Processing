from ETL_psql.Transform.Transform import Transform_df

# ---------------------------------- Child class shipment ---------------------------------- #

class Transform_shipment_df(Transform_df) : # Transform shipments dataframe class 
    def transform(self) :
        # Create 2 new columns 'Shipping address' and 'Shipping zipcode'
        self.df['Shipping address'] = [address.split(",")[0] for address in self.df['Destination']];
        self.df['Shipping zipcode'] = [address.split(",")[4] for address in self.df['Destination']];
        
        # Convert 'Shipping status' to lower case letters
        self.df['Shipping status'] = self.df['Shipping status'].str.lower();

        # Though 'Order ID' is not the primary key column, each 'Order ID' can belongs to one 'Shipment ID only'
        self.df.drop_duplicates(subset = ['Order ID'], inplace = True);
        
        # Drop column 'Destination'
        self.df.drop(columns = ['Destination'], inplace = True);

def Transform_shipments(Name, filePath) :
    shipment = Transform_shipment_df(Name, filePath);
