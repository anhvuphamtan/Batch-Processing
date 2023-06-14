from ETL_psql.Transform.Transform import Transform_df
import pandas as pd 
import os 
# ---------------------------------- Child class Location ---------------------------------- #

class Transform_location_df(Transform_df) : # Transform locations dataframe class
    def extra_var(self):
        filePath = os.path.join(self.root_dir, "Customers.csv");
        customer_df = pd.read_csv(filePath);
        
        filePath = os.path.join(self.root_dir, "Shipments.csv");
        shipment_df = pd.read_csv(filePath);
        
        # Get necessary columns from customer_df and shipment_df
        self.customer_location    = customer_df[['Postal code', 'City', 'State', 'Country']];
        self.shipment_destination = shipment_df['Destination'];

    def transform(self) :
        # self.shipment_destination contains address of shipping location in the following format :
        # Building number - City - State - Country - Postal code
        # Split them into separate information
        addr_arr = [address.split(",") for address in self.shipment_destination];
        
        # Create `location_dict` that stores separate information
        location_dict = {};
        location_dict['Postal code'] = [int(address[4]) for address in addr_arr];
        location_dict['City']        = [address[1] for address in addr_arr];
        location_dict['State']       = [address[2] for address in addr_arr];
        location_dict['Country']     = [address[3] for address in addr_arr];
        
        # concat customer_location and location_dict
        self.df = pd.concat([self.customer_location, pd.DataFrame(location_dict)]);

        # Postal code will be the primary key column -> drop duplicate values
        self.df.drop_duplicates(subset = ['Postal code'], keep = 'first', inplace = True);

def Transform_locations(Name, filePath) :
    location = Transform_location_df(Name, filePath);
