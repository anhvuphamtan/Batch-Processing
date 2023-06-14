from ETL_psql.Transform.Transform import Transform_df

# ---------------------------------- Child class Product ---------------------------------- #

class Transform_product_df(Transform_df) : # Transform products dataframe class
    def extra_var(self): 
        # Get columns from products df
        self.sell_price = self.df['SELL PRICE'];        
        self.comm_rate  = self.df['COMMISION RATE'];
    
    def transform(self) :
        # Replace nan value with "Unknown"
        self.df['BRAND'].fillna("Unknown", inplace = True); 
        
        # PRODUCT_SIZE column is an unecessary column
        self.df['PRODUCT_SIZE'] = [0 for i in range(len(self.df))];
        
        # Get comission for each product
        self.df['COMMISION'] = self.sell_price * self.comm_rate / 100;
        
def Transform_products(Name, filePath) :
    product = Transform_product_df(Name, filePath);
