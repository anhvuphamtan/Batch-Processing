from ETL_psql.Transform.Transform import Transform_df
from datetime import datetime
import pandas as pd
import os 

# ------------------------------------ Child class Sale ------------------------------------ #

class Transform_sale_df(Transform_df) : # Transform sales dataframe class
    def extra_var(self) :
        filePath = os.path.join(self.root_dir, "Products.csv");
        prod_arr = pd.read_csv(filePath);
        
        # `prod_arr` is a list derives from products df with below columns
        prod_arr = [(prod_arr['PRODUCT_ID'][i], 
                      prod_arr['SELL PRICE'][i], 
                      prod_arr['COMMISION RATE'][i]) for i in range(len(prod_arr))];
        
        # Sort `prod_arr` by the first value 'PRODUCT_ID'
        self.prod_arr = sorted(prod_arr, key = lambda x : x[0]); 
        self.n = len(self.df);
        
        # Get columns from sales df
        self.product  = list(self.df['Product ID']);
        self.quantity = list(self.df['Quantity']);


    # Binary search function which looks into sorted `prod_arr` list 
    # to find corresponding 'sale_prod_id' ('PRODUCT_ID' from sales df)
    def search_product(self, sale_prod_id) :                                    
        l = 0;
        r = len(self.prod_arr) - 1;

        while (l <= r) :
            mid = int((l + r) / 2);
            prod_id, prod_price, prod_comm_rate = self.prod_arr[mid];
            
            if (prod_id > sale_prod_id)   : r = mid - 1;
            elif (prod_id < sale_prod_id) : l = mid + 1;
            else : return prod_price, prod_comm_rate; # Return 'prod_price' and 'prod_comm_rate'
                                                      # of 'sale_prod_id'

    def transform(self) :
        # Covert to datetime format
        self.df['Date'] = [datetime.strptime(date, "%m-%d-%y").date() 
                            for date in self.df['Date']];

        # Create `revenue_arr` list which stores 'prod_price', 'prod_comm' of each 'sale_prod_id'
        revenue_arr = [self.search_product(self.product[i]) for i in range(self.n)];
       
        # Re-calculate total cost for each 'Order ID'
        self.df['Total cost'] = [val[0] * self.quantity[i] 
                                 for i, val in enumerate(revenue_arr)];
        
        # Create new column 'Profit' that calculates profit gained from sales 
        self.df['Profit'] = [(val[0] * val[1] / 100) * self.quantity[i] 
                             for i, val in enumerate(revenue_arr)];

def Transform_sales(Name, filePath) :
    sale = Transform_sale_df(Name, filePath);
