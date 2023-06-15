## 5. Implementation detail
### 5.1 Load sales data into PostgreSQL database

<img src=assets/ETL_psql.png alt="ETL psql" height="400">

<b> Airflow tasks </b>


 ```python
    # ./dags_setup.py # Airflow dags
    # -------------- Create schema task ------------- #
    Create_psql_schema = PostgresOperator(
        task_id = 'Create_psql_schema',
        postgres_conn_id = 'postgres_sale_db',
        sql = 'create_pgsql_schema.sql'
    )
    # ---------------------------------------------- #
    
    
    # ---------------- Extract task ---------------- #
    Extract_from_source = PythonOperator(
        task_id = 'Extract_from_source',
        python_callable = Extract_from_source
    )
    # ---------------------------------------------- #
    
    
    # ---------------- Transform task ---------------- #
    Transform_products = PythonOperator(
        task_id = "Transform_product_df",
        python_callable = Transform_products,
        op_kwargs = {"Name" : "products", "filePath" : "products.csv"}
    )

    .....
    
    Transform_shipments = PythonOperator(
        task_id = "Transform_shipment_df",
        python_callable = Transform_shipments,
        op_kwargs = {"Name" : "shipments", "filePath" : "shipments.csv"}
    )
    # ----------------------------------------------- #
    
    # ----------------- Load task ----------------- #
    Load_psql = PythonOperator(
        task_id = "Load_to_psql",
        python_callable = Load_schema
    )
    # -------------------------------------------- #
 ``` 

<br> <br>
<b> 1. Create_psql_schema : </b> Create PostgreSQL schema and its tables according to our data model design.
``` ./postgreSQL_setup/create_pgsql_schema.sql ```
```sql
-- Create Schema `Sale_schema` for postgreSQL database

DROP SCHEMA IF EXISTS Sale_schema CASCADE;

CREATE SCHEMA Sale_schema;

CREATE TABLE IF NOT EXISTS Sale_schema.Sales (
    Order_ID VARCHAR(255) PRIMARY KEY,
    Order_date Date,
    Product_ID BIGINT,
    Style VARCHAR(45),
    Size VARCHAR(45),
    Quantity INT,
    Payment_method VARCHAR(255),
    Total_cost DECIMAL,
    Profit DECIMAL,
    Customer_ID VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Sale_schema.Products (
    Product_ID BIGINT PRIMARY KEY,
    Product_name VARCHAR(255),
    SKU INT,
    Brand VARCHAR(255),
    Category VARCHAR(255),
    Product_size DECIMAL,
    Sell_price DECIMAL,
    Commision_rate DECIMAL,
    Commision DECIMAL
);

CREATE TABLE IF NOT EXISTS Sale_schema.Customers (
    Customer_ID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255),
    Phone VARCHAR(255),
    Age INT,
    Address VARCHAR(255),
    Postal_code INT
);

CREATE TABLE IF NOT EXISTS Sale_schema.Shipments (
    Shipment_ID VARCHAR(255) PRIMARY KEY,
    Order_ID VARCHAR(255),
    Shipping_date Date,
    Shipping_mode VARCHAR(255),
    Shipping_address VARCHAR(255),
    Shipping_status VARCHAR(255),
    Shipping_company VARCHAR(255),
    Shipping_cost DECIMAL,
    Shipping_zipcode INT
);

CREATE TABLE Sale_schema.Locations (
    Postal_code INT PRIMARY KEY,
    City VARCHAR(45),
    State VARCHAR(45),
    Country VARCHAR(45)
);

ALTER TABLE Sale_schema.Sales
ADD CONSTRAINT fk_sale_product_prodID FOREIGN KEY (Product_ID)
REFERENCES Sale_schema.Products (Product_ID)
ON DELETE CASCADE ON UPDATE CASCADE;


ALTER TABLE Sale_schema.Sales
ADD CONSTRAINT fk_sale_customer_custID FOREIGN KEY (Customer_ID)
REFERENCES Sale_schema.Customers (Customer_ID)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Shipments
ADD CONSTRAINT fk_shipment_sale_orderID FOREIGN KEY (Order_ID)
REFERENCES Sale_schema.Sales (Order_ID)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Shipments
ADD CONSTRAINT fk_shipment_location_zipcode FOREIGN KEY (Shipping_zipcode)
REFERENCES Sale_schema.Locations (Postal_code)
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE Sale_schema.Customers
ADD CONSTRAINT fk_customer_location_postcode FOREIGN KEY (Postal_code)
REFERENCES Sale_schema.Locations (Postal_code)
ON DELETE CASCADE ON UPDATE CASCADE;
```
<br> <br>
<b> 2. Extract_from_source : </b> Extract raw data from s3 bucket and store them in <i> Input_data </i> folder.
```python
# .airflow/dags/ETL_psql/Extract.py -> Extract_from_source()
def Extract_from_source() : # Extract raw data from S3 bucket
    session = boto3.Session( 
        aws_access_key_id = "AKIA33I2NGIK2R5PCCOT",
        aws_secret_access_key = "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"
    );
    
    s3 = session.client("s3");
    bucket_name = "amazon-us-sales-bucket";

    # List all objects in bucket
    response = s3.list_objects_v2(Bucket = bucket_name);

    write_dir = "/opt/airflow/Input_data";
    for obj in response['Contents'] :
        key = obj['Key'];

        write_path = os.path.join(write_dir, key);
        with open(write_path, "wb") as file :
            s3.download_fileobj(bucket_name, key, file);

```

<br> <br>
<b> 3. Perform transformation : </b> This part split into 5 small tasks, each handle the data transformation on a specific topic.
There are 6 python files : <i> Transform.py </i>, <i> Transform_***.py </i> where *** correspond to a topic ['sales', 'products', 'customers', 'shipments', 'locations'].
Each <i> Transform_***.py </i> responsible for cleaning, transforming and integrating to a corresponding OLTP table. Python class is used,
all they all inherit from the parent class in <i> Transform.py </i> :

```.airflow/dags//ETL_psql/Transform/Transform.py ```

```python
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
       
        self.clean();             # Drop duplicate values in primary key columns
        self.extra_var();         # Add additional variables if necessary
        self.transform();         # Perform transformation on data
        self.rename_col_df();     # Rename column to fit PostgreSQL table column names
        self.write_csv();         # Write to 'self.write_dir' folder

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

```
<br> <br>
<b> <i>a. Transform_locations.py </i> -> <i> Transform_locations class </i> </b>

```python
class Transform_location_df(Transform_df) : # Transform locations dataframe class
    ...
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
```


- Initially there is no csv file for locations, this class is generated from 'Customers.csv' and 'Shipments.csv' to store data
about locations by extracting customer and shipping addresses, merge them into a new dataframe. By doing this, we could reduce
dimensions in both 'customer' and 'shipment' dataframe, have better relationship as well.

<br> <br>
<b> <i>b. Transform_customers.py </i> -> <i> Transform_customers class </i> </b>

```python
class Transform_customer_df(Transform_df) : # Transfrom customers dataframe class    
    def transform(self) : 
        # Drop columns below since these information are stored in location_df
        self.df.drop(columns = ['City', 'State', 'Country'], inplace = True);


def Transform_customers(Name, filePath) :
    customer = Transform_customer_df(Name, filePath);
```
- Drop columns ['City', 'State', 'Country']

<br> <br>
<b> <i>c. Transform_shipments.py </i> -> <i> Transform_shipments class </i> </b>

```python
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
```

- Create new columns 'Shipping address' and 'Shipping zipcode' from original column 'Destination'.
- Convert column 'Shipping status' to lowercase letters 
- Drop duplicate values in column 'Order ID', since each 'Order ID' can belong exactly to one 'Shipment ID' only.
- Drop columns 'Destination'

<br> <br>
<b> <i>d. Transform_products.py </i> -> <i> Transform_products class </i> </b>

```python
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
```

- Fill null value in column "BRAND" with "unknown".
- Create new column 'COMMISION' which is the profit gained by selling the product.

<br> <br>
<b> <i>e. Transform_sales.py </i> -> <i> Transform_sales class </i> </b>

```python
class Transform_sale_df(Transform_df) : # Transform sales dataframe class
    ...
    self.prod_arr -> array of "PRODUCT ID" from products_df
    
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
```

- Covert to datetime format for column 'Date'
- Re-calculate column 'Total cost' which is the total cost of each 'Order ID' (there is quite a lot of inconsistent between 
table 'Products' and 'Sales') by using binary search function ```search_product(self, sale_prod_id) ``` :

    - Store column 'PRODUCT ID' from dataframe 'Products' in a list and sort it
    - For each 'Product ID' in an 'Order ID' in 'Sales' dataframe, perform binary search to retrieve 'Product sell price' & 'Product commision rate'
- The value 'Product commision rate' is also used to create new column 'Total Profit'.

<br> <br>
<b> <i> f. Rename_col_df.py </i> : This file renames columns in all dataframes to fit PostgreSQL schema.
 
<br> <br>
<b> 4. Load_to_psql : </b> Load all transformed data into PostgreSQL database.

```.airflow/dags/ETL_psql/Load/Load_psql.py```

```python
def Load_table(table_name, df, cur) : # Load dataframe 'df' with name 'table_name' to postgreSQL db
    # Convert to correct data type for each column
    df = df.convert_dtypes();
    records = df.to_records(index = False);
    # string of column names of df
    column_names = ', '.join(df.columns);

    # The number of values to be inserted
    s_list = ', '.join(['%s'] * len(df.columns));

    query = f"""
        INSERT INTO {table_name} ({column_names}) VALUES ({s_list});
    """

    # Execute query
    cur.executemany(query, records);

    print(f"Successfully insert data to table {table_name}");        

        
def Load_schema() :
    # parameters for connecting to postgreSQL database
    connect_params = {
        "host": "postgres",
        "port": 5432,
        "database": "airflow",
        "user": "airflow",
        "password": "airflow"
    };
    
    conn = psycopg2.connect(**connect_params); # Connect
    conn.autocommit = True; # All changes will take place
    cur = conn.cursor(); 
    
    
    table_order = ['locations', 'customers', 'products', 'sales', 'shipments'];
    # Reorder tables as `table_order` format due to foreign key constraint apply to several tables
    root_dir = "/opt/airflow/Transformed_data";
    for table in table_order : 
        filePath = os.path.join(root_dir, table + ".csv");
        # Check if the dataframe has been transformed and stored in root_dir yet
        while (os.path.isfile(filePath) != True) : time.sleep(3);
        
        # Extract and load to psql
        df = pd.read_csv(filePath);
        Load_table(f"Sale_schema.{table}", df, cur);     
    
    # Close connection to postgreSQL
    cur.close();    
    conn.close();
```

Since there is foreign key constraints, the table must be loaded in order ['locations'->'customers'->'products'->'sales'->'shipments'].
Psycopg2 library is used to establish connection to PostgreSQL database "airflow", each table is loaded using function ```Load_table(table_name, df, cur)```
that loads dynamically any dataframe 'df' to table 'table_name'.
 
<br> <br>
### 5.2 Load data from PostgreSQL to Amazon Redshift :
<img src=assets/ETL_redshift.png alt="ETL redshift" height="400">

<b> Airflow tasks </b>
  
```python
  
    ETL_s3 = PythonOperator(
        task_id = "ETL_s3",
        python_callable = ETL_s3
    )

    Create_redshift_schema = PythonOperator(
        task_id = "Create_redshift_schema",
        python_callable = Create_redshift_schema,
        op_kwargs = {"root_dir" : "/opt/airflow/redshift_setup"}  
    )

    Load_s3_redshift = PythonOperator(
        task_id = "Load_s3_redshift",
        python_callable = Load_s3_to_redshift
    )
```

<br> <br>
<b> 1. ETL_psql_s3 : </b> Extract data from PostgreSQL database, perform transformation, and load to S3 bucket

<b> <i> Extract data from PostgreSQL database </i> </b>
  
```python
# ----------------------------------- Extract ----------------------------------- 

def Extract_from_postgreSQL(df_dict) : # Extract data from postgreSQL database
    conn = Setup_psql_connection();
    cur = conn.cursor();

    table_list = ['products', 'sales', 'customers', 'shipments', 'locations'];
    
    # Query each table in schema and load into df_dict[table]
    for table in table_list :
        # column_query retrieve the columns of table
        column_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'sale_schema'
            AND table_name = '{table}';
        """

        cur.execute(column_query);
        
        column_list = list(cur.fetchall());
        # Create a column_list that stores the columns from table 
        column_list = [column[0] for column in column_list];
        
        # Query data
        data_query = f"""
            SELECT * FROM sale_schema.{table};
        """

        cur.execute(data_query);
        df_dict[table] = pd.DataFrame(columns = column_list, data = cur.fetchall());

    cur.close();
    conn.close();
```

<b> <i> Transformation step is done to fit star schema design in warehouse. </i> </b>

```python
# ----------------------------------- Transform -----------------------------------

def Generate_date() : # Generate all days from 2021-01-01 to 2022-12-31
    start_date = datetime(2021, 1, 1)  
    end_date = datetime(2022, 12, 31)  

    time_arr = [start_date + timedelta(days = i) for i in range((end_date - start_date).days + 1)];
    time_dict = {};
    time_dict["full_date"] = [];
    time_dict["day"] = [];
    time_dict["month"] = [];
    time_dict["year"] = [];
    
    # Generate a dictionary time_dict with 4 attributes : ['full_date', 'day', 'month', 'year']
    for date in time_arr :
        time_dict['full_date'].append(date.date());
        time_dict['day'].append(date.day);
        time_dict['month'].append(date.month);
        time_dict['year'].append(date.year); 

    return time_dict;

def Joining_df(df_1, df_2, left, right) : # Joining df_1 and df_2 
    return df_1.merge(df_2, left_on = left, right_on = right, how = 'inner');
    
def Drop_col_df(df, column_list) : # Drop 'column_list' from df
    df.drop(columns = column_list, inplace = True);

def Transform(df_dict) : # Transform df to fit star schema model of redshift
    df_dict['time'] = pd.DataFrame(Generate_date());
    
    # Joining sale_df and shipment_df based on 'order_id'
    df_to_join = df_dict['shipments'][['order_id', 'shipment_id', 'shipping_cost', 'shipping_zipcode']];
    df_dict['sales'] = Joining_df(df_dict['sales'], df_to_join, 'order_id', 'order_id');
    
    # Joining location_df and shipment_df based on 'postal_code' 
    df_to_join = df_dict['shipments'][['shipping_zipcode', 'shipping_address']];
    df_dict['locations'] = Joining_df(df_dict['locations'], df_to_join, 'postal_code', 'shipping_zipcode');
    
    # Drop uncessary columns
    Drop_col_df(df_dict['customers'], ['address', 'postal_code']);
    Drop_col_df(df_dict['products'], ['sell_price', 'commision_rate', 'commision']);
    Drop_col_df(df_dict['shipments'], ['shipping_date', 'shipping_address', 'order_id', 
                                       'shipping_zipcode', 'shipping_cost']);
    Drop_col_df(df_dict['locations'], ['shipping_zipcode']);

    # Rename column
    df_dict['sales'].rename(columns = {"order_id" : "sale_id", "total_cost" : "revenue"}, inplace = True);
    
    # Re-order columns to fit redshift table
    df_dict['sales'] = df_dict['sales'][['sale_id', 'revenue', 'profit', 'quantity', 'shipping_cost',
                                'product_id', 'customer_id', 'shipping_zipcode', 'order_date', 'shipment_id']];
    
```

<b> <i> Load data to S3 bucket </i> </b>
```python
# ----------------------------------- Load -----------------------------------

def Load_df_S3(s3, bucket_name, df, key) : # Load df to s3 bucket
    csv_buffer = BytesIO();
    df.to_csv(csv_buffer, index = False);
    csv_buffer.seek(0);

    s3.upload_fileobj(csv_buffer, bucket_name, key + ".csv");

def Load_S3(df_dict) : # Load all df to s3 bucket
    # Create session to connect to s3 bucket
    session = boto3.Session( 
        aws_access_key_id = "AKIA33I2NGIK2R5PCCOT",
        aws_secret_access_key = "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"
    );

    s3 = session.client("s3");
    bucket_name = "vupham-sale-bucket";

    # Delete all objects in s3 bucket before loading to redshift
    try : 
        response = s3.list_objects_v2(Bucket = bucket_name);
        for obj in response['Contents'] :
            key = obj['Key'];
            s3.delete_object(Bucket = bucket_name, Key = key);
    except :
        pass

    #Loading all df to s3 bucket
    for table, df in df_dict.items() : 
        print(f"Loading {table} to s3");
        Load_df_S3(s3, bucket_name, df, table);
        print("Load successfully \n");

# ----------------------------------- ETL -----------------------------------
def ETL_s3() :
    pd.set_option('display.max_columns', None)
    df_dict = {};
    Extract_from_postgreSQL(df_dict);
    Transform(df_dict);
    Load_S3(df_dict); 
```

<br> <br>
<b> 2. Create_redshift_schema : </b> Create redshift schema

```./redshift_setup/create_redshift_schema.sql```
 
 ```sql
 DROP SCHEMA IF EXISTS warehouse_sales CASCADE;

CREATE SCHEMA warehouse_sales;

CREATE TABLE warehouse_sales.Sales (
    Sale_ID VARCHAR(255) PRIMARY KEY,
    Revenue DECIMAL(10, 3),
    Profit DECIMAL(10, 3),
    Quantity INT,
    Shipping_cost DECIMAL(10, 3),

    Product_ID BIGINT,
    Customer_ID VARCHAR(255),
    Shipping_zipcode INT,
    Order_date DATE,
    Shipment_ID VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Products (
    Product_ID BIGINT PRIMARY KEY,
    Product_name VARCHAR(255),
    SKU INT,
    Brand VARCHAR(255),
    Category VARCHAR(255),
    Product_size DECIMAL
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Shipments (
    Shipment_ID VARCHAR(255) PRIMARY KEY,
    Shipping_mode VARCHAR(255),
    Shipping_status VARCHAR(255),
    Shipping_company VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Customers (
    Customer_ID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255),
    Phone VARCHAR(255),
    Age INT
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Locations (
    Shipping_zipcode INT PRIMARY KEY,
    City VARCHAR(45),
    State VARCHAR(45),
    Country VARCHAR(45),
    Shipping_address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_sales.Time (
    Full_date Date PRIMARY KEY,
    Day INT,
    Month INT,
    Year INT
);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_product_prodID FOREIGN KEY (Product_ID)
REFERENCES warehouse_sales.Products (Product_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_custID FOREIGN KEY (Customer_ID)
REFERENCES warehouse_sales.Customers (Customer_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_shipmentID FOREIGN KEY (Shipment_ID)
REFERENCES warehouse_sales.Shipments (Shipment_ID);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_zipcode FOREIGN KEY (Shipping_zipcode)
REFERENCES warehouse_sales.Locations (Shipping_zipcode);

ALTER TABLE warehouse_sales.Sales 
ADD CONSTRAINT fk_sale_customer_timeID FOREIGN KEY (Order_date)
REFERENCES warehouse_sales.Time (Full_date);
 ```

```./airflow/dags/ETL_redshift/Load_s3_to_redshift.py -> Establish_redshift_connection() & Create_redshift_schema() ```

```python
import redshift_connector
import os 

def Establish_redshift_connection() : 
    conn = redshift_connector.connect( 
        iam = True,
        database = 'redshift_main_db',
        db_user = 'vupham',
        password = 'vudet11Q',
        cluster_identifier = 'vupham-redshift-cluster',
        access_key_id = 'AKIA33I2NGIK5DXXZEOD',
        secret_access_key = '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',
        region = 'ap-southeast-1'
    );

    conn.autocommit = True;
    return conn;


def Create_redshift_schema(root_dir) : # Create redshift schema
    conn = Establish_redshift_connection();
    cur = conn.cursor();

    path = os.path.join(root_dir, "create_redshift_schema.sql");
    with open(path, 'r') as file :
        redshift_sql = file.read();

    redshift_sql = redshift_sql.split(";");
    redshift_sql = [statement + ";" for statement in redshift_sql];

    for idx, statement in enumerate(redshift_sql) :
        if (statement == ";") : continue;
        cur.execute(statement);

    print("Create redshift schema successfully");
    cur.close();
    conn.close();
```
  
Establish redshift connection using <b> redshift_connector </b> library, redshift schema will be created using redshift_connector

<br> <br>
<b> 3. Load_s3_redshift : </b> Load data from S3 bucket to Redshift

```./airflow/dags/ETL_redshift/Load_s3_to_redshift.py -> Load_s3_to_redshift() ```

```python
def Load_s3_to_redshift() : # Load data from s3 to redshift
    conn = Establish_redshift_connection();
    cur = conn.cursor();
    table_list = ['customers', 'products', 'locations', 'time', 'shipments', 'sales'];
    bucket_name = "vupham-sale-bucket";
    schema = "warehouse_sales";

    for table in table_list :      
        query = f"""
            COPY {schema}.{table}
            FROM 's3://{bucket_name}/{table}.csv'
            IAM_ROLE 'arn:aws:iam::814488564245:role/redshift_role'
            FORMAT AS CSV
            IGNOREHEADER 1
            FILLRECORD;
        """

        cur.execute(query);


    cur.close();
    conn.close();
```

Load each table from S3 bucket to redshift using COPY command.
  
