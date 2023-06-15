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
 
<b> 1. Create_psql_schema : </b> Create PostgreSQL schema and its tables according to our data model design.
``` ./postgreSQL_setup/create_pgsql_schema.sql ```

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
<br>
<b> <i> Transform_locations.py </i> -> <i> Transform_locations class </i> </b>

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

<b> <i> Transform_customers.py </i> -> <i> Transform_customers class </i> </b>

```python
class Transform_customer_df(Transform_df) : # Transfrom customers dataframe class    
    def transform(self) : 
        # Drop columns below since these information are stored in location_df
        self.df.drop(columns = ['City', 'State', 'Country'], inplace = True);


def Transform_customers(Name, filePath) :
    customer = Transform_customer_df(Name, filePath);
```
- Drop columns ['City', 'State', 'Country']

<b> <i> Transform_shipments.py </i> -> <i> Transform_shipments class </i> </b>

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

<b> <i> Transform_products.py </i> -> <i> Transform_products class </i> </b>

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

<b> <i> Transform_sales.py </i> -> <i> Transform_sales class </i> </b>

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

<b> <i> Rename_col_df.py </i> : This file renames columns in all dataframes to fit PostgreSQL schema.
 
<br>
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
  
<b> 1. ETL_psql_s3 : </b> Extract data from PostgreSQL database, perform transformation, and load to S3 bucket
  
```ETL_s3()``` contains three functions ```Extract_from_postgreSQL(df_dict)```, ```Transform(df_dict)```, ```Load_s3(df_dict)``` ('df_dict' is a dictionary of dataframes).

```Extract_from_postgreSQL(df_dict)``` extract data from PostgreSQL database, each table is loaded as dataframe into 'df_dict'.
  
```Transform(df_dict)``` perform transformation to fit data into star schema desgin in data warehouse, include joining dataframes, dropping unecessary columns and re-order columns.

```Load_s3(df_dict)``` Load all dataframes to S3 bucket.
  
<b> 2. Create_redshift_schema : </b> Create redshift schema

```./redshift_setup/create_redshift_schema.sql```

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
  
