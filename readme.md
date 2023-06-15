# Batch Processing : ETL pipeline, data modelling and warehousing using Airflow, Redshift

## 1. Introduction 
Data is collected from an e-commerce company about their sales in the US in 2022, the company's analytic teams is interested in understanding the business situation in the last year.

Data include 4 csv files : Sales, Products, Shipments, Customers.

## 2. Implementation overview 
Design Data Model for Postgres and build ETL pipeline using python; subsequently design a Star Schema for data warehousing (Redshift),
build a second ETL pipeline to handle this from Postgres to Redshift. Using Airflow to orchestrate pipeline workflow, Terraform for setting up AWS Redshift cluster, and
Docker for containerizing the project - allow for fast build, test, and deploy project.

<img src = assets/Airflow%20conceptual%20view.png alt = "Airflow conceptual view">

## 3. Design 
<div style="display: flex; flex-direction: column;">
  <img src=assets/Data%20model.png alt="Data model" width="600" height="500">
  <p style="text-align: center;"> Data model for Postgres </p>
</div>

<br> <br>

<div style="display: flex; flex-direction: column;">
  <img src=assets/Star%20schema.png alt="Star schema" width="600" height="500">
  <p style="text-align: center;">Data model (star schema) for Redshift </p>
</div>

<br> <br>

<div style="display: flex; flex-direction: column;">
  <img src=assets/Airflow_workflow.png alt="Star schema" width="900" height="500">
  <p style="text-align: center;">Airflow workflow </p>
</div>


## 4. Running 

### Prerequisites
- Docker
- Terraform 
- AWS account (if you wish to build your own cloud infrastructure, please go to terraform/* and modify)

Refer to Makefile for more details
```
# Clone and cd into the project directory
git clone https://github.com/anhvuphamtan/Batch-Processing.git
cd Batch-Processing

# Start docker containers on your local computer
make up

# Add airflow connections : postgres-redshift-aws connections
make connections

# Set up cloud infrastructure
make infra-init # Only need in the first run 

make infra-up # Build cloud infrastructure
```

## 5. Implementation detail

### Refer to [Implementation detail.md](/Implementation%20detail.md) for more details on implementation

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
<br> <br>
<b> 2. Extract_from_source : </b> Extract raw data from s3 bucket and store them in <i> Input_data </i> folder.
```./airflow/dags/ETL_psql/Extract.py -> Extract_from_source()```
<br> <br>
<b> 3. Perform transformation : </b> This part split into 5 small tasks, each handle the data transformation on a specific topic.
There are 6 python files : <i> Transform.py </i>, <i> Transform_***.py </i> where *** correspond to a topic ['sales', 'products', 'customers', 'shipments', 'locations'].
Each <i> Transform_***.py </i> responsible for cleaning, transforming and integrating to a corresponding OLTP table. Python class is used,
all they all inherit from the parent class in <i> Transform.py </i> :
<br>

```python
# ./airflow/dags/ETL_psql/Transform/Transform.py 
# ---------------------------------- Parent class ---------------------------------- #

class Transform_df : # Parent class for transformation of dataframe
    def __init__(self, Name, filePath = "") :
        ... # Init variables
       
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
        ... # Write to 'Transformed_data' folder

```
<br>
<b> <i> a. Transform_locations.py </i> -> <i> Transform_locations class </i> </b>

- Initially there is no csv file for locations, this class is generated from 'Customers.csv' and 'Shipments.csv' to store data
about locations by extracting customer and shipping addresses, merge them into a new dataframe. By doing this, we could reduce
dimensions in both 'customer' and 'shipment' dataframe, have better relationship as well.

<b> <i> b. Transform_customers.py </i> -> <i> Transform_customers class </i> </b>

- Drop columns ['City', 'State', 'Country']

<b> <i> c. Transform_shipments.py </i> -> <i> Transform_shipments class </i> </b>

- Create new columns 'Shipping address' and 'Shipping zipcode' from original column 'Destination'.
- Convert column 'Shipping status' to lowercase letters 
- Drop duplicate values in column 'Order ID', since each 'Order ID' can belong exactly to one 'Shipment ID' only.
- Drop columns 'Destination'

<b> <i> d. Transform_products.py </i> -> <i> Transform_products class </i> </b>

- Fill null value in column "BRAND" with "unknown".
- Create new column 'COMMISION' which is the profit gained by selling the product.

<b> <i> e. Transform_sales.py </i> -> <i> Transform_sales class </i> </b>

- Covert to datetime format for column 'Date'
- Re-calculate column 'Total cost' which is the total cost of each 'Order ID' (there is quite a lot of inconsistent between 
table 'Products' and 'Sales') by using binary search function ```search_product(self, sale_prod_id) ``` :

    - Store column 'PRODUCT ID' from dataframe 'Products' in a list and sort it
    - For each 'Product ID' in an 'Order ID' in 'Sales' dataframe, perform binary search to retrieve 'Product sell price' & 'Product commision rate'
- The value 'Product commision rate' is also used to create new column 'Total Profit'.

<b> <i> f. Rename_col_df.py </i> : This file renames columns in all dataframes to fit PostgreSQL schema.
 
<br> <br>
<b> 4. Load_to_psql : </b> Load all transformed data into PostgreSQL database.
<br> <br>
 ```python
# ./airflow/dags/ETL_psql/Load/Load_psql.py
def Load_schema() :
    ...
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
    
    ...
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
  
```./airflow/dags/ETL_redshift/ETL_psql_s3.py -> ETL_s3() ```
  
``ETL_s3()``` contains these below function
 
```Extract_from_postgreSQL(df_dict)``` extract data from PostgreSQL database, each table is loaded as dataframe into 'df_dict'.
  
```Transform(df_dict)``` perform transformation to fit data into star schema desgin in data warehouse, include joining dataframes, dropping unecessary columns and re-order columns.

```Load_s3(df_dict)``` Load all dataframes to S3 bucket.
  
 
<br> <br>
<b> 2. Create_redshift_schema : </b> Create redshift schema
```./airflow/dags/redshift_setup/create_redshift_schema.sql```
```./ETL_redshift/Load_s3_to_redshift.py -> Establish_redshift_connection & Create_redshift_schema() ```
  
Establish redshift connection using <b> redshift_connector </b> library, redshift schema will be created using redshift_connector
<br> <br>
<b> 3. Load_s3_redshift : </b> Load data from S3 bucket to Redshift
```./ETL_redshift/Load_s3_to_redshift.py -> Load_s3_to_redshift() ```

Load each table from S3 bucket to redshift using COPY command.
  
<br> <br>
# 6. Visualize result :

Connect redshift to metabase and visualize result


<div style="display: flex; flex-direction: column;">
  <img src=assets/Revenue%20by%20month.png alt="Revenue by month" height="500">
  <p style="text-align: center;">Revenue by month in 2022 </p>
</div>
  
<div style="display: flex; flex-direction: column;">
  <img src=assets/Brand%20popularity.png alt="Brand popularity.png" height="500">
  <p style="text-align: center;">Brand popularity </p>
</div>
  
<div style="display: flex; flex-direction: column;">
  <img src=assets/Profit%20by%20state.png alt="Profit by state.png" height="500">
  <p style="text-align: center;">Profit by state </p>
</div>
  
<div style="display: flex; flex-direction: column;">
  <img src=assets/Shipping%20orders%20by%20company.png alt="Shipping orders by company" height="500">
  <p style="text-align: center;">Shipping orders by company </p>
</div>
