import pandas as pd
import psycopg2 
import time
import os 

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