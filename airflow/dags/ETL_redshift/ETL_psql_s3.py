from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd 
import subprocess
import psycopg2 
import boto3 

def Setup_psql_connection() : # Setup postgreSQL connection
    # Parameters for establishing connection to postgreSQL db 'sale_db'
    connect_params = {
        "host": "postgres",
        "port": 5432,
        "database": "airflow",
        "user": "airflow",
        "password": "airflow"
    };

    # Connect
    conn = psycopg2.connect(**connect_params);
    conn.autocommit = True;
    return conn;

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