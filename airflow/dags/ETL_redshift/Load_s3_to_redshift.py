import redshift_connector
import os 

def Establish_redshift_connection() : 
    conn = redshift_connector.connect( 
        iam = True,
        database = 'redshift_main_db',
        db_user = 'vupham',
        password = 'vudet11Q',
        cluster_identifier = 'vupham-redshift-cluster',
        access_key_id = '********',  # You can use root access key or create redshift user to access to your redshift cluster
        secret_access_key = '********',
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
