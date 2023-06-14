import boto3 
import os

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