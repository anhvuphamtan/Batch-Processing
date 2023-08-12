# Batch Processing : ETL pipeline, data modelling and warehousing of Sales data

## Table of Contents
1. [Introduction](#1-introduction)
   - [Technologies used](#technologies-used)
3. [Implementation Overview](#2-implementation-overview)
4. [Design](#3-design)
5. [Project structure](#4-project-structure)
6. [Settings](#5-settings)
   - [Prerequisites](#prerequisites)
   - [AWS Infrastructure](#aws-infrastructure)
   - [Docker](#docker)
   - [Running](#running)
7. [Implementation](#6-implementation)
   - [Load Sales Data into PostgreSQL Database](#61-load-sales-data-into-postgresql-database)
   - [Load Data from PostgreSQL to Amazon Redshift](#62-load-data-from-postgresql-to-amazon-redshift)
8. [Visualize Result](#7-visualize-result)


## 1. Introduction 
Data is collected from an e-commerce company about their sales in 2022, the company's analytic teams is interested in understanding their business situation in the last year. We will build ETL pipelines which will transform raw data into actionable insights, store them in OLTP database (PostgreSQL) and OLAP database (Amazon Redshift) for enhanced data analytics capabilities.

Data include 4 csv files : <b> <i> Sales, Products, Shipments, Customers. </i> </b>

### Technologies used
- Python
- PostgreSQL
- Airflow
- Terraform (Infrastructure provisioning tool)
- AWS services : S3, Redshift (data warehouse)
- Docker

## 2. Implementation overview 
Design data models for OLTP database (PostgreSQL) and data warehouse (Amazon Redshift). Build an ETL pipeline to transform raw data into actionable insights in PostgreSQL, also store them in S3 for staging. Then implement another ETL pipeline which process data from S3 and load them to Amazon Redshift for enhanced data analytics . Using Airflow to orchestrate pipeline workflow, Terraform for setting up AWS Redshift cluster, and Docker to containerize the project - allow for fast build, test, and deploy project.

<img src = assets/Airflow%20conceptual%20view.png alt = "Airflow conceptual view">

## 3. Design 
<div style="display: flex; flex-direction: column;">
  <img src=assets/Data%20model.png alt="Data model" width="600" height="500">
  <p style="text-align: center;"> <b> <i> Data model for Postgres </i> </b> </p>
</div>

<br> <br>

<div style="display: flex; flex-direction: column;">
  <img src=assets/Star%20schema.png alt="Star schema" width="600" height="500">
  <p style="text-align: center;"> <b> <i> Data model (star schema) for Redshift </i> </b> </p>
</div>

<br> <br>

<div style="display: flex; flex-direction: column;">
  <img src=assets/Airflow_workflow.png alt="Star schema" width="900" height="500">
  <p style="text-align: center;"> <b> <i> Airflow workflow </i> </b> </p>
</div>


## 4. Project Structure

```bash

Batch-Processing/
  ├── airflow/
  │   ├── dags/
  │   │   ├── dags_setup.py
  │   │   ├── ETL_psql
  │   │   │   ├── Extract
  │   │   │   │   └── Extract.py
  │   │   │   ├── Load/
  │   │   │   │   └── Load_psql.py
  │   │   │   └── Transform
  │   │   │       ├── Rename_col_df.py
  │   │   │       ├── Transform.py
  │   │   │       ├── Transform_customers.py
  │   │   │       ├── Transform_locations.py
  │   │   │       ├── Transform_products.py
  │   │   │       ├── Transform_shipments.py
  │   │   │       └── Transfrom_sales.py
  │   │   └── ETL_redshift
  │   │       ├── ETL_psql_s3.py
  │   │       └── Load_s3_to_redshift.py
  │   └── logs
  ├── postgreSQL_setup
  │   └── create_pgsql_schema.sql
  ├── redshift_setup
  │   └── create_redshift_schema.sql
  ├── docker
  │   ├── Dockerfile
  │   └── requirements.txt
  ├── docker-compose.yaml
  ├── Implementation detail.md
  ├── assets
  │   └── Many images.png
  ├── Input_data
  ├── Transformed_data
  ├── Makefile
  ├── terraform
  │   ├── main.tf
  │   ├── terraform.tfvars
  │   └── variables.tf
  └── readme.md
```
<br>

## 5. Settings

### Prerequisites
- AWS account 
- Terraform
- Docker 

### AWS Infrastructure 

<img src="/assets/Redshift%20diagram.png" alt="Redshift diagram" height="500">

- Two <b> dc2.large </b> type nodes for Redshift cluster
- Redshift cluster type : multi-node
- Redshift cluster is put inside a VPC <i> (10.10.0.0/16) </i>, redshift subnet group consists of 2 subnets <i> "Subnet for redshift az 1"(10.10.0.0/24) </i> and <i> "Subnet for redshift az 2" (10.10.1.0/24) </i>, each subnet is put in an Availability zone.

- These two subnets associate with a public route table (outbound traffic to the public internet through the Internet Gateway).
 
- Redshift security group allows all inbound traffic from port 5439. 

- Finally, IAM role is created for Redshift with full S3 Access. 

- Create redshift cluster.

```python
resource "aws_redshift_cluster" "sale_redshift_cluster" {
    cluster_identifier  = var.redshift_cluster_identifier
    database_name       = var.redshift_database_name
    master_username     = var.redshift_master_username
    master_password     = var.redshift_master_password
    node_type           = var.redshift_node_type
    cluster_type        = var.redshift_cluster_type
    number_of_nodes     = var.redshift_number_of_nodes

    iam_roles = [aws_iam_role.redshift_iam_role.arn]  

    cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.id
    skip_final_snapshot = true

    tags = {
        Name = "vupham_redshift_cluster"
    }
}
```

### Docker 
```Python
# ./docker/Dockerfile
FROM apache/airflow:2.5.1
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt 

```

[Dockerfile](/docker/Dockerfile) build a custom images with <i> apache-airflow:2.5.1 and libraries in 'requirements.txt' </i>

```python
# ./docker/requirements.txt
redshift_connector
pandas
apache-airflow-providers-amazon==8.1.0
apache-airflow-providers-postgres==5.4.0
boto3==1.26.148
psycopg2-binary==2.9.6

```
[docker-compose.yaml](/docker-compose.yaml) will build containers to run our application.

### Running 

Please refer to Makefile for more details
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

# Notes : Please configure your own AWS access & secret keys in terraform.tfvars and /airflow/dags/ETL_redshift/Load_s3_to_redshift.py in order to create redshift cluster and connect to it
make infra-up # Build cloud infrastructure
```

## 6. Implementation

### Refer to [Implementation detail.md](/Implementation%20detail.md) for more details on implementation

### 6.1 Load sales data into PostgreSQL database

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

<b> 2. Extract_from_source : </b> Extract raw data from s3 bucket and store them in <i> Input_data </i> folder.

<b> 3. Perform transformation : </b> This part split into 5 small tasks, each handle the data transformation on a specific topic.
There are 6 python files : <i> Transform.py </i>, <i> Transform_<b>name</b>.py </i> where <b> <i> name </i> </b> correspond to a topic <b> <i> ['sales', 'products', 'customers', 'shipments', 'locations']. </i> </b>
Each <i> Transform_<b>name</b>.py </i> responsible for cleaning, transforming and integrating to a corresponding OLTP table. Python class is used,
all they all inherit from the parent class in <i> Transform.py </i> :

<b> 4. Load_to_psql : </b> Load all transformed data into PostgreSQL database.

<br>

### 6.2 Load data from PostgreSQL to Amazon Redshift
<img src=assets/ETL_redshift.png alt="ETL redshift" height="400">

<b> Airflow tasks </b>
  
```python
# ./dags_setup.py # Airflow dags
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
<br>
<b> 1. ETL_s3 : </b> Extract data from PostgreSQL database, perform transformation, and load to S3 bucket 

<b> 2. Create_redshift_schema : </b> Create redshift schema

<b> 3. Load_s3_redshift : </b> Load data from S3 bucket to Redshift
  
<br> 

## 7. Visualize result

Connect redshift to metabase and visualize results

<div style="display: flex; flex-direction: column;">
  <img src=assets/metabase.png alt="connect_metabase" height="500">
  <p style="text-align: center;"> <b> <i> Connect to metabase </i> </b> </p>
</div>

### Results

<div style="display: flex; flex-direction: column;">
  <img src=assets/Revenue%20by%20month.png alt="Revenue by month" height="500">
  <p style="text-align: center;"> <b> <i> Revenue by month in 2022 </i> </b> </p>
</div>

<br> <br>
  
<div style="display: flex; flex-direction: column;">
  <img src=assets/Brand%20popularity.png alt="Brand popularity.png" height="500">
  <p style="text-align: center;"> <b> <i> Brand popularity </i> </b> </p>
</div>

<br> <br>
  
<div style="display: flex; flex-direction: column;">
  <img src=assets/Profit%20by%20state.png alt="Profit by state.png" height="500">
  <p style="text-align: center;"> <b> <i> Profit by state </i> </b> </p>
</div>

<br> <br>

<div style="display: flex; flex-direction: column;">
  <img src=assets/Shipping%20orders%20by%20company.png alt="Shipping orders by company" height="500">
  <p style="text-align: center;"> <b> <i> Shipping orders by company </i> </b> </p>
</div>
  

