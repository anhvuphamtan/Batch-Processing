# AWS credentials variables
aws_access_key = "AKIA33I2NGIK7IS6LPN3"
aws_secret_key = "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"
aws_region = "ap-southeast-1"

# Network variables
server_vpc_cidr = "10.10.0.0/16"
server_subnet_redshift_1 = "10.10.0.0/24"
server_subnet_redshift_2 = "10.10.1.0/24"

availability_zone = ["ap-southeast-1a", "ap-southeast-1b", "ap-southeast-1c"]


# Redshift cluster variables
redshift_cluster_identifier  = "vupham-redshift-cluster"
redshift_database_name       = "redshift_main_db"
redshift_master_username     = "vupham"
redshift_master_password     = "vudet11Q"
redshift_node_type           = "dc2.large"
redshift_cluster_type        = "multi-node"
redshift_number_of_nodes     = 2
