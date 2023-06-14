#AWS credentials

variable "aws_access_key" {
    type        = string
    description = "AWS access key"
}

variable "aws_secret_key" {
    type        = string
    description = "AWS secret key"
}

variable "aws_region" {
    type        = string
    description = "AWS region"
}

# Network variables

variable "server_vpc_cidr" {
    type        = string
    description = "VPC for the main server"
}

variable "server_subnet_redshift_1" {
    type        = string
    description = "IPV4 redshift subnet 1"
}

variable "server_subnet_redshift_2" {
    type        = string
    description = "IPV4 redshift subnet 2"
}

variable "availability_zone" {
    type        = list 
    description = "List of availability zones"
}


# Redshift cluster variables

variable "redshift_cluster_identifier" {
    type        = string
    description = "Redshift cluster identifier"
}

variable "redshift_database_name" {
    type        = string
    description = "Redshift database name"
}

variable "redshift_master_username" {
    type        = string
    description = "Redshift master username"
}

variable "redshift_master_password" {
    type        = string
    description = "Redshift master password"
}

variable "redshift_node_type" {
    type        = string
    description = "Redshift node type"
    default     = "dc2.large"
}

variable "redshift_cluster_type" {
    type        = string
    description = "Redshift cluster type"
    default     = "single-node"  // options are single-node or multi-node
}

variable "redshift_number_of_nodes" {
    type        = string
    description = "Number of redshift cluster nodes"
    default     = 1
}