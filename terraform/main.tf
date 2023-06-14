# This terraform main.tf sets up the AWS infrastructure for Amazon Redshift, S3 buckets, EC2 instances


# --------------------------- AWS credentials --------------------------- #

terraform {
    required_version = ">= 1.3"

    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "5.0.1"
        }
    }
}

provider "aws" {
    access_key  = var.aws_access_key
    secret_key  = var.aws_secret_key
    region      = var.aws_region
}


# --------------------------- Network/Security configuration --------------------------- #

# VPC
resource "aws_vpc" "server_vpc" {
    cidr_block = var.server_vpc_cidr
}

# Subnet
resource "aws_subnet" "redshift_az_1" {
    vpc_id              = aws_vpc.server_vpc.id
    cidr_block          = var.server_subnet_redshift_1
    availability_zone   = var.availability_zone[0]
    
    tags = {
        Name = "Subnet for redshift az 1"
    }
}

resource "aws_subnet" "redshift_az_2" {
    vpc_id              = aws_vpc.server_vpc.id
    cidr_block          = var.server_subnet_redshift_2
    availability_zone   = var.availability_zone[1]
    
    tags = {
        Name = "Subnet for redshift az 2"
    }
}

# Subnet group
resource "aws_redshift_subnet_group" "redshift_subnet_group" {
    depends_on  = [
        aws_subnet.redshift_az_1,
        aws_subnet.redshift_az_2
    ]

    name        = "redshift-subnet-group"
    subnet_ids  = [
        aws_subnet.redshift_az_1.id, 
        aws_subnet.redshift_az_2.id
    ]
}

# Internet gateway
resource "aws_internet_gateway" "vpc_igw" {
    vpc_id = aws_vpc.server_vpc.id
    
    tags = {
        Name = "VPC Internet gateway"
    }
}

# Route table
resource "aws_route_table" "public_route_igw" {
    vpc_id = aws_vpc.server_vpc.id 

    route {
        cidr_block = "0.0.0.0/0"
        gateway_id = aws_internet_gateway.vpc_igw.id
    }
}

# Attach route table to subnets
resource "aws_route_table_association" "redshift_subnet_1_to_igw" {
    subnet_id       = aws_subnet.redshift_az_1.id
    route_table_id  = aws_route_table.public_route_igw.id
}

resource "aws_route_table_association" "redshift_subnet_2_to_igw" {
    subnet_id       = aws_subnet.redshift_az_2.id
    route_table_id  = aws_route_table.public_route_igw.id
}

# Security Group
resource "aws_default_security_group" "redshift_security_group" {
    depends_on = [aws_vpc.server_vpc]
    
    vpc_id = aws_vpc.server_vpc.id
    
    ingress {
        description = "Redshift Port"
        from_port   = 5439
        to_port     = 5439
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"] 
    }
    
    tags = {
        Name = "redshift_security_group"
    }
}


# --------------------------- IAM Role for Redshift clusters --------------------------- #
resource "aws_iam_role" "redshift_iam_role" {
    name                = "redshift_role"
    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
        {
            Action = "sts:AssumeRole"
            Effect = "Allow"
            Sid    = ""
            Principal = {
            Service = "redshift.amazonaws.com"
            }
        },
        ]
    })
}

resource "aws_iam_role_policy" "redshift_s3_full_access" {
    name = "red_shift_s3_full_access_policy"
    role = aws_iam_role.redshift_iam_role.id
    policy = jsonencode({
        Version   = "2012-10-17"
        Statement = [{
            Action   = "s3:*"
            Effect   = "Allow"
            Resource = "*"
        }, ] 
    })
}


# --------------------------- Create Redshift cluster --------------------------- #

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
        