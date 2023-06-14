docker-start:
	docker compose up -d

docker-airflow-connections:
	docker exec scheduler sh -c 'airflow connections add --conn-uri '\''postgres://airflow:airflow@postgres:5432/airflow'\'' postgres_sale_db && \
	airflow connections add --conn-extra '\''{"aws_access_key_id": "AKIA33I2NGIK7HI25JY3", "aws_secret_access_key": "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", "region": "ap-southeast-1"}'\'' --conn-type aws aws && \
	airflow connections add --conn-uri '\''postgres://vupham:vudet11Q@vupham-redshift-cluster.c3otdg01elsn.ap-southeast-1.redshift.amazonaws.com:5439/redshift_main_db'\'' redshift'

# Start docker compose
up: docker-start 

# Add connections to airflow
connections: docker-airflow-connections

# Set up cloud infrastructure
infra-init: 
	terraform -chdir=./terraform init

infra-plan: 
	terraform -chdir=./terraform plan

infra-up: 
	terraform -chdir=./terraform apply --auto-approve

infra-down: 
	terraform -chdir=./terraform destroy --auto-approve