#!/bin/bash
mkdir -p ./dags ./logs ./plugins
echo "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up -d
docker-compose run airflow-worker bash << EOF
airflow variables set exchangerate_db_secret '{"db_host": "exchangerate-db", "db_port": "5432", "db_database": "exchangerate", "db_user": "exchangerate", "db_password": "exchangerate"}'
airflow variables set pairs '["BTC/USD"]'
airflow connections add 'exchangerate_db' --conn-uri 'postgres://exchangerate:exchangerate@exchangerate-db:5432/exchangerate'
EOF
