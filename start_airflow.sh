#!/bin/bash
set -e

# Fix for Codespaces (proxy + CSRF issue)
export AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True
export AIRFLOW__WEBSERVER__WTF_CSRF_ENABLED=False

echo "========================================="
echo "Initializing Airflow Environment"
echo "========================================="

echo "1. Initializing Airflow Database..."
airflow db init

echo "2. Creating default Admin User (admin / admin)..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "3. Starting Airflow Scheduler in background..."
airflow scheduler &

echo "4. Starting Airflow Webserver on port 8080..."
airflow webserver --port 8080
