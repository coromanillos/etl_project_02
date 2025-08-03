#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done

# Initialize database if needed
airflow db upgrade

# Create default user if not exists (can be refactored for idempotency)
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Execute the passed command (e.g., webserver, scheduler)
exec "$@"
