# Dockerfile

# Use official Airflow image with Python 3.10
FROM apache/airflow:3.0.2-python3.10

# Install dependencies as root
USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs into the container
COPY ./airflow/dags /opt/airflow/dags

# Switch back to airflow user
USER airflow
