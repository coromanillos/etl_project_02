# Dockerfile

# Use official Airflow image with Python 3.10
FROM apache/airflow:3.0.2-python3.10

# Switch to root to install dependencies
USER root

# Copy and install Python requirements
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy source code
COPY ./airflow/dags /opt/airflow/dags
COPY ./src /opt/airflow/src
COPY ./models /opt/airflow/models

# Set permissions if needed (optional)
RUN chown -R airflow: /opt/airflow

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Set default entrypoint
ENTRYPOINT ["/entrypoint.sh"]
