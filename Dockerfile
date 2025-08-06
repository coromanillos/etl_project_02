# Use official Airflow image with Python 3.10
FROM apache/airflow:3.0.2-python3.10

# Switch to root to install system dependencies
USER root

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy project files for dependency installation
COPY pyproject.toml poetry.lock /app/

WORKDIR /app

# Install project dependencies via Poetry
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

# Copy the actual code
COPY ./airflow/dags /opt/airflow/dags
COPY ./src /opt/airflow/src
COPY ./models /opt/airflow/models

# Set permissions (optional)
RUN chown -R airflow: /opt/airflow

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Default entrypoint
ENTRYPOINT ["/entrypoint.sh"]
