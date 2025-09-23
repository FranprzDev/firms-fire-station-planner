FROM astrocrpublic.azurecr.io/runtime:3.0-11

# Dentro de tu Dockerfile para el entorno de Airflow
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate


