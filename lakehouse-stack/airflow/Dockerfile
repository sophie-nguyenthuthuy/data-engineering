# Bootcamp Airflow image with DataHub lineage plugin pre-baked.
# Avoids the Airflow 2.8.0 entrypoint quirk where _PIP_ADDITIONAL_REQUIREMENTS
# silently fails when the container runs as root.
FROM apache/airflow:2.8.0

USER airflow
RUN pip install --no-cache-dir "acryl-datahub-airflow-plugin==0.14.1.2"
