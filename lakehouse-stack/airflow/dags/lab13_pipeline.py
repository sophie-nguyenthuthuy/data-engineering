from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta

# DataHub Airflow plugin entities — emit dataset lineage on each run.
# Plugin is installed via _PIP_ADDITIONAL_REQUIREMENTS in docker-compose.yml.
try:
    from datahub_airflow_plugin.entities import Dataset
    DATAHUB_AVAILABLE = True
except ImportError:
    DATAHUB_AVAILABLE = False

def _ds(platform, name):
    """Helper to build a Dataset URN entry only if the plugin is loaded."""
    return [Dataset(platform, name)] if DATAHUB_AVAILABLE else []

def check_data_ready():
    print("Checking if input data is ready...")
    return True

def validate():
    print("Validating input data...")
    pass

default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="lab13_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['lab13', 'datahub-lineage']
) as dag:

    # 1. Sensor — waits for raw input file
    wait_for_data = PythonSensor(
        task_id="wait_for_input_data",
        python_callable=check_data_ready,
        mode="poke",
        timeout=600,
        poke_interval=10,
    )

    # 2. Ingest: reads source orders from Postgres into Bronze
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="echo 'Ingesting data from source...'",
        inlets=_ds("postgres", "de_db.public.orders"),
        outlets=_ds("postgres", "de_db.bootcamp_dw.fact_sales"),
    )

    # 3. Validate (PythonOperator) — passes Bronze through quality gate
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate,
        inlets=_ds("postgres", "de_db.bootcamp_dw.fact_sales"),
        outlets=_ds("postgres", "de_db.bootcamp_dw.fact_sales"),
    )

    # 4. Transform: build Silver star-schema fact + dimensions
    transform = BashOperator(
        task_id="transform_data",
        bash_command="echo 'Transforming data...'",
        inlets=_ds("postgres", "de_db.bootcamp_dw.fact_sales"),
        outlets=_ds("postgres", "de_db.lab06_dw.fact_orders"),
    )

    # 5. Publish: writes Gold customer_sales
    publish = BashOperator(
        task_id="publish_data",
        bash_command="echo 'Publishing to Gold layer...'",
        trigger_rule="all_success",
        inlets=_ds("postgres", "de_db.lab06_dw.fact_orders"),
        outlets=_ds("postgres", "de_db.lab06_dw.dim_customer"),
    )

    # Explicit dependency chain
    wait_for_data >> ingest >> validate_task >> transform >> publish
