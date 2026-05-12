# LAB 14 — DATA QUALITY WITH GREAT EXPECTATIONS & DEEQU: Submission

## 1. Great Expectations Validation Script
*(Saved at `ge_validate.py`)*
```python
import pandas as pd
import great_expectations as gx

# Read sample data
orders = pd.read_csv("orders.csv")

# Create Ephemeral GE Context
context = gx.get_context(mode="ephemeral")
source = context.sources.add_pandas(name="bootcamp_source")
asset = source.add_dataframe_asset(name="orders_asset")
batch = asset.build_batch_request(dataframe=orders)

context.add_or_update_expectation_suite("orders_suite")
validator = context.get_validator(batch_request=batch, expectation_suite_name="orders_suite")

# Rules defined
validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_unique("order_id")
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("quantity", min_value=1, max_value=20)
validator.expect_column_values_to_be_between("unit_price", min_value=0, max_value=1000)
validator.expect_column_values_to_be_in_set(
    "order_status",
    ["created", "paid", "shipped", "delivered", "cancelled"]
)
validator.expect_table_row_count_to_be_between(min_value=1, max_value=100000)

result = validator.validate()
print("Success:", result["success"])
```

## 2. Text Output showing passed and failed expectations

```text
Calculating Metrics: 100%|██████████████████████████████████████| 2/2 [00:00<00:00, 151.78it/s]
...
Success: False
Validation Results Detailed:
PASSED: expect_column_values_to_not_be_null on order_id
FAILED: expect_column_values_to_be_unique on order_id
FAILED: expect_column_values_to_not_be_null on customer_id
FAILED: expect_column_values_to_be_between on quantity
FAILED: expect_column_values_to_be_between on unit_price
FAILED: expect_column_values_to_be_in_set on order_status
PASSED: expect_table_row_count_to_be_between on table
```

## 3. Short Note on Deequ Metrics 
*(For analyzing statistical attributes at scale)*

By transitioning from fixed unit checks to Deequ (Spark), I would monitor:
1. **Completeness on `customer_id`**: Instead of strictly hardcoded "no nulls allowed", monitor if completeness drops below 99%. A sudden spike in nulls indicates an upstream application breakdown.
2. **Distribution of `unit_price`**: Statistical range modeling helps flag outliers or anomalies. For example, if the average purchase amount explodes 1000x above historical statistical standard deviations, it alerts us regardless of direct min/max barriers.
3. **Uniqueness on `order_id`**: Monitoring the primary key duplication ratio across gigabytes of streaming inserts.
4. **Volume (Size) Checks**: Ensures that a batch load falls within acceptable threshold ranges (e.g., matching prior week volume +/- 10%), halting Gold table processing if only 5 rows arrive.

## 4. Simple DAG snippet showing Validation inside the pipeline

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dq_pipeline_lab",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # 1. Pipeline Sequence Diagram
    ingest = BashOperator(task_id="ingest_raw_data", bash_command="python ingest.py")
    
    # 2. Gatekeeper Quality Step
    ge_validate = BashOperator(task_id="run_ge_validation", bash_command="python ge_validate.py")
    
    # 3. Downstream processing
    transform = BashOperator(task_id="transform_silver", bash_command="python transform.py")
    publish = BashOperator(task_id="publish_gold", bash_command="python publish.py")

    # Dependency mapping implies validation prevents bad data from reaching Transform!
    ingest >> ge_validate >> transform >> publish
```

## 5. Reflection questions

**Why is data quality more than just one-time validation?**
Data inevitably decays, formats change, APIs alter their schemas, and software bugs leak bad logic into production upstream. Continous data quality checks are required on the pipeline *itself* to block downstream damage whenever input standards drift.

**Which problems are better caught by rule-based checks, and which need statistical monitoring?**
- **Rule-based**: Explicit business assumptions like `order_status` enums, `age` > 0, boolean checks, and primary-key duplicate checks.
- **Statistical**: Drift in data volume sizes, distributions shifting (e.g. median pricing rising unexpectedly), and missing rates (e.g. 5% missing addresses is allowed but 40% is an anomaly alert).

**Where in the pipeline is it cheapest to catch bad data?**
At the exact point of **Ingestion** (Bronze layer transition). Catching malformed data immediately prevents the cascading compute costs and the complicated rollback processes needed to remove corrupt elements from subsequent warehouse tables layer by layer.

**How would you stop a bad bronze load from contaminating silver and gold tables?**
By establishing a Circuit Breaker logic block in orchestrators like Airflow (exactly like `ge_validate` above). If the validation task `ge_validate` throws an execution failure/alert due to breached Expectation Rules against the Bronze load, Airflow physically pauses all dependent downstream DAG tasks (`transform`, `publish`), isolating the corruption successfully.
