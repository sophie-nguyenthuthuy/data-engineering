# LAB 13 — WORKFLOW ORCHESTRATION WITH APACHE AIRFLOW: Submission

## 1. Airflow UI DAG Graph Screenshot
*(Using the artifact captured via browser testing at `airflow_dag_graph_view_1775184600152.png`)*
![Airflow DAG Graph](file:///Users/Thuy/.gemini/antigravity/brain/a0ceb3fb-1350-465c-8b3b-619504db04da/airflow_dag_graph_view_1775184600152.png)

## 2. DAG File (`lab13_pipeline.py`)
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta

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
    tags=['lab13']
) as dag:

    # 1. Sensor
    wait_for_data = PythonSensor(
        task_id="wait_for_input_data",
        python_callable=check_data_ready,
        mode="poke",
        timeout=600,
        poke_interval=10
    )

    # 2. Ingest (BashOperator)
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="echo 'Ingesting data from source...'"
    )

    # 3. Validate (PythonOperator)
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate
    )

    # 4. Transform
    transform = BashOperator(
        task_id="transform_data",
        bash_command="echo 'Transforming data...'"
    )

    # 5. Publish
    publish = BashOperator(
        task_id="publish_data",
        bash_command="echo 'Publishing to Gold layer...'",
        trigger_rule="all_success"
    )

    # Explicit Dependency
    wait_for_data >> ingest >> validate_task >> transform >> publish
```

## 3. Log of a Successful DAG Run
```text
[2026-04-03T02:51:07.269+0000] {dag.py:4028} INFO - [DAG TEST] starting task_id=transform_data map_index=-1
[2026-04-03T02:51:07.276+0000] {taskinstance.py:2481} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='admin' AIRFLOW_CTX_DAG_ID='lab13_end_to_end_pipeline' AIRFLOW_CTX_TASK_ID='transform_data'
[2026-04-03T02:51:07.277+0000] {subprocess.py:75} INFO - Running command: ['/usr/bin/bash', '-c', "echo 'Transforming data...'"]
[2026-04-03T02:51:07.281+0000] {subprocess.py:86} INFO - Output:
[2026-04-03T02:51:07.282+0000] {subprocess.py:93} INFO - Transforming data...
[2026-04-03T02:51:07.282+0000] {subprocess.py:97} INFO - Command exited with return code 0
[2026-04-03T02:51:07.286+0000] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=lab13_end_to_end_pipeline, task_id=transform_data
[2026-04-03T02:51:07.291+0000] {dag.py:4042} INFO - [DAG TEST] end task task_id=transform_data map_index=-1
[2026-04-03T02:51:07.295+0000] {dag.py:4028} INFO - [DAG TEST] starting task_id=publish_data map_index=-1
[2026-04-03T02:51:07.307+0000] {taskinstance.py:2481} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='admin' AIRFLOW_CTX_DAG_ID='lab13_end_to_end_pipeline' AIRFLOW_CTX_TASK_ID='publish_data'
[2026-04-03T02:51:07.307+0000] {subprocess.py:75} INFO - Running command: ['/usr/bin/bash', '-c', "echo 'Publishing to Gold layer...'"]
[2026-04-03T02:51:07.311+0000] {subprocess.py:86} INFO - Output:
[2026-04-03T02:51:07.312+0000] {subprocess.py:93} INFO - Publishing to Gold layer...
[2026-04-03T02:51:07.312+0000] {subprocess.py:97} INFO - Command exited with return code 0
[2026-04-03T02:51:07.316+0000] {taskinstance.py:1138} INFO - Marking task as SUCCESS. dag_id=lab13_end_to_end_pipeline, task_id=publish_data
[2026-04-03T02:51:07.320+0000] {dagrun.py:732} INFO - Marking run <DagRun lab13_end_to_end_pipeline @ 2026-04-03 00:00:00+00:00: manual__2026-04-03T00:00:00+00:00, state:running, queued_at: None. externally triggered: False> successful
```

## 4. Short Answers

**Airflow khác cron ở đâu?**
Airflow là một hệ thống thiết kế và điều phối workflow chuyên nghiệp hỗ trợ quản lý các luồng phụ thuộc (dependencies) cực kỳ phức tạp (DAGs). Nó có thông tin trạng thái metadata, log, tính năng backfilling/catchup, automated retries tích hợp, và khả năng scale trên nhiều worker nodes. Ngược lại, Cron chỉ đơn thuần là bộ kích hoạt hẹn giờ độc lập, không có khả năng thiết lập sự phụ thuộc giữa các process, và hoàn toàn thiếu cơ chế logging hay quản lý trạng thái khi một công việc bị ngắt giữa chừng.

**DAG là gì?**
DAG stands for "Directed Acyclic Graph" (Đồ thị có hướng phi chu trình). Trong Airflow, DAG được dùng để mô hình hóa toàn bộ một workflow nơi mà dữ liệu luân chuyển theo luồng duy nhất thông qua các Task mà không bao giờ quay lại các bước đã hoàn tất (không tạo vòng lặp vô hạn).

**Khi nào dùng sensor?**
Sensor là một loại Operator đặc biệt dùng để lắng nghe/chờ đợi một sự kiện (event) hay điều kiện thoả mãn cụ thể trước khi thực thi các chuỗi tác vụ logic bên dưới. Ví dụ ứng dụng là dùng FileSensor để đợi file CSV được đẩy lên SFTP/S3, hoặc dùng TimeSensor để đảm bảo quá trình ingest không bắt đầu cho đến khi qua 5:00 AM server time. Lợi thế là sensor sẽ tự idle poke rồi timeout mà không tốn công sức làm các logic ngắt script thủ công.
