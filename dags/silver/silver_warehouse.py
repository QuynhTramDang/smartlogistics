# dags/silver_warehouse.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys

# Add path to job scripts
sys.path.insert(0, "/opt/airflow/jobs")

from silver.warehouse.silver_run import run_silver_job
from silver.warehouse.silver_config import ALL_SILVER_CONFIGS
from silver.silver_utils import JSONFormatter

# ---------- Logger setup ----------
logger = logging.getLogger("silver_warehouse")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

# ---------- DAG config ----------
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'silver_warehouse',
    default_args=default_args,
    description='DAG run silver layer for warehouse entities',
    schedule_interval='0 4 * * *',
    start_date=datetime(2025, 7, 25),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'warehouse'],
)

# ---------- Main callable ----------
def run_job_airflow_wrapper(job_name, **kwargs):
    batch_id = f"airflow_{kwargs['ds_nodash']}_{kwargs['ts_nodash']}"
    logger.info(f"Running silver job {job_name} with batch_id {batch_id}")
    run_silver_job(job_name, batch_id)

# ---------- Define tasks from ALL_SILVER_CONFIGS ----------
tasks = {}

for cfg in ALL_SILVER_CONFIGS:
    job_name = cfg["job_name"]
    task_id = f"silver_{job_name}"  
    tasks[task_id] = PythonOperator(
        task_id=task_id,
        python_callable=run_job_airflow_wrapper,
        op_kwargs={"job_name": job_name},
        dag=dag,
    )

# ---------- Define execution order ----------
tasks["silver_warehouse_order"] >> tasks["silver_warehouse_task"]
