from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Config logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hàm backup
def backup_sales_order():
    logger.info("🔄 Starting backup for bronze_sales_order.json ...")
    # Code backup thực tế của bạn ở đây
    logger.info("✅ Backup completed successfully!")

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Khai báo DAG
with DAG(
    dag_id="backup_bronze_sales_order",
    default_args=default_args,
    description="Backup Bronze Sales Order JSON to storage",
    schedule_interval="@daily",  # hoặc cron expression
    catchup=False,
    tags=["bronze", "backup"]
) as dag:

    task_backup_sales_order = PythonOperator(
        task_id="backup_sales_order_task",
        python_callable=backup_sales_order
    )

    task_backup_sales_order
