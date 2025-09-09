from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Config logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hàm backup
def backup_outbound_delivery():
    logger.info("🔄 Starting backup for bronze_outbound_delivery.json ...")
    # Code backup thực tế của bạn ở đây
    logger.info("✅ Backup completed successfully!")

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Khai báo DAG
with DAG(
    dag_id="backup_bronze_outbound_delivery",
    default_args=default_args,
    description="Backup Bronze Outbound Delivery JSON to storage",
    schedule_interval="@daily",  # hoặc cron expression nếu muốn
    catchup=False,
    tags=["bronze", "backup"]
) as dag:

    task_backup_outbound_delivery = PythonOperator(
        task_id="backup_outbound_delivery_task",
        python_callable=backup_outbound_delivery
    )

    task_backup_outbound_delivery
