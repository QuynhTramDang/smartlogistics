import os
from datetime import timedelta
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from bronze.bronze_common import load_config, get_batches_for_object_task, flatten_batches

# --- Load config ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(CURRENT_DIR, "config", "bronze_outbound_delivery.json")
config_dict = load_config(CONFIG_PATH)

# --- Default args ---
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

@dag(
    dag_id='bronze_outbound_delivery',
    default_args=default_args,
    schedule_interval='@hourly', 
    start_date=days_ago(1),
    catchup=False,
    tags=['bronze', 'outbound_delivery'],
    max_active_runs=1,
)
def bronze_outbound_delivery_dag():
    object_list = config_dict["object_names"]
    get_batches = get_batches_for_object_task(config_dict, api_key_env_var="OUTBOUND_DELIVERY_KEY")
    batch_lists = get_batches.expand(object_name=object_list)
    flat_batches = flatten_batches(batch_lists)

dag = bronze_outbound_delivery_dag()
