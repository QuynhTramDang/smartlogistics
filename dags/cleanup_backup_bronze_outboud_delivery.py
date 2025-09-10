# dags/cleanup_backup_bronze_outboud_delivery.py
from cleanup_backup_bronze_common import build_cleanup_backup_dag

dag = build_cleanup_backup_dag(
    dag_id="cleanup_backup_bronze_outbound_delivery",
    config_filename="bronze_outbound_delivery.json"
)()
