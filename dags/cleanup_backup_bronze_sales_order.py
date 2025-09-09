from cleanup_backup_bronze_common import build_cleanup_backup_dag

dag = build_cleanup_backup_dag(
    dag_id="cleanup_backup_bronze_sales_order",
    config_filename="bronze_sales_order.json"
)()