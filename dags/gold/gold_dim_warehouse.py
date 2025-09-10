# dags/gold_dim_warehouse.py

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gold_build_dim_warehouse",
    default_args=default_args,
    description="Build/upsert gold.dim_warehouse from silver warehouse_order/task and ref map",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "warehouse", "spark"],
) as dag:

    # spark binary and job path
    spark_submit_cmd = Variable.get("spark_submit_cmd", default_var="/opt/spark/bin/spark-submit")
    job_path = Variable.get(
        "gold_dim_warehouse_job_path",
        default_var="/opt/airflow/jobs/gold/gold_dim_warehouse.py",
    )

    # MinIO / S3 variables (same pattern as geocode dag)
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin")
    minio_ssl = Variable.get("MINIO_SSL", default_var="false")  # "true" or "false"

    warehouse_order_path = Variable.get("warehouse_order_path", default_var="s3a://smart-logistics/silver/warehouse_order")
    warehouse_task_path = Variable.get("warehouse_task_path", default_var="s3a://smart-logistics/silver/warehouse_task")
    warehouse_map_csv = Variable.get("warehouse_map_csv", default_var="s3a://smart-logistics/reference/warehouse_location_map.csv")
    gold_path = Variable.get("gold_path", default_var="s3a://smart-logistics/gold")
    metastore_uri = Variable.get("HIVE_METASTORE_URI", default_var="thrift://delta-metastore:9083")

    # catalog/db/table names
    catalog_db = Variable.get("catalog_db", default_var="smartlogistics")
    catalog_table = Variable.get("catalog_table", default_var="dim_warehouse")

    # Build spark-submit confs for s3a / minio 
    s3_confs = (
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_access} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_secret} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled={minio_ssl} "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        f"--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        f"--conf spark.executorEnv.AWS_ACCESS_KEY_ID={minio_access} "
        f"--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY={minio_secret} "
        f"--conf spark.executorEnv.PYSPARK_PYTHON=/usr/local/bin/python3.12 "
    )

    metastore_conf = (
        f"--conf spark.sql.catalogImplementation=hive "
        f"--conf hive.metastore.uris={metastore_uri} "
    )

   
    cmd = (
        f"{spark_submit_cmd} "
        "--master spark://spark-master:7077 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "--packages io.delta:delta-core_2.12:2.4.0 "
        f"{s3_confs} "
        f"{metastore_conf} "
        f"{job_path} "
        f"--warehouse_order_path {warehouse_order_path} "
        f"--warehouse_task_path {warehouse_task_path} "
        f"--warehouse_map_csv {warehouse_map_csv} "
        f"--gold_path {gold_path} "
        f"--metastore_uri {metastore_uri} "
        f"--catalog_db {catalog_db} "
        f"--catalog_table {catalog_table} "
    )

    # Env for spark-submit (driver env vars)
    env_vars = {
        "AWS_ACCESS_KEY_ID": minio_access,
        "AWS_SECRET_ACCESS_KEY": minio_secret,
        "HIVE_METASTORE_URI": metastore_uri,
        "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
        "SPARK_HOME": "/opt/spark",
    }

    run_build = BashOperator(
        task_id="spark_submit_build_dim_warehouse",
        bash_command=cmd,
        env=env_vars,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    run_build
