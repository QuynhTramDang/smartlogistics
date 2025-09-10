# dags/gold_dim_time.py

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_dim_time",
    default_args=default_args,
    description="Build gold.dim_time daily calendar from silver sources",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dim", "time"],
) as dag:

    spark_submit_cmd = Variable.get("spark_submit_cmd", "/opt/spark/bin/spark-submit")
    job_path = Variable.get("gold_dim_time_job_path", "/opt/airflow/jobs/gold/gold_dim_time.py")

    minio_endpoint = Variable.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", "minioadmin")
    minio_ssl = Variable.get("MINIO_SSL", "false")

    silver_paths = Variable.get("gold_dim_time_silver_paths",
                                default_var="s3a://smart-logistics/silver/outbound_delivery_header,s3a://smart-logistics/silver/sales_order,s3a://smart-logistics/silver/warehouse_order,s3a://smart-logistics/silver/warehouse_task")

    gold_path = Variable.get("gold_base_path", "s3a://smart-logistics/gold")
    metastore_uri = Variable.get("HIVE_METASTORE_URI", "thrift://delta-metastore:9083")

    s3_confs = (
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_access} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_secret} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.executorEnv.AWS_ACCESS_KEY_ID={minio_access} "
        f"--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY={minio_secret} "
    )
    metastore_conf = f"--conf spark.sql.catalogImplementation=hive --conf hive.metastore.uris={metastore_uri} "

    cmd = (
        f"{spark_submit_cmd} "
        "--master spark://spark-master:7077 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        "--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 "
        f"{s3_confs} {metastore_conf} "
        f"--py-files /opt/airflow/jobs/silver/silver_utils.py "   
        f"{job_path} "
        f"--silver_paths \"{silver_paths}\" "
        f"--gold_path {gold_path} "
        f"--s3_endpoint {minio_endpoint} "
        f"--s3_key {minio_access} "
        f"--s3_secret {minio_secret} "
        f"--metastore_uri {metastore_uri} "
    )


    env_vars = {
        "AWS_ACCESS_KEY_ID": minio_access,
        "AWS_SECRET_ACCESS_KEY": minio_secret,
        "HIVE_METASTORE_URI": metastore_uri,
        "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
    }

    run = BashOperator(
        task_id="run_gold_dim_time",
        bash_command=cmd,
        env=env_vars,
        retries=2,
    )

    run
