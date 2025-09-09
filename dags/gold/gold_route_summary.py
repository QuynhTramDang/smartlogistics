# dags/gold_build_routes_summary.py  (final patched)
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
    dag_id="gold_build_routes_summary",
    default_args=default_args,
    description="Build gold.routes_summary from silver + distance_matrix",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold","routes"]
) as dag:

    spark_submit_cmd = Variable.get("spark_submit_cmd", "/opt/bitnami/spark/bin/spark-submit")
    job_path = Variable.get("gold_routes_summary_job_path", "/opt/airflow/jobs/gold/gold_route_summary.py")

    minio_endpoint = Variable.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", "minioadmin")
    metastore_uri = Variable.get("HIVE_METASTORE_URI", "thrift://delta-metastore:9083")
    gold_path = Variable.get("gold_path", "s3a://smart-logistics/gold")
    silver_warehouse_task = Variable.get("silver_warehouse_task", "s3a://smart-logistics/silver/warehouse_task")
    silver_outbound_item = Variable.get("silver_outbound_item", "s3a://smart-logistics/silver/outbound_delivery_item")
    silver_outbound_address = Variable.get("silver_outbound_address", "s3a://smart-logistics/silver/outbound_delivery_address")
    silver_outbound_header = Variable.get("silver_outbound_header", "s3a://smart-logistics/silver/outbound_delivery_header")
    catalog_db = Variable.get("catalog_db", "smartlogistics")
    catalog_table = Variable.get("catalog_table_routes_summary", "routes_summary")

    # packages (Delta) — if your spark-submit/container cannot download packages, put the jar in $SPARK_HOME/jars instead
    delta_packages = "--packages io.delta:delta-core_2.12:2.4.0"

    # Spark confs for S3/MinIO and Hive Metastore
    s3_confs = (
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_access} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_secret} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        # optionally disable SSL if using http endpoint
        # f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    )

    # Hive + Delta configs (must be present)
    hive_delta_confs = (
        f"--conf spark.sql.catalogImplementation=hive "
        f"--conf hive.metastore.uris={metastore_uri} "
        f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        f"--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        # Delta on S3/MinIO
        f"--conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore "
    )

    # Extra safety confs (optional, helpful in some MinIO envs)
    extra_confs = (
        f"--conf spark.sql.warehouse.dir=/user/hive/warehouse "
    )

    # Build the spark-submit command and pass --metastore_uri to the Python script (args)
    cmd = (
        f"{spark_submit_cmd} "
        "--master spark://spark-master:7077 "
        f"{delta_packages} "
        f"{hive_delta_confs} "
        f"{s3_confs} "
        f"{extra_confs} "
        f"{job_path} "
        f"--gold_path {gold_path} "
        f"--silver_warehouse_task {silver_warehouse_task} "
        f"--silver_outbound_item {silver_outbound_item} "
        f"--silver_outbound_address {silver_outbound_address} "
        f"--silver_outbound_header {silver_outbound_header} "
        f"--catalog_db {catalog_db} "
        f"--catalog_table {catalog_table} "
        f"--metastore_uri {metastore_uri} "
    )

    env_vars = {
        "AWS_ACCESS_KEY_ID": minio_access,
        "AWS_SECRET_ACCESS_KEY": minio_secret,
        # keep HIVE_METASTORE_URI too (if other parts use env)
        "HIVE_METASTORE_URI": metastore_uri,
        "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
        "SPARK_HOME": "/opt/spark",
    }

    run = BashOperator(
        task_id="spark_submit_build_routes_summary",
        bash_command=cmd,
        env=env_vars,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    run
