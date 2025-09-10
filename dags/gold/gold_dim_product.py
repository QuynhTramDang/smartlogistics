# dags/gold_dim_product.py

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
    dag_id="gold_dim_product",
    default_args=default_args,
    description="Build gold.dim_product from silver.sales_order_item + silver.outbound_delivery_item",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dim", "product"],
) as dag:
    spark_submit_cmd = Variable.get("spark_submit_cmd", default_var="/opt/spark/bin/spark-submit")
    job_path = Variable.get("gold_dim_product_job_path", default_var="/opt/airflow/jobs/gold/gold_dim_product.py")

    # S3 / MinIO variables
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin")
    minio_ssl = Variable.get("MINIO_SSL", default_var="false")

    # input silver + output gold
    so_path = Variable.get("silver_sales_order_item_path", default_var="s3a://smart-logistics/silver/sales_order_item")
    odi_path = Variable.get("silver_outbound_delivery_item_path", default_var="s3a://smart-logistics/silver/outbound_delivery_item")
    gold_path = Variable.get("gold_base_path", default_var="s3a://smart-logistics/gold")

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
    metastore_uri = Variable.get("HIVE_METASTORE_URI", default_var="thrift://delta-metastore:9083")
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
        f"--py-files /opt/airflow/jobs/silver/silver_utils.py "
        f"{job_path} "
        f"--so_path {so_path} "
        f"--odi_path {odi_path} "
        f"--gold_path {gold_path} "
        f"--batch_id batch_{{{{ ds_nodash }}}} "
    )

    env_vars = {
        "AWS_ACCESS_KEY_ID": minio_access,
        "AWS_SECRET_ACCESS_KEY": minio_secret,
        "HIVE_METASTORE_URI": metastore_uri,
        "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
        "SPARK_HOME": "/opt/spark",
    }

    run_gold_dim_product = BashOperator(
        task_id="run_gold_dim_product",
        bash_command=cmd,
        env=env_vars,
        retries=2,
    )

    run_gold_dim_product
