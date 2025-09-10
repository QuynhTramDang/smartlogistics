# dags/gold_build_osm_graph_distance_matrix.py
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import shlex

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_osrm_distance_matrix",
    default_args=default_args,
    description="Build distance matrix using OSRM (chunked table API)",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "osrm", "distance"],
) as dag:

    spark_submit_cmd = Variable.get("spark_submit_cmd", default_var="/opt/spark/bin/spark-submit")
    job_path = Variable.get("gold_osrm_job_path", default_var="/opt/airflow/jobs/gold/gold_dim_distance_matrix.py")

    # MinIO / S3 variables
    minio_endpoint = Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin")

    # Job-specific
    gold_path = Variable.get("gold_path", default_var="s3a://smart-logistics/gold")
    osrm_url = Variable.get("OSRM_URL", default_var="http://osrm:5000")
    top_n = Variable.get("distance_top_n", default_var="300")
    orig_chunk_size = Variable.get("orig_chunk_size", default_var="50")
    dest_chunk_size = Variable.get("dest_chunk_size", default_var="50")
    num_partitions = Variable.get("distance_num_partitions", default_var="20")

    # metastore & catalog naming
    metastore_uri = Variable.get("HIVE_METASTORE_URI", default_var="thrift://delta-metastore:9083")
    catalog_db = Variable.get("catalog_db", default_var="smartlogistics")
    catalog_table = Variable.get("catalog_table", default_var="dim_distance_matrix")

    # s3 confs passed to spark-submit (driver + executors)
    s3_confs = (
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_access} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_secret} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        f"--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        f"--conf spark.executorEnv.AWS_ACCESS_KEY_ID={minio_access} "
        f"--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY={minio_secret} "
    )

    metastore_conf = f"--conf hive.metastore.uris={metastore_uri} --conf spark.sql.catalogImplementation=hive "

    cmd = (
        f"{spark_submit_cmd} "
        "--master spark://spark-master:7077 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"{s3_confs} "
        f"{metastore_conf} "
        f"{job_path} "
        f"--gold_path {shlex.quote(gold_path)} "
        f"--top_n {top_n} "
        f"--orig_chunk_size {orig_chunk_size} "
        f"--dest_chunk_size {dest_chunk_size} "
        f"--num_partitions {num_partitions} "
        f"--osrm_url {shlex.quote(osrm_url)} "
        f"--metastore_uri {shlex.quote(metastore_uri)} "
        f"--s3_endpoint {shlex.quote(minio_endpoint)} "
        f"--s3_key {shlex.quote(minio_access)} "
        f"--s3_secret {shlex.quote(minio_secret)} "
        f"--catalog_db {shlex.quote(catalog_db)} "
        f"--catalog_table {shlex.quote(catalog_table)} "
    )

    # Environment for spark-submit (driver env vars)
    env_vars = {
        "AWS_ACCESS_KEY_ID": minio_access,
        "AWS_SECRET_ACCESS_KEY": minio_secret,
        "HIVE_METASTORE_URI": metastore_uri,
        "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
        "SPARK_HOME": "/opt/spark",
    }

    run_build = BashOperator(
        task_id="spark_submit_build_osrm_distance_matrix",
        bash_command=cmd,
        env=env_vars,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    run_build
