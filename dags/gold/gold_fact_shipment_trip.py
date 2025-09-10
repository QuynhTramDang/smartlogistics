# dags/gold_fact_shipment_trip.py
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
    dag_id="gold_fact_shipment_trip",
    default_args=default_args,
    description="Build gold.fact_shipment_trip (hourly)",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "fact", "shipment"],
) as dag:

    spark_submit_cmd = Variable.get("spark_submit_cmd", "/opt/spark/bin/spark-submit")
    job_path = Variable.get("gold_fact_shipment_trip_job_path", "/opt/airflow/jobs/gold/gold_fact_shipment_trip.py")

    minio_endpoint = Variable.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access = Variable.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = Variable.get("MINIO_SECRET_KEY", "minioadmin")

    odh_path = Variable.get("silver_outbound_delivery_header_path", "s3a://smart-logistics/silver/outbound_delivery_header")
    oda_path = Variable.get("silver_outbound_delivery_address_path", "s3a://smart-logistics/silver/outbound_delivery_address")
    dl_path  = Variable.get("gold_dim_location_path", "s3a://smart-logistics/gold/dim_location")
    ddm_path = Variable.get("gold_dim_distance_matrix_path", "s3a://smart-logistics/gold/dim_distance_matrix")
    dwh_path = Variable.get("gold_dim_warehouse_path", "s3a://smart-logistics/gold/dim_warehouse")
    gold_path = Variable.get("gold_base_path", "s3a://smart-logistics/gold")
    batch_id = Variable.get("gold_fact_shipment_batch_id", f"batch_{{{{ ds_nodash }}}}")

    s3_confs = (
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_access} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_secret} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.executorEnv.AWS_ACCESS_KEY_ID={minio_access} "
        f"--conf spark.executorEnv.AWS_SECRET_ACCESS_KEY={minio_secret} "
    )
    metastore_uri = Variable.get("HIVE_METASTORE_URI", "thrift://delta-metastore:9083")
    metastore_conf = f"--conf spark.sql.catalogImplementation=hive --conf hive.metastore.uris={metastore_uri} "

    cmd = (
        f"{spark_submit_cmd} "
        "--master spark://spark-master:7077 "
        "--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 "
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"{s3_confs} {metastore_conf} "
        f"--py-files /opt/airflow/jobs/silver/silver_utils.py "
        f"{job_path} "
        f"--odh_path {odh_path} "
        f"--oda_path {oda_path} "
        f"--dl_path {dl_path} "
        f"--ddm_path {ddm_path} "
        f"--dwh_path {dwh_path} "
        f"--gold_path {gold_path} "
        f"--batch_id {batch_id} "
        f"--on_time_threshold 15 "
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
        task_id="run_gold_fact_shipment_trip",
        bash_command=cmd,
        env=env_vars,
        retries=2,
    )

    run
