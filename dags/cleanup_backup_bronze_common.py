# dags/cleanup_backup_bronze_common.py
import os
import json
import logging
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from zoneinfo import ZoneInfo
import boto3

def build_cleanup_backup_dag(dag_id: str, config_filename: str):

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.abspath(os.path.join(CURRENT_DIR, "..", "bronze", "config", config_filename))

    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)

    AWS_CONN_ID = config.get('AWS_CONN_ID')
    S3_BUCKET = config.get('S3_BUCKET')
    N_MONTHS_TO_KEEP = config.get('N_MONTHS_TO_KEEP', 3)  # default 3 months if not set

    default_args = {
        'owner': 'data_engineer',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2025, 7, 1),
        catchup=False,
        tags=['cleanup', 'backup', dag_id],
        max_active_runs=1,
    )
    def cleanup_backup_common_dag():

        @task
        def delete_old_files():
            vietnam_tz = ZoneInfo('Asia/Ho_Chi_Minh')
            now_vn = datetime.now(vietnam_tz)
            cutoff_date = now_vn - timedelta(days=N_MONTHS_TO_KEEP * 30)

            conn = BaseHook.get_connection(AWS_CONN_ID)
            session = boto3.session.Session(
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='us-east-1'
            )
            s3 = session.client('s3', endpoint_url=conn.extra_dejson.get("endpoint_url"))

            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix='backup/')

            deleted_files = []
            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    last_modified = obj['LastModified']

                    if last_modified.replace(tzinfo=None) < cutoff_date.replace(tzinfo=None):
                        s3.delete_object(Bucket=S3_BUCKET, Key=key)
                        deleted_files.append({'key': key, 'last_modified': str(last_modified)})

            logging.info(f"[{dag_id}] Deleted {len(deleted_files)} files older than {N_MONTHS_TO_KEEP} months")
            if deleted_files:
                logging.info(json.dumps(deleted_files, indent=2))

        delete_old_files()

    return cleanup_backup_common_dag
