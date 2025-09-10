# dags/backup_bronze_common.py

from pathlib import Path
import json
import os
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
import boto3
from airflow.utils.dates import days_ago
from zoneinfo import ZoneInfo
import logging

def build_backup_dag(
    dag_id: str,
    config_filename: str,
    days_threshold: int = 7,
    raw_prefix: str = "raw/",
    backup_prefix: str = "backup/"
):
    current_dir = os.path.dirname(__file__)
    config_path = os.path.join(current_dir, "bronze", "config", config_filename)

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        logging.exception("Failed to load backup config")
        default_args = {
            'owner': 'data_engineer',
            'retries': 0,
        }
        @dag(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval='@daily',
            start_date=days_ago(1),
            catchup=False,
            tags=['cleanup', 'bronze'],
        )
        def error_dag():
            @task
            def report_error():
                logging.error(f"Cannot load config {config_path}: {e}")
            report_error()
        return error_dag()

    S3_BUCKET = config.get('S3_BUCKET')
    AWS_CONN_ID = config.get('AWS_CONN_ID')

    default_args = {
        'owner': 'data_engineer',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['cleanup', 'bronze'],
    )
    def backup_dag():
        @task
        def move_old_files():
            conn = BaseHook.get_connection(AWS_CONN_ID)
            session = boto3.session.Session(
                aws_access_key_id=conn.login,
                aws_secret_access_key=conn.password,
                region_name='us-east-1'
            )
            s3 = session.client('s3', endpoint_url=conn.extra_dejson.get("endpoint_url"))

            vn_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
            threshold_date = vn_now - timedelta(days=days_threshold)

            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=raw_prefix)

            for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    last_modified = obj.get('LastModified')
                    if last_modified and last_modified.replace(tzinfo=None) < threshold_date.replace(tzinfo=None):
                        backup_key = key.replace(raw_prefix, backup_prefix, 1)
                        s3.copy_object(
                            Bucket=S3_BUCKET,
                            CopySource={'Bucket': S3_BUCKET, 'Key': key},
                            Key=backup_key
                        )
                        s3.delete_object(Bucket=S3_BUCKET, Key=key)

        move_old_files()

    return backup_dag()
