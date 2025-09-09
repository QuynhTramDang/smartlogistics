import os
import json
import logging
import requests
import tempfile
import time
import gzip
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import boto3
from zoneinfo import ZoneInfo

VIETNAM_TZ = ZoneInfo('Asia/Ho_Chi_Minh')

def load_config(config_path: str):
    with open(config_path, 'r') as f:
        config = json.load(f)

    special_keys = {'BATCH_SIZE', 'S3_BUCKET', 'POSTGRES_CONN_ID', 'AWS_CONN_ID'}
    return {
        "config": config,
        "object_names": [k for k in config if k not in special_keys],
        "batch_size": config.get('BATCH_SIZE'),
        "s3_bucket": config.get('S3_BUCKET'),
        "postgres_conn_id": config.get('POSTGRES_CONN_ID'),
        "aws_conn_id": config.get('AWS_CONN_ID')
    }

def setup_logger(object_name: str, batch_index: int):
    def log(event: str, extra: dict = None):
        log_data = {
            "object": object_name,
            "batch": batch_index,
            "event": event,
            "timestamp": datetime.now(VIETNAM_TZ).isoformat()
        }
        if extra:
            log_data.update(extra)
        logging.info(json.dumps(log_data))
    return log

def get_batches_for_object_task(config_dict, api_key_env_var):
    @task
    def _inner(object_name: str, execution_date=None):
        object_conf = config_dict["config"][object_name]
        api_url = object_conf["api_url"]
        prefix = object_conf["prefix"]
        table_name = object_conf["table_name"]
        custom_headers = object_conf.get("headers")

        date_str = datetime.now(VIETNAM_TZ).strftime('%Y-%m-%d')
        logger = setup_logger(object_name, -1)
        logger("start_fetching", {"object_name": object_name})

        # Setup connections
        conn = BaseHook.get_connection(config_dict["aws_conn_id"])
        session = boto3.session.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name='us-east-1'
        )
        s3 = session.client('s3', endpoint_url=conn.extra_dejson.get("endpoint_url"))
        pg = PostgresHook(postgres_conn_id=config_dict["postgres_conn_id"])

        skip = 0
        batch_index = 0
        results = []
        api_key = os.environ.get(api_key_env_var)

        # Headers: merge apiKey with custom or default headers
        default_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        }
        headers = custom_headers if custom_headers else default_headers
        headers = {**headers, "apikey": api_key}  # ensure API key is always included

        while True:
            # Check idempotent
            count = pg.get_first(
                f"SELECT COUNT(1) FROM {table_name} WHERE batch_index=%s AND execution_date=%s",
                parameters=(batch_index, execution_date)
            )
            if count and count[0] > 0:
                logger = setup_logger(object_name, batch_index)
                logger("batch_skipped", {"reason": "already_processed"})
                skip += config_dict["batch_size"]
                batch_index += 1
                continue

            # Fetch API
            full_url = f"{api_url}&$top={config_dict['batch_size']}&$skip={skip}" \
                if '?' in api_url else f"{api_url}?$top={config_dict['batch_size']}&$skip={skip}"
            logger = setup_logger(object_name, batch_index)
            logger("fetching_batch", {"url": full_url})

            try:
                response = requests.get(full_url, headers=headers, timeout=30)
                response.raise_for_status()
            except Exception as e:
                logger("api_error", {"error": str(e)})
                break

            # Save temp gzip
            with tempfile.NamedTemporaryFile(delete=False, suffix='.json.gz') as tmp_file:
                with gzip.GzipFile(fileobj=tmp_file, mode='wb') as gz_file:
                    gz_file.write(response.content)
                local_path = tmp_file.name

            file_size = os.path.getsize(local_path)
            if file_size == 0:
                logger("empty_response")
                os.remove(local_path)
                break

            # Upload to S3
            timestamp_str = datetime.now(VIETNAM_TZ).strftime('%Y%m%d_%H%M%S')
            filename = f"{prefix}_batch{batch_index}_{timestamp_str}.json.gz"
            s3_key = f"raw/{prefix}/{date_str}/{filename}"

            upload_success = False
            for attempt in range(3):
                try:
                    s3.upload_file(local_path, config_dict["s3_bucket"], s3_key)
                    logger("upload_success", {"attempt": attempt+1})
                    upload_success = True
                    break
                except Exception as e:
                    logger("upload_retry_failed", {"attempt": attempt+1, "error": str(e)})
                    time.sleep(2)
            os.remove(local_path)

            if not upload_success:
                logger("upload_failed_final")
                break

            # Insert metadata
            pg.run(
                f"""
                INSERT INTO {table_name} (file_path, file_size_bytes, status, batch_index, timestamp, execution_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                parameters=(s3_key, file_size, "success", batch_index, datetime.now(VIETNAM_TZ), execution_date)
            )
            logger("metadata_saved", {"batch_index": batch_index})

            results.append({
                "object_name": object_name,
                "s3_key": s3_key,
                "file_size": file_size,
                "batch_index": batch_index
            })

            if file_size < 500:
                logger("last_batch_detected", {"file_size": file_size})
                break

            skip += config_dict["batch_size"]
            batch_index += 1
            time.sleep(1)

        return results
    return _inner

@task
def flatten_batches(nested_lists):
    return [item for sublist in nested_lists for item in sublist]
