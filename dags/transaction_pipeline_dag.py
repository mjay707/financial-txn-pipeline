import logging
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from config.config import (
    DAG_OWNER,
    DAG_RETRIES,
    DAG_RETRY_DELAY,
    DAG_START_DATE,
    DAG_SCHEDULE,
    DAG_CATCHUP,
    SOURCE_FOLDER,
    FILE_NAME_PATTERN,
    S3_BUCKET,
    S3_BRONZE_PREFIX,
)

logger = logging.getLogger(__name__)

default_args = {
    'owner': DAG_OWNER,
    'retries': DAG_RETRIES,
    'retry_delay': DAG_RETRY_DELAY,
    'email_on_failure': True,
    'email': ['mjay2911@gmail.com'],
}

@dag(
    dag_id='financial_txn_pipeline',
    schedule=DAG_SCHEDULE,
    start_date=DAG_START_DATE,
    catchup=DAG_CATCHUP,
    default_args=default_args,
    tags=['financial', 'batch', 'production'],
)
def financial_txn_pipeline():

    @task
    def check_source_file(**context):
        import os

        hour = context['execution_date'].strftime('%H')
        date = context['execution_date'].strftime('%Y_%m_%d')
        file_name = FILE_NAME_PATTERN.format(date=date, hour=hour)
        file_path = f"{SOURCE_FOLDER}/{file_name}"

        logger.info("Checking source file: %s", file_path)

        if not os.path.exists(file_path):
            logger.error("Source file missing: %s", file_path)
            raise FileNotFoundError(f"Source file missing: {file_path}")

        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.error("Source file is empty: %s", file_path)
            raise ValueError(f"Source file is empty: {file_path}")

        logger.info("File found. Size: %.2f KB", file_size / 1024)
        return file_path

    # ── dependencies ──
    check_source_file()

financial_txn_pipeline()