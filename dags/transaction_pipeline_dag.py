import logging
import os
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
    DBT_PROFILES_DIR,
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

    @task
    def bronze_load(file_path, **context):
        
        logger.info("Starting bronze load for file: %s", file_path)

        if not os.path.exists(file_path):
            logger.error("Source file does not exist: %s", file_path)
            raise FileNotFoundError(f"Source file does not exist: {file_path}")

        exec_date = context['execution_date']
        s3_key = (
            f"{S3_BRONZE_PREFIX}/"
            f"year={exec_date.strftime('%Y')}/"
            f"month={exec_date.strftime('%m')}/"
            f"day={exec_date.strftime('%d')}/"
            f"hour={exec_date.strftime('%H')}/"
            f"{os.path.basename(file_path)}"
        )

        hook = S3Hook(aws_conn_id='aws_default')
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )

        logger.info(
            "File %s (%.2f KB) uploaded to s3://%s/%s",
                file_path,
            os.path.getsize(file_path) / 1024,
            S3_BUCKET,
            s3_key
        )

        return s3_key

    silver_pyspark = BashOperator(
        task_id='silver_pyspark',
        bash_command=(
            'spark-submit '
            '/opt/airflow/pyspark/silver_transform.py '
            '{{ ds }} '
            '{{ execution_date.strftime("%H") }} '
            '{{ ti.xcom_pull(task_ids="bronze_load") }}'
        ),
    )

    @task
    def validate_counts(**context):
        logger.info("Validating count of records in the silver layer.")

        exec_date = context['execution_date']

        s3_prefix = (
                f"{S3_SILVER_PREFIX}/"
                f"year={exec_date.strftime('%Y')}/"
                f"month={exec_date.strftime('%m')}/"
                f"day={exec_date.strftime('%d')}/"
                f"hour={exec_date.strftime('%H')}/"
            )

        hook = S3Hook(aws_conn_id='aws_default')
        keys = hook.list_keys(
            bucket_name=S3_BUCKET,
            prefix=s3_prefix
        )

        if not keys:
            logger.error("No files found in silver layer for prefix: %s", s3_prefix)
            raise ValueError(f"No files found in silver layer for prefix: {s3_prefix}")
        else:
            logger.info("Found %d files in silver layer for prefix: %s", len(keys), s3_prefix)
            return len(keys)

    dbt_gold_models = BashOperator(
        task_id='dbt_gold_models',
        bash_command=(
        f'cd {DBT_PROJECT_DIR} && '
        f'dbt run '
        f'--project-dir {DBT_PROJECT_DIR} '
        f'--profiles-dir {DBT_PROFILES_DIR}'
        ),
    )
        
    # ── dependencies ──
    path = check_source_file()
    bronze_load(path) >> silver_pyspark >> validate_counts() >> dbt_gold_models

financial_txn_pipeline()