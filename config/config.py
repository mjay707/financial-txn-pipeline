from datetime import timedelta
from datetime import datetime

# ── Pipeline Schedule ──────────────────────────────
DAG_OWNER = "jay"
DAG_RETRIES = 3
DAG_RETRY_DELAY = timedelta(minutes=5)
DAG_START_DATE = datetime(2024, 1, 15)
DAG_SCHEDULE = "@hourly"
DAG_CATCHUP = False

# ── Watermark & SLA ────────────────────────────────
WATERMARK_MINUTES = 30
SLA_HOURS = 2
DLQ_THRESHOLD_HOURS = 2
LOOKBACK_HOURS = 2

# ── S3 Settings ────────────────────────────────────
S3_BUCKET = "financial-txn-pipeline"
S3_BRONZE_PREFIX = "bronze"
S3_SILVER_PREFIX = "silver"
S3_DLQ_PREFIX = "dlq"

# ── Source Settings ────────────────────────────────
SOURCE_FOLDER = "/opt/airflow/synthetic_data/raw"
FILE_NAME_PATTERN = "transactions_{date}_{hour}.csv"

# ── AWS Settings ───────────────────────────────────
AWS_REGION = "ca-central-1"
REDSHIFT_DATABASE = "financial_txn"
REDSHIFT_SCHEMA = "public"

# ── dbt Settings ───────────────────────────────────
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"