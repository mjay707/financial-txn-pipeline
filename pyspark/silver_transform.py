import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    unix_timestamp,
    sha2,
    concat_ws,
    lit,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from config.config import (
    S3_BUCKET,
    S3_SILVER_PREFIX,
    S3_DLQ_PREFIX,
    WATERMARK_MINUTES,
    DLQ_THRESHOLD_HOURS,
    LOOKBACK_HOURS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read command line arguments
if len(sys.argv) != 4:
    logger.error("Usage: silver_transform.py <date> <hour> <s3_bronze_key>")
    sys.exit(1)

execution_date = sys.argv[1]
hour           = sys.argv[2]
s3_bronze_key  = sys.argv[3]

logger.info("Starting Silver transformation for date=%s hour=%s", execution_date, hour)

spark = SparkSession \
    .builder \
    .appName("financial_txn_silver_transform") \
    .getOrCreate() 

schema = StructType(
    [
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("branch_id", IntegerType(), False),
        StructField("region", StringType(), True),
        StructField("amount", DoubleType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("received_time", TimestampType(), False),
    ]
)

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(f"s3a://{S3_BUCKET}/{s3_bronze_key}")

logger.info("Read %d records from Bronze: s3a://%s/%s", df.count(), S3_BUCKET, s3_bronze_key)

before_count = df.count()
df = df.dropna(subset=["transaction_id", "amount", "event_time", "received_time"])
after_count = df.count()

logger.info(
    "Null handling complete. Dropped %d records. Remaining: %d",
    before_count - after_count,
    after_count
)

df = df.fillna({"customer_name": "UNKNOWN", "region": "UNKNOWN", "transaction_type": "UNKNOWN"})
logger.info("Filled null values. Remaining records: %d", df.count())

dup_records = Window.partitionBy("transaction_id"). \
    orderBy(col("received_time").desc())

df = df.withColumn("row_num", row_number().over(dup_records)) \
    .filter(col("row_num") == 1) \
    .drop("row_num") \

logger.info("Deduplication complete. Records after dedup: %d", df.count())

df = df.withColumn(
    "latency_seconds",
    unix_timestamp(col("received_time")) - unix_timestamp(col("event_time"))
    ).withColumn(
        "record_status",
        when(col("latency_seconds") <= WATERMARK_MINUTES * 60, "NORMAL")
        .when(col("latency_seconds") <= DLQ_THRESHOLD_HOURS * 3600, "LATE")
        .otherwise("DEAD_LETTER")
)

logger.info(
    "Watermark logic applied. NORMAL: %d, LATE: %d, DEAD_LETTER: %d",
    df.filter(col("record_status") == "NORMAL").count(),
    df.filter(col("record_status") == "LATE").count(),
    df.filter(col("record_status") == "DEAD_LETTER").count()
)

df = df.withColumn("customer_name",sha2(col("customer_name"), 256)) \
    .withColumn("account_number",sha2(col("account_number"), 256))

# Silver — NORMAL and LATE records
df_silver = df.filter(col("record_status") != "DEAD_LETTER")

# DLQ — only DEAD_LETTER records
df_dlq = df.filter(col("record_status") == "DEAD_LETTER")

silver_path = (
    f"s3a://{S3_BUCKET}/{S3_SILVER_PREFIX}/"
    f"year={execution_date[:4]}/"
    f"month={execution_date[5:7]}/"
    f"day={execution_date[8:10]}/"
    f"hour={hour}/"
)

df_silver.write \
    .mode("overwrite") \
    .parquet(silver_path)

logger.info("Silver layer written to: %s", silver_path)

dlq_path = (
    f"s3a://{S3_BUCKET}/{S3_DLQ_PREFIX}/"
    f"year={execution_date[:4]}/"
    f"month={execution_date[5:7]}/"
    f"day={execution_date[8:10]}/"
    f"hour={hour}/"
)

df_dlq.write \
    .mode("overwrite") \
    .parquet(dlq_path)

logger.info("DLQ layer written to: %s", dlq_path)

logger.info("Silver transformation complete.")
spark.stop()