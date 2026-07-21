# Financial Transaction Data Pipeline
### End-to-End Lakehouse Architecture on AWS

![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazon-aws&logoColor=white)
![Redshift](https://img.shields.io/badge/Redshift-8C4FFF?style=flat&logo=amazon-redshift&logoColor=white)

---

## Business Problem

A retail bank processes millions of customer transactions every day. The data team needs a reliable, scalable pipeline that transforms raw messy transaction data into clean, tested, analytics-ready datasets — serving three business consumers with different latency requirements.

| Consumer | Need | Latency |
|---|---|---|
| Finance Team | Intraday revenue and volume reporting | Hourly |
| Risk Team | End-of-day suspicious account summary | Daily by 6 AM |
| Executives | KPI dashboard (Tableau) | Daily by 6 AM |

---

## Architecture Overview

```
Core Banking System  →  CSV files (every hour)
        ↓
  Airflow DAG  (orchestrates every step, retry=3, delay=5min)
        ↓
Bronze Layer  →  Raw CSV, immutable, source of truth         [S3]
        ↓
  PySpark  (schema validation, null handling, dedup,
            watermark logic, PII masking)
        ↓
Silver Layer  →  Cleaned, validated, enriched Parquet        [S3]
        ↓
  dbt  (staging models → fact + dimension tables, tests, docs)
        ↓
Gold Layer  →  Star schema, tested, documented               [Redshift]
        ↓
Finance  |  Risk Team  |  Executives (Tableau)
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | Apache Airflow | Scheduling, retry, alerting, task dependencies |
| Processing | PySpark | 5M+ records/day, distributed compute, scales 3x |
| Transformation | dbt | SQL modeling, quality tests, auto documentation |
| Storage | AWS S3 | Infinite scale, Bronze/Silver/DLQ layers |
| Warehouse | AWS Redshift Serverless | Pay-per-use, cost constraint, Gold layer |
| Query Engine | AWS Athena | Ad hoc queries on S3, dbt source |
| Catalog | AWS Glue Data Catalog | Schema registry for all layers |
| CI/CD | GitHub Actions | Automated tests on every PR, no manual deploys |
| Format | Parquet (SNAPPY) | Columnar, compressed, partition pruning |

---

## Pipeline Design

### Bronze Layer — Raw Ingestion
- CSV files landed in S3 **exactly as received**. No modifications. Ever.
- Immutable — source of truth and recovery point for everything downstream.
- Partitioned by `year/month/day/hour` for efficient querying.

```
s3://bucket/bronze/year=2024/month=01/day=15/hour=10/transactions.csv
```

### Silver Layer — Transformation & Enrichment

PySpark applies 5 steps in sequence:

**Step 1 — Schema Validation**
Enforce strict StructType schema. Wrong data types flagged immediately. Never trust `inferSchema` in production.

**Step 2 — Null Handling**
- Critical fields (`transaction_id`, `amount`) → drop record
- Non-critical fields (`branch_name`) → fill with `UNKNOWN`
- Timestamp fields (`received_time`) → null = can't calculate latency → route to DEAD_LETTER

**Step 3 — Deduplication**
Window function: `partitionBy(transaction_id).orderBy(received_time DESC)`. Keep `row_number = 1` only. Handles upstream retries sending same transaction twice.

**Step 4 — Watermark Logic**
Every record has two timestamps:
- `event_time` — when the transaction actually happened
- `received_time` — when it arrived in the system

```
latency = received_time - event_time

latency ≤ 30 min     →  NORMAL       (process in current run)
30 min < latency ≤ 2hr  →  LATE      (caught by next run lookback)
latency > 2 hours    →  DEAD_LETTER  (SLA missed, manual review)
```

> DLQ threshold = 2 hours because SLA = 2 hours. Beyond this the business deadline is already missed.

**Step 5 — PII Masking**
`customer_name` and `account_number` SHA-256 hashed for PIPEDA compliance. Masked key preserves join capability without exposing raw PII.

### Gold Layer — Business-Ready Models

dbt creates a star schema in Redshift Serverless:

**Fact Tables**
```
fct_transactions_hourly   →  finance team
fct_transactions_daily    →  risk team
fct_executive_kpi_daily   →  executives (Tableau)
```

**Dimension Tables**
```
dim_customers   →  customer_id, masked_name, region, risk_level
dim_branches    →  branch_id, branch_name, region, country
dim_date        →  full date spine
```

**dbt Quality Gates**
- `not_null` — all primary keys and critical columns
- `unique` — transaction_id across all fact tables
- `accepted_values` — transaction_type, record_status
- `relationships` — all foreign keys have matching primary keys
- `source freshness` — alert if Silver data is stale before dbt runs

---

## Airflow DAG

One DAG, runs every hour. Task-level retry — failures retry at individual task, not pipeline level.

```
check_source_file  →  bronze_load  →  silver_pyspark
→  validate_counts  →  dbt_gold_models  →  dbt_tests  →  notify
```

| Task | Purpose |
|---|---|
| `check_source_file` | Fail fast if CSV missing — SLA at risk |
| `bronze_load` | Move CSV to S3 Bronze partition |
| `silver_pyspark` | Run full PySpark transformation |
| `validate_counts` | Assert Silver record count > 0 |
| `dbt_gold_models` | Build all dbt fact + dimension models |
| `dbt_tests` | Run all dbt quality tests |
| `notify` | Slack alert — success or failure |

> **Fail fast principle:** If source file doesn't arrive, pipeline fails immediately and alerts. Given 2 hour SLA, a missing file is already a risk. Waiting makes it worse.

---

## Idempotency

Running the pipeline 10 times produces the same result as running it once.

| Layer | Implementation |
|---|---|
| PySpark | `write.mode("overwrite").partitionBy("transaction_hour")` — re-run overwrites only that hour |
| dbt | Incremental model with `unique_key="transaction_id"` — upsert not insert |
| Airflow | Each DAG run scoped to its own time window — never touches other partitions |

---

## Late Data Strategy

```
Watermark        =  30 minutes
SLA              =  2 hours
Lookback window  =  48 hours
DLQ threshold    =  2 hours
```

- **NORMAL** records processed in current run
- **LATE** records caught by next run's 48-hour lookback window
- **DEAD_LETTER** records written to separate S3 path with full metadata for investigation

---

## Performance Decisions

**Broadcast Join** — dimension table lookups use `broadcast()`. Small tables copied to every machine. Zero network shuffling.

**Parquet + SNAPPY** — columnar format means Athena only reads columns needed. Significant cost and speed improvement over CSV.

**Partition pruning** — all layers partitioned by time. Time-bounded queries never scan full dataset.

---

## CI/CD

GitHub Actions runs on every pull request:

```
Push → Run dbt tests → Run PySpark unit tests → Code quality check
→ All pass: merge allowed  |  Any fail: merge blocked
→ Merge to main: auto deploy
```

---

## Cloud Provider Mapping

This pipeline runs on AWS. Equivalent services on other clouds:

| AWS | Azure | GCP |
|---|---|---|
| S3 | ADLS Gen2 | Cloud Storage |
| Redshift | Synapse Analytics | BigQuery |
| Athena | Synapse SQL | BigQuery |
| Glue Catalog | Purview | Dataplex |
| IAM | Azure AD | GCP IAM |

---

## Project Structure

```
financial-transaction-pipeline/
├── dags/
│   └── transaction_pipeline_dag.py
├── pyspark/
│   ├── bronze_load.py
│   ├── silver_transform.py
│   └── schemas.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
├── synthetic_data/
│   └── generate_transactions.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── DESIGN.md
└── README.md
```

---

## Key Engineering Decisions

- **Bronze immutability** — full reprocessability if Silver has a bug
- **Watermark + lookback** — catches 99%+ of late records without impacting SLA
- **dbt tests as quality gates** — Gold only updates if all tests pass
- **Broadcast joins** — eliminate network shuffling for small reference tables
- **Partition overwrite** — Airflow retries are safe and predictable
- **PII masking in Silver** — compliance before data reaches any analyst
- **Serverless architecture** — cost proportional to usage

---

*Jay Mehta — Data Engineer — Kitchener, ON — mjay2911@gmail.com*
