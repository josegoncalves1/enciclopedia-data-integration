# 2️⃣-3️⃣ ETL/ELT PIPELINES & DATA WAREHOUSING

---

# 2️⃣ ETL/ELT PIPELINES

## 📦 EXEMPLO 1: Apache Airflow - Complete DAG

```python
# File: dags/ecommerce_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging

logger = logging.getLogger(__name__)

# Default args
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# DAG
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'etl', 'production'],
)

# ============================================
# EXTRACT TASKS
# ============================================

def extract_from_api():
    """Extract orders from API"""
    url = 'https://api.example.com/orders'
    params = {
        'start_date': '{{ ds }}',
        'end_date': '{{ tomorrow_ds }}',
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    df = pd.DataFrame(data['orders'])
    filepath = f"/tmp/orders_{{{ 'ds' }}}}.parquet"
    df.to_parquet(filepath, index=False)
    
    logger.info(f"✓ Extracted {len(df)} orders")
    return filepath

def extract_from_db():
    """Extract products from database"""
    from sqlalchemy import create_engine
    
    engine = create_engine('postgresql://user:pass@localhost/source_db')
    df = pd.read_sql('SELECT * FROM products', engine)
    
    filepath = f"/tmp/products_{{{ 'ds' }}}.parquet"
    df.to_parquet(filepath, index=False)
    
    logger.info(f"✓ Extracted {len(df)} products")
    return filepath

# ============================================
# TRANSFORM TASKS
# ============================================

def transform_orders(**context):
    """Clean and transform orders"""
    ti = context['task_instance']
    orders_path = ti.xcom_pull(task_ids='extract_orders')
    products_path = ti.xcom_pull(task_ids='extract_products')
    
    # Load data
    orders_df = pd.read_parquet(orders_path)
    products_df = pd.read_parquet(products_path)
    
    # Clean orders
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    orders_df['amount'] = pd.to_numeric(orders_df['amount'], errors='coerce')
    orders_df = orders_df.dropna(subset=['amount'])
    
    # Join with products
    merged = orders_df.merge(
        products_df[['product_id', 'category']],
        on='product_id',
        how='left'
    )
    
    # Add calculations
    merged['revenue'] = merged['amount'] * (1 + merged.get('tax_rate', 0))
    merged['processed_at'] = datetime.now()
    
    # Save
    output_path = f"/tmp/transformed_orders_{{{ 'ds' }}}.parquet"
    merged.to_parquet(output_path, index=False)
    
    logger.info(f"✓ Transformed {len(merged)} orders")
    return output_path

# ============================================
# LOAD TASKS
# ============================================

def load_to_snowflake(**context):
    """Load data to Snowflake"""
    from sqlalchemy import create_engine
    
    ti = context['task_instance']
    filepath = ti.xcom_pull(task_ids='transform_orders')
    
    df = pd.read_parquet(filepath)
    
    # Connect to Snowflake
    engine = create_engine(
        'snowflake://user:password@account/database/schema'
    )
    
    # Load data
    df.to_sql(
        'raw_orders',
        engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    
    logger.info(f"✓ Loaded {len(df)} rows to Snowflake")

def validate_data(**context):
    """Validate loaded data"""
    from sqlalchemy import create_engine, text
    
    engine = create_engine(
        'snowflake://user:password@account/database/schema'
    )
    
    # Run validations
    with engine.connect() as conn:
        # Check row count
        result = conn.execute(
            text("SELECT COUNT(*) as cnt FROM raw_orders WHERE DATE(processed_at) = :date"),
            {'date': context['ds']}
        )
        count = result.scalar()
        
        if count < 100:
            raise ValueError(f"Expected >100 rows, got {count}")
        
        logger.info(f"✓ Data validation passed ({count} rows)")

# ============================================
# DEFINE TASKS
# ============================================

extract_orders = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_from_api,
    dag=dag,
)

extract_products = PythonOperator(
    task_id='extract_products',
    python_callable=extract_from_db,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_orders',
    python_callable=transform_orders,
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

generate_report = BashOperator(
    task_id='generate_report',
    bash_command='echo "Pipeline completed successfully" | mail -s "ETL Report" alerts@example.com',
    dag=dag,
)

# ============================================
# DEPENDENCIES
# ============================================

[extract_orders, extract_products] >> transform >> load >> validate >> generate_report
```

---

## 📦 EXEMPLO 2: dbt - SQL Transformations

```yaml
# File: dbt_project.yml

name: 'ecommerce_dbt'
version: '1.0.0'
config-version: 2
profile: 'snowflake'

vars:
  start_date: '2024-01-01'
  end_date: '{{ var("start_date") }}'

models:
  ecommerce_dbt:
    staging:
      materialized: view
    marts:
      materialized: table
      tags: ['critical']

seeds:
  ecommerce_dbt:
    database: analytics
    schema: seeds

snapshots:
  ecommerce_dbt:
    target_schema: snapshots
```

```sql
-- File: models/staging/stg_orders.sql

{{ config(
    materialized='table',
    tags=['daily'],
    meta={
        'owner': 'analytics',
        'description': 'Cleaned order data'
    }
) }}

WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        amount,
        tax_rate,
        status,
        _extracted_at
    FROM {{ source('raw', 'orders') }}
    WHERE order_date >= '{{ var("start_date") }}'
),

cleaned_data AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) as order_date,
        CAST(amount AS DECIMAL(10, 2)) as amount,
        COALESCE(tax_rate, 0) as tax_rate,
        LOWER(TRIM(status)) as status,
        _extracted_at
    FROM source_data
    WHERE amount > 0
        AND status IN ('completed', 'pending', 'shipped')
)

SELECT * FROM cleaned_data
```

```sql
-- File: models/marts/fct_orders.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

stg_products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

joined AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        c.customer_name,
        c.segment,
        p.product_id,
        p.category,
        o.amount,
        o.tax_rate,
        ROUND(o.amount * (1 + o.tax_rate), 2) as revenue,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC) as customer_order_seq
    FROM stg_orders o
    LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
    LEFT JOIN stg_products p ON o.product_id = p.product_id
)

SELECT
    *,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM joined

{% if execute %}
    WHERE order_date >= (
        SELECT COALESCE(MAX(order_date), '1900-01-01') FROM {{ this }}
    )
{% endif %}
```

```yaml
-- File: models/staging/schema.yml

version: 2

sources:
  - name: raw
    description: Raw data from source systems
    tables:
      - name: orders
        description: Order transactions
        columns:
          - name: order_id
            description: Unique order identifier
            tests:
              - unique
              - not_null
          - name: customer_id
            tests:
              - not_null
          - name: amount
            tests:
              - not_null
              - dbt_utils.accepted_values:
                  values: ['positive']

models:
  - name: stg_orders
    description: Staged orders (cleaned, deduplicated)
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: source('raw', 'orders')
          tolerance_pct: 0.01
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: amount
        tests:
          - not_null
          - dbt_utils.accepted_values:
              values: ['positive']
```

---

## 📦 EXEMPLO 3: Python ETL (Pandas/Polars)

```python
# File: etl/python_etl.py

import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, source_db, target_db):
        self.source_engine = create_engine(source_db)
        self.target_engine = create_engine(target_db)
    
    def extract(self, query, date_filter=None):
        """Extract data from source"""
        if date_filter:
            query = query.format(date=date_filter)
        
        logger.info(f"Extracting: {query[:100]}...")
        df = pd.read_sql(query, self.source_engine)
        logger.info(f"✓ Extracted {len(df)} rows")
        return df
    
    def transform_orders(self, df):
        """Transform order data"""
        # Convert to Polars for performance
        df_pl = pl.from_pandas(df)
        
        transformed = df_pl.select([
            pl.col('order_id'),
            pl.col('customer_id'),
            pl.col('order_date').cast(pl.Date),
            pl.col('amount').cast(pl.Float64),
            pl.col('tax_rate').fill_null(0),
            pl.col('status').str.to_lowercase(),
            (pl.col('amount') * (1 + pl.col('tax_rate'))).alias('revenue'),
            pl.col('_extracted_at')
        ]).filter(
            pl.col('amount') > 0
        )
        
        logger.info(f"✓ Transformed {len(transformed)} rows")
        return transformed.to_pandas()
    
    def load(self, df, table_name, if_exists='append'):
        """Load data to target"""
        df.to_sql(
            table_name,
            self.target_engine,
            if_exists=if_exists,
            index=False,
            chunksize=5000,
            method='multi'
        )
        logger.info(f"✓ Loaded {len(df)} rows to {table_name}")
    
    def run_pipeline(self, run_date=None):
        """Run complete pipeline"""
        if run_date is None:
            run_date = (datetime.now() - timedelta(days=1)).date()
        
        # Extract
        df = self.extract(
            f"SELECT * FROM orders WHERE DATE(order_date) = '{run_date}'",
            run_date
        )
        
        # Transform
        df_transformed = self.transform_orders(df)
        
        # Load
        self.load(df_transformed, 'staging_orders', if_exists='append')
        
        return df_transformed

# Usage
if __name__ == "__main__":
    pipeline = ETLPipeline(
        source_db='postgresql://source_user:pass@source_host/source_db',
        target_db='postgresql://target_user:pass@target_host/target_db'
    )
    
    pipeline.run_pipeline(run_date='2024-01-15')
```

---

## 📦 EXEMPLO 4: Luigi Task Pipeline

```python
# File: luigi_tasks.py

import luigi
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtractOrdersTask(luigi.Task):
    """Extract orders from API"""
    run_date = luigi.DateParameter()
    
    def output(self):
        return luigi.LocalTarget(f'data/orders_{self.run_date}.parquet')
    
    def run(self):
        import requests
        
        response = requests.get(
            'https://api.example.com/orders',
            params={'date': self.run_date}
        )
        
        df = pd.DataFrame(response.json())
        df.to_parquet(self.output().path, index=False)
        logger.info(f"✓ Extracted {len(df)} orders")

class ExtractProductsTask(luigi.Task):
    """Extract products from database"""
    run_date = luigi.DateParameter()
    
    def output(self):
        return luigi.LocalTarget(f'data/products_{self.run_date}.parquet')
    
    def run(self):
        engine = create_engine('postgresql://user:pass@localhost/db')
        df = pd.read_sql('SELECT * FROM products', engine)
        df.to_parquet(self.output().path, index=False)

class TransformOrdersTask(luigi.Task):
    """Transform orders"""
    run_date = luigi.DateParameter()
    
    def requires(self):
        return [
            ExtractOrdersTask(self.run_date),
            ExtractProductsTask(self.run_date)
        ]
    
    def output(self):
        return luigi.LocalTarget(f'data/transformed_orders_{self.run_date}.parquet')
    
    def run(self):
        orders_df = pd.read_parquet('data/orders_' + str(self.run_date) + '.parquet')
        products_df = pd.read_parquet('data/products_' + str(self.run_date) + '.parquet')
        
        merged = orders_df.merge(products_df, on='product_id', how='left')
        merged['revenue'] = merged['amount'] * (1 + merged['tax_rate'].fillna(0))
        
        merged.to_parquet(self.output().path, index=False)
        logger.info(f"✓ Transformed {len(merged)} orders")

class LoadToDataWarehouse(luigi.Task):
    """Load to data warehouse"""
    run_date = luigi.DateParameter()
    
    def requires(self):
        return TransformOrdersTask(self.run_date)
    
    def output(self):
        return luigi.LocalTarget(f'data/load_complete_{self.run_date}.txt')
    
    def run(self):
        from sqlalchemy import create_engine
        
        df = pd.read_parquet(f'data/transformed_orders_{self.run_date}.parquet')
        engine = create_engine('snowflake://user:pass@account/db/schema')
        
        df.to_sql('staging_orders', engine, if_exists='append', index=False, chunksize=5000)
        
        with open(self.output().path, 'w') as f:
            f.write('Load complete')
        
        logger.info(f"✓ Loaded {len(df)} rows to data warehouse")

if __name__ == '__main__':
    luigi.build([LoadToDataWarehouse(run_date='2024-01-15')], local_scheduler=True)
```

---

# 3️⃣ DATA WAREHOUSING

## 📦 EXEMPLO 5: Snowflake Setup & Optimization

```sql
-- File: snowflake_setup.sql

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
WAREHOUSE_SIZE = 'LARGE'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 10
SCALING_POLICY = 'STANDARD';

-- Create database
CREATE DATABASE IF NOT EXISTS ANALYTICS;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ANALYTICS.RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MARTS;

-- Create file format for Parquet
CREATE FILE FORMAT IF NOT EXISTS ANALYTICS.RAW.PARQUET_FORMAT
TYPE = 'PARQUET';

-- Create stage for S3
CREATE STAGE IF NOT EXISTS ANALYTICS.RAW.S3_STAGE
URL = 's3://my-bucket/data/'
CREDENTIALS = (AWS_KEY_ID = 'xxxxx' AWS_SECRET_KEY = 'xxxxx');

-- Create tables with clustering
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.ORDERS (
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2),
    tax_rate DECIMAL(5, 4),
    status VARCHAR(50),
    _extracted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT orders_pk PRIMARY KEY (order_id)
)
CLUSTER BY (order_date, customer_id)
DATA_RETENTION_TIME_IN_DAYS = 30;

-- Create transient table (no fail-safe, cheaper)
CREATE TRANSIENT TABLE ANALYTICS.STAGING.STAG_ORDERS AS
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    COALESCE(tax_rate, 0) as tax_rate,
    LOWER(TRIM(status)) as status,
    _extracted_at
FROM ANALYTICS.RAW.ORDERS
WHERE order_date >= CURRENT_DATE - 30;

-- Create dynamic table (auto-refresh)
CREATE DYNAMIC TABLE ANALYTICS.MARTS.DAILY_ORDERS
TARGET LAG = '1 hour'
WAREHOUSE = ANALYTICS_WH
AS
SELECT
    DATE_TRUNC('day', order_date) as order_day,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM ANALYTICS.STAGING.STAG_ORDERS
GROUP BY DATE_TRUNC('day', order_date);

-- Create view
CREATE VIEW ANALYTICS.MARTS.V_ORDERS_SUMMARY AS
SELECT
    order_id,
    customer_id,
    order_date,
    amount * (1 + tax_rate) as revenue,
    status,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as order_seq
FROM ANALYTICS.STAGING.STAG_ORDERS;

-- Create materialized view for performance
CREATE MATERIALIZED VIEW ANALYTICS.MARTS.MV_CUSTOMER_METRICS AS
SELECT
    customer_id,
    COUNT(DISTINCT order_id) as lifetime_orders,
    SUM(amount * (1 + tax_rate)) as lifetime_revenue,
    MAX(order_date) as last_order_date,
    MIN(order_date) as first_order_date
FROM ANALYTICS.STAGING.STAG_ORDERS
GROUP BY customer_id;

-- Grant permissions
GRANT USAGE ON DATABASE ANALYTICS TO ROLE ANALYST;
GRANT USAGE ON SCHEMA ANALYTICS.MARTS TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.MARTS TO ROLE ANALYST;

-- Create procedure for data loading
CREATE OR REPLACE PROCEDURE LOAD_ORDERS_FROM_S3(date_to_load STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'load_data'
AS
$$
def load_data(session, date_to_load):
    try:
        # Load from S3
        df = session.read.parquet(f"@S3_STAGE/orders/{date_to_load}/*.parquet")
        
        # Write to Snowflake
        df.write.mode("append").save_as_table("RAW.ORDERS")
        
        return f"Successfully loaded data for {date_to_load}"
    except Exception as e:
        return f"Error: {str(e)}"
$$;

-- Execute procedure
CALL LOAD_ORDERS_FROM_S3('2024-01-15');

-- Query optimization
-- Add query history
SELECT
    query_id,
    query_text,
    execution_time,
    bytes_scanned,
    rows_produced
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_time > 10000  -- > 10 seconds
ORDER BY execution_time DESC
LIMIT 10;

-- Find expensive queries
SELECT
    query_id,
    query_text,
    partitions_scanned,
    partitions_total
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE partitions_scanned > partitions_total * 0.5
AND creation_time > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY creation_time DESC;
```

---

## 📦 EXEMPLO 6: Google BigQuery Setup

```python
# File: bigquery_setup.py

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

class BigQueryWarehouse:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
    
    def create_dataset(self, dataset_id, description=""):
        """Create dataset"""
        dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        dataset.location = "US"
        dataset.description = description
        
        try:
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"✓ Created dataset {dataset_id}")
        except Exception as e:
            print(f"Dataset already exists: {e}")
    
    def create_table(self, dataset_id, table_id, schema):
        """Create table with schema"""
        table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="order_date",
        )
        table.clustering_fields = ["customer_id", "order_date"]
        
        table = self.client.create_table(table)
        print(f"✓ Created table {table_id}")
        return table
    
    def load_from_gcs(self, gcs_uri, dataset_id, table_id, schema):
        """Load data from GCS"""
        table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.autodetect = False
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        load_job = self.client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config,
            location="US",
        )
        
        load_job.result()
        print(f"✓ Loaded {load_job.output_rows} rows to {table_id}")
    
    def create_view(self, dataset_id, view_id, query):
        """Create view"""
        view_id = f"{self.project_id}.{dataset_id}.{view_id}"
        
        view = bigquery.Table(view_id)
        view.view_query = query
        
        view = self.client.create_table(view)
        print(f"✓ Created view {view_id}")
    
    def query(self, sql):
        """Execute query"""
        job = self.client.query(sql)
        return job.result().to_dataframe()
    
    def get_table_stats(self, dataset_id, table_id):
        """Get table statistics"""
        table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_id)
        
        return {
            'rows': table.num_rows,
            'bytes': table.num_bytes,
            'created': table.created,
            'modified': table.modified,
            'schema': table.schema,
        }

# Usage
if __name__ == "__main__":
    bq = BigQueryWarehouse('my-project')
    
    # Create datasets
    bq.create_dataset('raw', 'Raw data from sources')
    bq.create_dataset('staging', 'Staged data')
    bq.create_dataset('marts', 'Analytics tables')
    
    # Create schema
    schema = [
        bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
    ]
    
    # Create table
    bq.create_table('raw', 'orders', schema)
    
    # Load from GCS
    bq.load_from_gcs(
        'gs://my-bucket/orders/*.parquet',
        'raw',
        'orders',
        schema
    )
    
    # Create view
    bq.create_view(
        'marts',
        'orders_by_day',
        """
        SELECT
            DATE_TRUNC(order_date, DAY) as day,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM `my-project.raw.orders`
        GROUP BY DATE_TRUNC(order_date, DAY)
        """
    )
```

---

## 📦 EXEMPLO 7: AWS Redshift Setup

```sql
-- File: redshift_setup.sql

-- Create cluster
CREATE CLUSTER my-analytics-cluster
NODE TYPE: dc2.large
NUMBER OF NODES: 3
MASTER USERNAME: admin
MASTER PASSWORD: *** (strong password)
DATABASE NAME: analytics
PORT: 5439;

-- Connect and create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create table with distribution and sort keys
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2),
    tax_rate DECIMAL(5, 4),
    status VARCHAR(50),
    _extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
DISTKEY(customer_id)  -- Distribute by customer
SORTKEY(order_date);  -- Sort by date

-- Create table with EVEN distribution
CREATE TABLE IF NOT EXISTS raw.products (
    product_id BIGINT NOT NULL,
    name VARCHAR(500),
    category VARCHAR(100),
    price DECIMAL(10, 2)
)
DISTSTYLE EVEN;

-- Create external table (query from S3)
CREATE EXTERNAL TABLE staging.orders_external (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    amount DECIMAL(10, 2)
)
STORED AS PARQUET
LOCATION 's3://my-bucket/orders/';

-- Copy from S3
COPY raw.orders (order_id, customer_id, order_date, amount, tax_rate, status)
FROM 's3://my-bucket/orders/2024-01-15/*.parquet'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- Create view
CREATE VIEW marts.v_daily_revenue AS
SELECT
    DATE_TRUNC('day', order_date) as day,
    COUNT(*) as order_count,
    SUM(amount * (1 + COALESCE(tax_rate, 0))) as revenue
FROM raw.orders
GROUP BY DATE_TRUNC('day', order_date);

-- Performance: Analyze tables
ANALYZE raw.orders;
ANALYZE raw.products;

-- Check distribution skew
SELECT
    slice,
    col,
    num_values,
    minval,
    maxval,
    avg_width
FROM stv_blocklist
ORDER BY slice, col
LIMIT 10;

-- Create materialized view (Manual - no auto-refresh)
CREATE TABLE marts.customer_metrics AS
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount * (1 + COALESCE(tax_rate, 0))) as lifetime_value,
    MAX(order_date) as last_order_date,
    MIN(order_date) as first_order_date
FROM raw.orders
GROUP BY customer_id;

-- Grant permissions
GRANT USAGE ON SCHEMA marts TO GROUP analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO GROUP analysts;
```

---

## RESUMO CATEGORIAS 2-3

✅ **ETL/ELT:**
- Airflow complete DAGs
- dbt SQL transformations
- Python ETL (Pandas/Polars)
- Luigi task pipelines
- Error handling & monitoring
- Incremental loading
- Data validation

✅ **Data Warehousing:**
- Snowflake setup & optimization
- BigQuery setup & loading
- Redshift setup & queries
- Schema design (Star, Snowflake)
- Partitioning & clustering
- Performance optimization
- Cost optimization

Próximas: Data Lakes, BI, Real-time, Python/SQL, Cloud, Quality, ML
