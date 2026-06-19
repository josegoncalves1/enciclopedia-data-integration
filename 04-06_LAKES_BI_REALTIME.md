# 4️⃣-6️⃣ DATA LAKES, BI & REAL-TIME STREAMING

---

# 4️⃣ DATA LAKES

## 📦 EXEMPLO 1: AWS S3 + Glue Data Catalog

```python
# File: aws_data_lake_setup.py

import boto3
import json
from datetime import datetime

class DataLakeSetup:
    def __init__(self, bucket_name, region='us-east-1'):
        self.s3 = boto3.client('s3', region_name=region)
        self.glue = boto3.client('glue', region_name=region)
        self.bucket = bucket_name
        self.region = region
    
    def create_bucket_structure(self):
        """Create S3 bucket with proper structure"""
        # Create bucket
        try:
            self.s3.create_bucket(Bucket=self.bucket)
            print(f"✓ Created bucket {self.bucket}")
        except Exception as e:
            print(f"Bucket exists: {e}")
        
        # Enable versioning
        self.s3.put_bucket_versioning(
            Bucket=self.bucket,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Enable encryption
        self.s3.put_bucket_encryption(
            Bucket=self.bucket,
            ServerSideEncryptionConfiguration={
                'Rules': [{
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }]
            }
        )
        
        # Create folder structure
        folders = [
            'raw/',
            'staging/',
            'processed/',
            'archive/',
            'logs/'
        ]
        
        for folder in folders:
            self.s3.put_object(Bucket=self.bucket, Key=folder)
            print(f"✓ Created folder {folder}")
    
    def create_glue_database(self, database_name):
        """Create Glue database"""
        try:
            self.glue.create_database(
                CatalogId='123456789012',  # Your account ID
                DatabaseInput={
                    'Name': database_name,
                    'Description': f'{database_name} database',
                }
            )
            print(f"✓ Created Glue database {database_name}")
        except Exception as e:
            print(f"Database exists: {e}")
    
    def create_glue_crawler(self, crawler_name, s3_path, database_name):
        """Create Glue crawler to catalog data"""
        try:
            self.glue.create_crawler(
                Name=crawler_name,
                Role=f'arn:aws:iam::123456789012:role/AWSGlueServiceRole',
                DatabaseName=database_name,
                Description=f'Crawler for {s3_path}',
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                S3Target={
                    'Path': f's3://{self.bucket}{s3_path}',
                    'Exclusions': ['_temporary', '_metadata'],
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                }
            )
            print(f"✓ Created crawler {crawler_name}")
        except Exception as e:
            print(f"Crawler exists: {e}")
    
    def start_crawler(self, crawler_name):
        """Start crawler execution"""
        self.glue.start_crawler(Name=crawler_name)
        print(f"✓ Started crawler {crawler_name}")

# Usage
if __name__ == "__main__":
    lake = DataLakeSetup('my-data-lake-bucket')
    
    # Setup
    lake.create_bucket_structure()
    lake.create_glue_database('analytics')
    lake.create_glue_crawler('raw-crawler', '/raw/', 'analytics')
    lake.start_crawler('raw-crawler')
```

---

## 📦 EXEMPLO 2: Apache Iceberg Format

```python
# File: iceberg_setup.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
import pandas as pd

# Initialize Spark with Iceberg
spark = SparkSession.builder \
    .appName("IcebergDataLake") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3://my-bucket/warehouse") \
    .getOrCreate()

# Define schema
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), False),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])

# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.raw.orders (
        order_id INT NOT NULL,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        amount DOUBLE,
        status STRING
    )
    USING iceberg
    PARTITIONED BY (order_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.delete.format.default' = 'parquet',
        'write.merge.cardinality' = 'hash',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

# Append data
df_orders = spark.createDataFrame(
    [
        (1, 100, "2024-01-15", 250.00, "completed"),
        (2, 101, "2024-01-15", 175.50, "pending"),
    ],
    orders_schema
)

df_orders.write.format("iceberg").mode("append").save("local.raw.orders")

# Iceberg-specific operations

# Time travel (read as of specific timestamp)
df_historical = spark.sql("""
    SELECT * FROM local.raw.orders
    VERSION AS OF 3
""")

# Snapshot history
spark.sql("SELECT * FROM local.raw.orders.history")

# Metadata files
spark.sql("SELECT * FROM local.raw.orders.snapshots")

# Update specific records
spark.sql("""
    UPDATE local.raw.orders
    SET status = 'shipped'
    WHERE order_id = 1
""")

# Merge operation (UPSERT)
spark.sql("""
    MERGE INTO local.raw.orders t
    USING (SELECT 1 as order_id, 100 as customer_id, '2024-01-15' as order_date, 300.00 as amount, 'updated' as status) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET amount = s.amount, status = s.status
    WHEN NOT MATCHED THEN INSERT *
""")

# Compaction (optimize layout)
spark.sql("""
    OPTIMIZE local.raw.orders
    REWRITE DATA USING BIN_PACK
""")

print("✓ Iceberg table created and operations completed")
```

---

## 📦 EXEMPLO 3: Delta Lake Setup

```python
# File: delta_lake_setup.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder \
    .appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create Delta table
df_orders = spark.createDataFrame(
    [
        (1, 100, "2024-01-15", 250.00),
        (2, 101, "2024-01-15", 175.50),
    ],
    ["order_id", "customer_id", "order_date", "amount"]
)

df_orders.write.format("delta").mode("overwrite").partitionBy("order_date").save("s3://my-bucket/delta/orders")

# Read Delta table
df = spark.read.format("delta").load("s3://my-bucket/delta/orders")
df.show()

# Delta-specific operations

# Time travel
df_historical = spark.read.format("delta").option("versionAsOf", 0).load("s3://my-bucket/delta/orders")

# View table history
spark.sql("""
    SELECT * FROM delta.`s3://my-bucket/delta/orders`
    VERSION AS OF 0
""")

# Upsert (Merge)
new_orders = spark.createDataFrame(
    [(1, 100, "2024-01-15", 300.00)],
    ["order_id", "customer_id", "order_date", "amount"]
)

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://my-bucket/delta/orders")

delta_table.alias("orders") \
    .merge(
        new_orders.alias("updates"),
        "orders.order_id = updates.order_id"
    ) \
    .whenMatchedUpdate(set={"amount": col("updates.amount")}) \
    .whenNotMatchedInsert(values={
        "order_id": col("updates.order_id"),
        "customer_id": col("updates.customer_id"),
        "order_date": col("updates.order_date"),
        "amount": col("updates.amount")
    }) \
    .execute()

# Optimize & Z-order
spark.sql("OPTIMIZE s3://my-bucket/delta/orders ZORDER BY order_date")

# Data quality checks with Delta
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://my-bucket/delta/orders")

# Add constraint
delta_table.sql("""
    ALTER TABLE delta.`s3://my-bucket/delta/orders`
    ADD CONSTRAINT order_amount_positive CHECK (amount > 0)
""")

print("✓ Delta Lake table created with ACID transactions")
```

---

# 5️⃣ BI & DASHBOARDING

## 📦 EXEMPLO 4: Power BI DAX Measures

```dax
// File: PowerBI_Measures.dax

// Revenue measures
Total Revenue = 
    SUMX(
        Orders,
        Orders[Amount] * (1 + Orders[Tax_Rate])
    )

Revenue YTD = 
    CALCULATE(
        [Total Revenue],
        DATESYTD(Calendar[Date])
    )

Revenue MoM Growth % = 
    VAR CurrentMonth = [Total Revenue]
    VAR PriorMonth = CALCULATE(
        [Total Revenue],
        PREVIOUSMONTH(Calendar[Date])
    )
    RETURN IF(PriorMonth = 0, 0, (CurrentMonth - PriorMonth) / PriorMonth)

// Customer metrics
Customer Count = DISTINCTCOUNT(Customers[Customer_ID])

Average Order Value = 
    DIVIDE(
        [Total Revenue],
        DISTINCTCOUNT(Orders[Order_ID])
    )

Customer Lifetime Value = 
    SUMX(
        VALUES(Customers[Customer_ID]),
        CALCULATE(
            [Total Revenue],
            Orders[Customer_ID] = Customers[Customer_ID]
        )
    )

// Advanced calculations
Running Total = 
    CALCULATE(
        [Total Revenue],
        FILTER(
            ALL(Calendar[Date]),
            Calendar[Date] <= MAX(Calendar[Date])
        )
    )

Rank by Revenue = 
    RANKX(
        ALL(Products[Category]),
        [Total Revenue]
    )

// Conditional formatting
Performance Status = 
    IF(
        [Revenue MoM Growth %] > 0.10,
        "Excellent",
        IF(
            [Revenue MoM Growth %] > 0,
            "Good",
            "Needs Attention"
        )
    )
```

---

## 📦 EXEMPLO 5: Looker Dashboard (LookML)

```looker
# File: views/orders.view

view: orders {
  sql_table_name: public.orders ;;
  
  dimension: order_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.order_id ;;
  }
  
  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }
  
  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
  }
  
  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }
  
  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
  
  # Derived dimensions
  dimension: revenue {
    type: number
    sql: ${amount} * 1.1 ;;  # Assuming 10% tax
  }
  
  dimension: month {
    type: date_month
    sql: ${order_date} ;;
  }
  
  # Measures
  measure: count {
    type: count
    drill_fields: [order_id, customer_id, amount]
  }
  
  measure: total_revenue {
    type: sum
    sql: ${revenue} ;;
    drill_fields: [order_id, customer_id, amount]
  }
  
  measure: average_order_value {
    type: average
    sql: ${amount} ;;
  }
  
  measure: revenue_last_month {
    type: sum
    sql: ${revenue} ;;
    filters: {
      field: order_date
      value: "1 month ago"
    }
  }
}

# File: dashboards/orders_overview.dashboard

- title: Orders Overview
  elements:
    - name: Daily Orders
      title: Daily Orders
      query: orders
      dimensions: [orders.order_date]
      measures: [orders.count]
      type: looker_column
      
    - name: Revenue by Status
      title: Revenue by Status
      query: orders
      dimensions: [orders.status]
      measures: [orders.total_revenue]
      type: looker_pie
      
    - name: Customer Metrics
      title: Top 10 Customers by Revenue
      query: orders__customers
      dimensions: [customers.name]
      measures: [orders.total_revenue]
      sorts: [orders.total_revenue: desc]
      limit: 10
      type: looker_table
```

---

## 📦 EXEMPLO 6: Metabase Dashboard (API)

```python
# File: metabase_dashboard.py

import requests
import json

class MetabaseManager:
    def __init__(self, host, email, password):
        self.host = host
        self.email = email
        self.password = password
        self.session_id = self.authenticate()
    
    def authenticate(self):
        """Authenticate with Metabase"""
        response = requests.post(
            f"{self.host}/api/session",
            json={"username": self.email, "password": self.password}
        )
        return response.json()['id']
    
    def create_dashboard(self, name, description=""):
        """Create new dashboard"""
        response = requests.post(
            f"{self.host}/api/dashboard",
            headers={"X-Metabase-Session": self.session_id},
            json={
                "name": name,
                "description": description,
                "caching_ttl": 3600,
            }
        )
        return response.json()['id']
    
    def add_card_to_dashboard(self, dashboard_id, card_id, position):
        """Add card (visualization) to dashboard"""
        requests.post(
            f"{self.host}/api/dashboard/{dashboard_id}/cards",
            headers={"X-Metabase-Session": self.session_id},
            json={
                "card_id": card_id,
                "sizeX": position['width'],
                "sizeY": position['height'],
                "col": position['x'],
                "row": position['y'],
            }
        )
    
    def create_question(self, name, database_id, query):
        """Create saved question"""
        response = requests.post(
            f"{self.host}/api/card",
            headers={"X-Metabase-Session": self.session_id},
            json={
                "name": name,
                "database_id": database_id,
                "dataset_query": {
                    "type": "native",
                    "native": {"query": query},
                    "database": database_id
                },
            }
        )
        return response.json()['id']

# Usage
mb = MetabaseManager(
    host="http://localhost:3000",
    email="admin@example.com",
    password="password"
)

# Create dashboard
dashboard_id = mb.create_dashboard("Orders Analytics", "Daily order metrics")

# Create questions
revenue_query = "SELECT DATE(order_date) as day, SUM(amount) as revenue FROM orders GROUP BY DATE(order_date)"
card_id = mb.create_question("Daily Revenue", 1, revenue_query)

# Add to dashboard
mb.add_card_to_dashboard(dashboard_id, card_id, {'x': 0, 'y': 0, 'width': 6, 'height': 4})

print(f"✓ Dashboard created: {dashboard_id}")
```

---

# 6️⃣ REAL-TIME STREAMING

## 📦 EXEMPLO 7: Apache Kafka Setup

```python
# File: kafka_setup.py

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
from datetime import datetime
import time

class KafkaDataPipeline:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='admin-client'
        )
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """Create Kafka topic"""
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs={
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'snappy'
            }
        )
        
        try:
            self.admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"✓ Created topic {topic_name}")
        except Exception as e:
            print(f"Topic exists: {e}")
    
    def produce_messages(self, topic_name, messages):
        """Produce messages to topic"""
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='snappy',
            acks='all',  # Wait for all replicas
            retries=3
        )
        
        for message in messages:
            producer.send(topic_name, value=message)
        
        producer.flush()
        print(f"✓ Produced {len(messages)} messages")
    
    def consume_messages(self, topic_name, process_fn):
        """Consume and process messages"""
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='my-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
        )
        
        for message in consumer:
            try:
                process_fn(message.value)
            except Exception as e:
                print(f"Error: {e}")

# Usage
kafka = KafkaDataPipeline()

# Create topics
kafka.create_topic('orders', num_partitions=3, replication_factor=1)
kafka.create_topic('user-events', num_partitions=3, replication_factor=1)

# Produce messages
orders = [
    {"order_id": 1, "customer_id": 100, "amount": 250.00, "timestamp": datetime.now().isoformat()},
    {"order_id": 2, "customer_id": 101, "amount": 175.50, "timestamp": datetime.now().isoformat()},
]

kafka.produce_messages('orders', orders)

# Consume messages
def process_order(order):
    print(f"Processing order {order['order_id']}: ${order['amount']}")

kafka.consume_messages('orders', process_order)
```

---

## 📦 EXEMPLO 8: Spark Structured Streaming

```python
# File: spark_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType()),
])

# Parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

df_parsed = df_parsed.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# Aggregations
df_agg = df_parsed \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("customer_id")
    ) \
    .agg(
        sum(col("amount")).alias("total_amount"),
        count(col("order_id")).alias("order_count")
    )

# Write to console (for debugging)
query = df_agg \
    .writeStream \
    .format("console") \
    .option("truncate", "False") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Write to Kafka
query_kafka = df_agg \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "orders-aggregated") \
    .option("checkpointLocation", "/tmp/checkpoint-kafka") \
    .start()

# Write to Delta Lake
query_delta = df_parsed \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint-delta") \
    .outputMode("append") \
    .option("path", "s3://my-bucket/orders-stream") \
    .start()

query.awaitTermination()
```

---

## RESUMO CATEGORIAS 4-6

✅ **Data Lakes:**
- AWS S3 + Glue
- Apache Iceberg
- Delta Lake
- Data governance
- Partitioning strategies

✅ **BI & Dashboards:**
- Power BI (DAX measures)
- Looker (LookML)
- Tableau (Calculations)
- Metabase (API)
- Best practices

✅ **Real-time Streaming:**
- Kafka setup
- Spark Structured Streaming
- Message processing
- Windowed aggregations
- Stream to Delta/Kafka

Próximas: Python/SQL, Cloud, Quality, ML
