# 7️⃣-🔟 PYTHON/SQL, CLOUD, QUALITY & ML

---

# 7️⃣ PYTHON & SQL PARA DATA

## 📦 EXEMPLO 1: Advanced Pandas Operations

```python
# File: pandas_advanced.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Read data
df = pd.read_csv('orders.csv')

# Data profiling
print(df.describe())
print(df.dtypes)
print(df.isnull().sum())

# Advanced grouping and aggregation
agg_result = df.groupby(['customer_id', pd.Grouper(key='order_date', freq='M')]).agg({
    'amount': ['sum', 'mean', 'count'],
    'order_id': 'count'
}).reset_index()

# Window functions (rolling/expanding)
df['30day_rolling_avg'] = df.groupby('customer_id')['amount'].rolling(window=30).mean().reset_index(drop=True)
df['cumsum_revenue'] = df.groupby('customer_id')['amount'].cumsum()
df['rank_by_amount'] = df.groupby('customer_id')['amount'].rank(method='dense', ascending=False)

# Merges and joins
df_merged = df.merge(
    df_customers,
    left_on='customer_id',
    right_on='id',
    how='left'
)

# MultiIndex operations
df_multi = df.set_index(['customer_id', 'order_date']).sort_index()
df_multi.loc[(100, '2024-01-15')]  # Fast lookups

# Pivoting
df_pivot = df.pivot_table(
    values='amount',
    index='customer_id',
    columns=pd.Grouper(key='order_date', freq='M'),
    aggfunc='sum'
)

# Apply custom functions
df['category_bin'] = df['amount'].apply(
    lambda x: 'High' if x > 500 else 'Medium' if x > 100 else 'Low'
)

# String operations
df['customer_segment'] = df['customer_type'].str.upper() + '_' + df['region'].str.lower()

# Datetime operations
df['order_date'] = pd.to_datetime(df['order_date'])
df['day_of_week'] = df['order_date'].dt.day_name()
df['month_year'] = df['order_date'].dt.to_period('M')

# Performance optimization
df['category'] = pd.Categorical(df['category'])  # Reduce memory
df_small = df[df['amount'] > 0].copy()  # Copy on write

# Save in efficient format
df.to_parquet('orders.parquet', index=False, compression='snappy')
df.to_feather('orders.feather')

print("✓ Advanced Pandas operations completed")
```

---

## 📦 EXEMPLO 2: PySpark for Distributed Processing

```python
# File: pyspark_data_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DistributedDataProcessing") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# Read data
df = spark.read.parquet("s3://bucket/orders/")

# Data exploration
df.printSchema()
df.count()
df.describe().show()

# Window functions (partition-based)
window_spec = Window.partitionBy("customer_id").orderBy("order_date")

df_with_windows = df.withColumn(
    "row_num", row_number().over(window_spec)
).withColumn(
    "rank_by_amount", rank().over(Window.partitionBy("customer_id").orderBy(desc("amount")))
).withColumn(
    "prev_amount", lag("amount").over(window_spec)
).withColumn(
    "running_total", sum("amount").over(window_spec)
)

# Complex aggregations
agg_df = df.groupBy("customer_id") \
    .agg(
        sum("amount").alias("total_spent"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date"),
        collect_set("category").alias("categories_purchased")
    )

# Joins
df_joined = df.join(
    spark.read.parquet("s3://bucket/customers/"),
    "customer_id",
    "left"
)

# UNION
df_all = df_2024.union(df_2023)

# Filtering and transformations
df_filtered = df.filter(
    (col("amount") > 100) & 
    (col("status") == "completed") &
    (year(col("order_date")) == 2024)
).withColumn(
    "revenue", col("amount") * (1 + col("tax_rate"))
)

# Explode arrays
df_exploded = df.select(
    col("order_id"),
    explode(col("items")).alias("item")
)

# Pivot (transpose)
df_pivot = df.groupBy("customer_id") \
    .pivot("category") \
    .agg(sum("amount"))

# Save in different formats
df.write.mode("overwrite").parquet("s3://bucket/processed/")
df.write.mode("overwrite").format("delta").save("s3://bucket/delta/orders")
df.coalesce(1).write.csv("s3://bucket/csv/")

print("✓ PySpark processing completed")
```

---

## 📦 EXEMPLO 3: Advanced SQL Patterns

```sql
-- File: advanced_sql.sql

-- CTEs (Common Table Expressions)
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as lifetime_value,
        MAX(order_date) as last_order_date
    FROM orders
    GROUP BY customer_id
),

customer_segments AS (
    SELECT
        customer_id,
        CASE
            WHEN lifetime_value > 5000 THEN 'VIP'
            WHEN lifetime_value > 1000 THEN 'Gold'
            ELSE 'Standard'
        END as segment,
        ROW_NUMBER() OVER (ORDER BY lifetime_value DESC) as rank
    FROM customer_orders
)

SELECT
    c.customer_id,
    c.customer_name,
    cs.segment,
    cs.order_count,
    cs.lifetime_value,
    cs.last_order_date
FROM customers c
JOIN customer_segments cs ON c.customer_id = cs.customer_id
WHERE cs.segment = 'VIP'
ORDER BY cs.lifetime_value DESC;

-- Window functions
SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    -- Cumulative sum
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total,
    -- Ranking
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_num,
    RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as amount_rank,
    -- Lead/Lag
    LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_amount,
    LEAD(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as next_order_date,
    -- Percent rank
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY amount) as pct_rank
FROM orders
ORDER BY customer_id, order_date;

-- Recursive CTE
WITH RECURSIVE date_range AS (
    SELECT DATE '2024-01-01' as date
    UNION ALL
    SELECT date + INTERVAL '1 day'
    FROM date_range
    WHERE date < DATE '2024-12-31'
)
SELECT * FROM date_range;

-- String aggregation
SELECT
    customer_id,
    STRING_AGG(DISTINCT category, ', ' ORDER BY category) as categories,
    STRING_AGG(CAST(order_id AS VARCHAR), '|') as order_ids
FROM orders
GROUP BY customer_id;

-- Pivot/Unpivot
SELECT
    customer_id,
    DATE_TRUNC('month', order_date) as month,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed,
    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) as pending,
    SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) as cancelled
FROM orders
GROUP BY customer_id, DATE_TRUNC('month', order_date);

-- Conditional aggregation
SELECT
    customer_id,
    COUNT(*) as total_orders,
    COUNT(*) FILTER (WHERE status = 'completed') as completed_orders,
    AVG(amount) FILTER (WHERE status = 'completed') as avg_completed,
    MAX(CASE WHEN status = 'completed' THEN order_date END) as last_completed
FROM orders
GROUP BY customer_id;
```

---

# 8️⃣ CLOUD DATA PLATFORMS

## 📦 EXEMPLO 4: AWS Data Architecture

```python
# File: aws_data_architecture.py

import boto3
from datetime import datetime

class AWSDataArchitecture:
    def __init__(self, region='us-east-1'):
        self.s3 = boto3.client('s3', region_name=region)
        self.glue = boto3.client('glue', region_name=region)
        self.athena = boto3.client('athena', region_name=region)
        self.redshift = boto3.client('redshift', region_name=region)
    
    def setup_data_pipeline(self, bucket_name):
        """Setup complete AWS data pipeline"""
        
        # 1. Ingest to S3
        print("1. Setting up S3 ingestion...")
        self.setup_s3_buckets(bucket_name)
        
        # 2. Catalog with Glue
        print("2. Setting up Glue catalog...")
        self.setup_glue_catalog(bucket_name)
        
        # 3. Query with Athena
        print("3. Querying with Athena...")
        self.query_with_athena(bucket_name)
        
        # 4. Load to Redshift
        print("4. Loading to Redshift...")
        self.load_to_redshift(bucket_name)
    
    def setup_s3_buckets(self, bucket_name):
        """Setup S3 buckets with lifecycle policies"""
        try:
            self.s3.create_bucket(Bucket=bucket_name)
        except:
            pass
        
        # Add lifecycle policy (archive old data)
        lifecycle_config = {
            'Rules': [
                {
                    'Id': 'Archive',
                    'Status': 'Enabled',
                    'Prefix': 'raw/',
                    'Transitions': [
                        {
                            'Days': 90,
                            'StorageClass': 'GLACIER'
                        }
                    ]
                }
            ]
        }
        
        self.s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
    
    def setup_glue_catalog(self, bucket_name):
        """Setup Glue catalog for data discovery"""
        # Create database
        self.glue.create_database(
            CatalogId='123456789012',
            DatabaseInput={'Name': 'analytics_db'}
        )
        
        # Create crawler
        self.glue.create_crawler(
            Name='data-crawler',
            Role='arn:aws:iam::123456789012:role/GlueRole',
            DatabaseName='analytics_db',
            S3Target={
                'Path': f's3://{bucket_name}/raw/',
                'Exclusions': ['_temporary/'],
            }
        )
    
    def query_with_athena(self, bucket_name):
        """Query data with Athena"""
        query = """
            SELECT
                order_id,
                customer_id,
                DATE(order_date) as order_day,
                SUM(amount) as daily_revenue
            FROM analytics_db.orders
            WHERE order_date >= DATE_ADD('day', -30, CURRENT_DATE)
            GROUP BY order_id, customer_id, DATE(order_date)
        """
        
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'analytics_db'},
            ResultConfiguration={'OutputLocation': f's3://{bucket_name}/athena-results/'},
        )
        
        return response['QueryExecutionId']
    
    def load_to_redshift(self, bucket_name):
        """Load aggregated data to Redshift"""
        # SQL COPY command
        copy_command = f"""
            COPY staging.orders FROM 's3://{bucket_name}/processed/'
            IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
            FORMAT AS PARQUET;
        """
        
        print(f"Execute in Redshift: {copy_command}")

# Usage
aws_arch = AWSDataArchitecture()
aws_arch.setup_data_pipeline('my-data-lake')
```

---

## 📦 EXEMPLO 5: Azure Data Architecture

```python
# File: azure_data_architecture.py

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.synapse.spark import SparkClient
import pandas as pd

class AzureDataArchitecture:
    def __init__(self, storage_account_name, synapse_workspace):
        self.storage_account = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=DefaultAzureCredential()
        )
        self.synapse_workspace = synapse_workspace
    
    def upload_to_data_lake(self, local_path, container_name, blob_path):
        """Upload data to Azure Data Lake"""
        container_client = self.storage_account.get_container_client(container_name)
        
        with open(local_path, 'rb') as data:
            container_client.upload_blob(blob_path, data, overwrite=True)
        
        print(f"✓ Uploaded {blob_path}")
    
    def query_with_synapse(self, query):
        """Query data with Synapse Analytics"""
        # Execute SQL query in Synapse
        # This would typically use Synapse SDK or direct SQL connection
        print(f"Executing in Synapse: {query}")

# Usage
azure_arch = AzureDataArchitecture(
    storage_account_name='mydata',
    synapse_workspace='mysynapse'
)

azure_arch.upload_to_data_lake('local_data.parquet', 'raw', 'orders/2024-01-15/')
```

---

# 9️⃣ DATA QUALITY & GOVERNANCE

## 📦 EXEMPLO 6: Great Expectations Validation

```python
# File: great_expectations_setup.py

from great_expectations.dataset import PandasDataset
from great_expectations.profile import BasicDatasetProfiler
import pandas as pd

class DataQualityFramework:
    def __init__(self, df):
        self.df = PandasDataset(df)
        self.results = []
    
    def validate_completeness(self):
        """Check for null values"""
        expectations = [
            self.df.expect_column_values_to_not_be_null("order_id"),
            self.df.expect_column_values_to_not_be_null("customer_id"),
            self.df.expect_column_values_to_not_be_null("order_date"),
        ]
        self.results.extend(expectations)
        return expectations
    
    def validate_uniqueness(self):
        """Check for duplicates"""
        expectations = [
            self.df.expect_column_values_to_be_unique("order_id"),
        ]
        self.results.extend(expectations)
        return expectations
    
    def validate_ranges(self):
        """Check value ranges"""
        expectations = [
            self.df.expect_column_values_to_be_between("amount", min_value=0, max_value=100000),
            self.df.expect_column_values_to_be_in_set("status", ["completed", "pending", "cancelled"]),
        ]
        self.results.extend(expectations)
        return expectations
    
    def validate_patterns(self):
        """Check string patterns"""
        expectations = [
            self.df.expect_column_values_to_match_regex("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
        ]
        self.results.extend(expectations)
        return expectations
    
    def run_all_validations(self):
        """Run all quality checks"""
        self.validate_completeness()
        self.validate_uniqueness()
        self.validate_ranges()
        self.validate_patterns()
        
        # Report
        passed = sum(1 for r in self.results if r['success'])
        total = len(self.results)
        
        print(f"✓ Data Quality: {passed}/{total} checks passed")
        
        return {
            'passed': passed,
            'total': total,
            'details': self.results
        }

# Usage
df = pd.read_csv('orders.csv')
quality = DataQualityFramework(df)
quality.run_all_validations()
```

---

## 📦 EXEMPLO 7: dbt Data Tests

```yaml
# File: dbt/tests/generic/test_not_null_and_unique.sql

{% test not_null_and_unique(model, column_name) %}

  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NULL
    OR {{ column_name }} IN (
      SELECT {{ column_name }}
      FROM {{ model }}
      GROUP BY {{ column_name }}
      HAVING COUNT(*) > 1
    )

{% endtest %}

# File: dbt/models/staging/schema.yml

version: 2

models:
  - name: stg_orders
    description: Cleaned orders
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: source('raw', 'orders')
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
          - not_null_and_unique
      - name: amount
        tests:
          - dbt_utils.accepted_values:
              values: ['positive']
              where: "status = 'completed'"
```

---

# 🔟 ML & ADVANCED ANALYTICS

## 📦 EXEMPLO 8: Feature Engineering for ML

```python
# File: feature_engineering.py

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

class FeatureEngineering:
    def __init__(self, df):
        self.df = df.copy()
    
    def create_temporal_features(self):
        """Extract time-based features"""
        self.df['order_date'] = pd.to_datetime(self.df['order_date'])
        self.df['day_of_week'] = self.df['order_date'].dt.dayofweek
        self.df['month'] = self.df['order_date'].dt.month
        self.df['quarter'] = self.df['order_date'].dt.quarter
        self.df['is_weekend'] = self.df['day_of_week'].isin([5, 6]).astype(int)
        self.df['days_since_first_order'] = (
            self.df.groupby('customer_id')['order_date'].transform('min')
        )
        self.df['days_since_first_order'] = (
            self.df['order_date'] - self.df['days_since_first_order']
        ).dt.days
        
        return self.df
    
    def create_customer_features(self):
        """Create customer-level features"""
        customer_stats = self.df.groupby('customer_id').agg({
            'order_id': 'count',
            'amount': ['sum', 'mean', 'std', 'min', 'max'],
        })
        
        customer_stats.columns = [
            'total_orders',
            'lifetime_value',
            'avg_order_value',
            'order_std',
            'min_order',
            'max_order'
        ]
        
        self.df = self.df.merge(
            customer_stats,
            left_on='customer_id',
            right_index=True,
            how='left'
        )
        
        # Order frequency
        self.df['orders_per_month'] = (
            self.df['total_orders'] / 
            (self.df['days_since_first_order'] / 30 + 1)
        )
        
        return self.df
    
    def create_lag_features(self):
        """Create lag features for time series"""
        for lag in [1, 7, 30]:
            self.df[f'amount_lag_{lag}'] = (
                self.df.groupby('customer_id')['amount'].shift(lag)
            )
        
        return self.df
    
    def create_interaction_features(self):
        """Create interaction features"""
        self.df['amount_per_order'] = (
            self.df['lifetime_value'] / self.df['total_orders']
        )
        
        self.df['high_value_customer'] = (
            self.df['lifetime_value'] > self.df['lifetime_value'].median()
        ).astype(int)
        
        return self.df
    
    def build_pipeline(self):
        """Build preprocessing pipeline"""
        numeric_features = ['amount', 'day_of_week', 'month']
        categorical_features = ['status', 'category']
        
        numeric_transformer = Pipeline(steps=[
            ('scaler', StandardScaler())
        ])
        
        categorical_transformer = Pipeline(steps=[
            ('onehot', OneHotEncoder(drop='first', sparse_output=False))
        ])
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_features),
                ('cat', categorical_transformer, categorical_features)
            ]
        )
        
        return preprocessor
    
    def prepare_for_ml(self):
        """Complete feature engineering pipeline"""
        df_features = self.create_temporal_features()
        df_features = self.create_customer_features()
        df_features = self.create_lag_features()
        df_features = self.create_interaction_features()
        
        # Drop unnecessary columns
        drop_cols = ['order_date', 'customer_id']
        df_features = df_features.drop(columns=drop_cols)
        
        # Handle missing values
        df_features = df_features.fillna(df_features.mean(numeric_only=True))
        
        return df_features

# Usage
df = pd.read_csv('orders.csv')
fe = FeatureEngineering(df)
df_ml = fe.prepare_for_ml()
print(df_ml.head())
```

---

## 📦 EXEMPLO 9: Model Training & Evaluation

```python
# File: model_training.py

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import xgboost as xgb
import pickle

class MLPipeline:
    def __init__(self, df, target_col):
        self.df = df
        self.target_col = target_col
        self.model = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
    
    def prepare_data(self, test_size=0.2, random_state=42):
        """Split data into train/test"""
        X = self.df.drop(columns=[self.target_col])
        y = self.df[self.target_col]
        
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y,
            test_size=test_size,
            random_state=random_state,
            stratify=y if y.nunique() < 10 else None
        )
        
        print(f"✓ Train: {len(self.X_train)}, Test: {len(self.X_test)}")
    
    def train_models(self):
        """Train multiple models"""
        models = {
            'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
            'Gradient Boosting': GradientBoostingClassifier(n_estimators=100, random_state=42),
            'XGBoost': xgb.XGBClassifier(n_estimators=100, random_state=42, eval_metric='logloss'),
        }
        
        results = {}
        
        for name, model in models.items():
            # Train
            model.fit(self.X_train, self.y_train)
            
            # Cross-validation
            cv_scores = cross_val_score(model, self.X_train, self.y_train, cv=5, scoring='roc_auc')
            
            # Test predictions
            y_pred = model.predict(self.X_test)
            y_pred_proba = model.predict_proba(self.X_test)[:, 1]
            
            # Evaluate
            auc = roc_auc_score(self.y_test, y_pred_proba)
            
            results[name] = {
                'model': model,
                'cv_mean': cv_scores.mean(),
                'cv_std': cv_scores.std(),
                'test_auc': auc,
                'y_pred': y_pred,
            }
            
            print(f"{name}: CV={cv_scores.mean():.4f}±{cv_scores.std():.4f}, Test AUC={auc:.4f}")
        
        # Select best model
        best_model_name = max(results, key=lambda x: results[x]['test_auc'])
        self.model = results[best_model_name]['model']
        
        return results
    
    def evaluate(self):
        """Detailed evaluation"""
        y_pred = self.model.predict(self.X_test)
        
        print("\nClassification Report:")
        print(classification_report(self.y_test, y_pred))
        
        print("\nConfusion Matrix:")
        print(confusion_matrix(self.y_test, y_pred))
    
    def save_model(self, filepath):
        """Save model to disk"""
        with open(filepath, 'wb') as f:
            pickle.dump(self.model, f)
        print(f"✓ Model saved to {filepath}")
    
    def load_model(self, filepath):
        """Load model from disk"""
        with open(filepath, 'rb') as f:
            self.model = pickle.load(f)
        print(f"✓ Model loaded from {filepath}")

# Usage
ml = MLPipeline(df_ml, target_col='churn')
ml.prepare_data()
results = ml.train_models()
ml.evaluate()
ml.save_model('churn_model.pkl')
```

---

## RESUMO TODAS AS 10 CATEGORIAS

✅ **Categoria 1:** Extração (Web, APIs, DB)
✅ **Categoria 2:** ETL/ELT (Airflow, dbt, Luigi)
✅ **Categoria 3:** Data Warehousing (Snowflake, BigQuery, Redshift)
✅ **Categoria 4:** Data Lakes (S3, Iceberg, Delta)
✅ **Categoria 5:** BI (Power BI, Looker, Tableau, Metabase)
✅ **Categoria 6:** Real-time (Kafka, Spark Streaming)
✅ **Categoria 7:** Python/SQL (Pandas, PySpark, SQL avançado)
✅ **Categoria 8:** Cloud (AWS, Azure, GCP)
✅ **Categoria 9:** Data Quality (Great Expectations, dbt tests)
✅ **Categoria 10:** ML & Analytics (Feature engineering, Models)

**Total:** 150+ exemplos, 100+ scripts prontos

---

**Todas as 10 categorias estão COMPLETAS!** 🚀
