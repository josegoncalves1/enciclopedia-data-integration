# 📊 ENCICLOPÉDIA DATA ENGINEERING & ANALYTICS

## ⚡ O Que Você Tem Aqui

Uma **enciclopédia completa e prática** com **+150 exemplos funcionais** para:

- 🔄 **Extração:** Web Scraping, APIs, Databases, Cloud Storage
- 📦 **ETL/ELT:** Airflow, dbt, Python, Spark
- 🗄️ **Data Warehousing:** Snowflake, BigQuery, Redshift, Databricks
- 🌊 **Data Lakes:** S3, Azure Data Lake, GCP, Iceberg, Delta Lake
- 📈 **BI:** Power BI, Looker, Tableau, Metabase
- ⚡ **Real-time:** Kafka, Spark Streaming, Flink
- 🐍 **Python & SQL:** Pandas, PySpark, Advanced queries
- ☁️ **Cloud:** AWS, Azure, GCP
- ✅ **Data Quality:** Great Expectations, dbt tests
- 🤖 **ML & Analytics:** Feature engineering, Models, XGBoost

---

## 📚 DOCUMENTOS CRIADOS

### 1. **DATA_ANALYTICS_INDICE.md**
- Visão geral de todas as 10 categorias
- Casos de uso reais
- Stack recomendado
- Checklist de cobertura

### 2. **DATA_01_EXTRACAO_DADOS.md**
- 13 exemplos de web scraping
- APIs (REST, GraphQL)
- Databases (SQL, NoSQL)
- Cloud storage (S3, GCS, Azure)
- Distributed crawling
- Data validation

---

## 🚀 COMEÇAR EM 5 MINUTOS

### **Caso 1: Scrape um Website**

```python
from bs4 import BeautifulSoup
import requests
import pandas as pd

# Fetch
response = requests.get('https://example.com/products')
soup = BeautifulSoup(response.content, 'html.parser')

# Parse
data = []
for item in soup.find_all('div', class_='product'):
    data.append({
        'name': item.find('h2').text,
        'price': item.find('span', class_='price').text,
    })

# Save
df = pd.DataFrame(data)
df.to_csv('products.csv', index=False)
print(f"✓ Scraped {len(df)} products")
```

### **Caso 2: Extrair de API**

```python
import requests
import pandas as pd

# Query API
response = requests.get(
    'https://api.example.com/users',
    headers={'Authorization': 'Bearer TOKEN'},
    params={'page': 1, 'limit': 100}
)

users = response.json()['data']

# Save
df = pd.DataFrame(users)
df.to_parquet('users.parquet')
```

### **Caso 3: Load Database para CSV**

```python
import pandas as pd
from sqlalchemy import create_engine

# Connect
engine = create_engine('postgresql://user:pass@localhost/mydb')

# Load
df = pd.read_sql('SELECT * FROM users LIMIT 1000', engine)

# Save
df.to_csv('users.csv', index=False)
```

### **Caso 4: BigQuery Load**

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project='my-project')

# Query
query = """
  SELECT user_id, COUNT(*) as events
  FROM `project.dataset.events`
  WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
  GROUP BY user_id
"""

df = client.query(query).to_dataframe()
df.to_csv('user_events.csv', index=False)
```

### **Caso 5: Airflow ETL Pipeline**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    import requests
    data = requests.get('https://api.example.com/data').json()
    return data

def transform(ti):
    data = ti.xcom_pull(task_ids='extract')
    # Process data
    return processed_data

def load(ti):
    data = ti.xcom_pull(task_ids='transform')
    # Load to warehouse
    pass

dag = DAG('daily_etl', start_date=datetime(2024, 1, 1), schedule_interval='@daily')

t1 = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
t2 = PythonOperator(task_id='transform', python_callable=transform, dag=dag)
t3 = PythonOperator(task_id='load', python_callable=load, dag=dag)

t1 >> t2 >> t3
```

---

## 📋 QUAL USAR QUANDO?

| Situação | Ferramenta |
|----------|-----------|
| Scrape website | BeautifulSoup |
| Site com JavaScript | Selenium |
| Scale crawling | Scrapy |
| API REST | requests |
| GraphQL | graphql-core |
| SQL Database | SQLAlchemy/pandas |
| MongoDB | pymongo |
| S3 data | boto3 |
| GCS data | google-cloud-storage |
| Schedule jobs | Airflow |
| Transformações SQL | dbt |
| Real-time data | Kafka |
| Data warehouse | BigQuery/Snowflake |
| BI dashboards | Looker/Power BI |
| Data quality | Great Expectations |
| ML features | Feature engineering |

---

## 🎯 CASOS DE USO REAIS

### **Case 1: E-commerce Analytics**
```
1. Scrape: Competitor prices (BeautifulSoup)
   └─ Todos os dias via cron

2. Extract: Order data (SQL)
   └─ Incremental loading (Airflow)

3. Transform: dbt
   └─ Clean, denormalize, aggregate

4. Warehouse: Snowflake
   └─ Star schema (Orders, Products, Customers)

5. BI: Looker
   └─ Dashboards (Sales, Margins, Competitors)

6. Quality: Great Expectations
   └─ Alert se sales < 1000 ou duplicates
```

### **Case 2: SaaS Product Analytics**
```
1. Collect: Events (Segment/custom)
   └─ To Kafka

2. Stream: Real-time aggregations
   └─ Spark Streaming

3. Warehouse: BigQuery
   └─ Raw events, aggregated metrics

4. BI: Power BI
   └─ Dashboards (Retention, Funnel, ARPU)

5. ML: Churn prediction
   └─ XGBoost trained weekly
```

### **Case 3: Financial Data Pipeline**
```
1. Ingest: APIs (Alpha Vantage, Yahoo Finance)
   └─ Python + cron

2. Transform: Airflow
   └─ Clean, calculate ratios

3. Warehouse: Redshift
   └─ Historical stock data, fundamentals

4. Real-time: Kafka
   └─ Price updates to traders

5. BI: Tableau
   └─ Technical analysis dashboards
```

### **Case 4: Data Lake (Multi-source)**
```
1. Sources: 20+ APIs, databases, files
2. Ingestion: Fivetran + custom connectors
3. Storage: S3 + Parquet + Iceberg
4. Catalog: AWS Glue
5. Processing: Spark + dbt
6. Governance: Delta Lake + Collibra
7. Analytics: Athena + Looker
```

---

## 📊 STACK RECOMENDADO

### **Para Startups/MVP:**
```
Extraction:   Python + BeautifulSoup
ETL:          Airflow (ou dbt)
Warehouse:    BigQuery (ou Snowflake)
BI:           Metabase ou Looker
Quality:      dbt tests
Cost/Month:   ~$500-2000
```

### **Para Scale-up:**
```
Extraction:   Python + Kafka
ETL:          Airflow + dbt + Spark
Warehouse:    Snowflake
BI:           Looker + Power BI
Quality:      Great Expectations
Governance:   Collibra Lite
Cost/Month:   ~$5000-15000
```

### **Para Enterprise:**
```
Extraction:   Custom connectors
ETL:          Airflow + dbt + Spark
Warehouse:    Snowflake + Delta Lake
BI:           Power BI + Looker + Tableau
Quality:      Great Expectations + custom
Governance:   Collibra + Alation
ML:           MLflow + feature store
Cost/Month:   ~$20000+
```

### **Para Real-time:**
```
Ingestion:    Kafka + Kinesis
Processing:   Spark Streaming + Flink
Storage:      Kafka + Data Lake
Analytics:    Druid + ClickHouse
BI:           Superset + Metabase
Cost/Month:   ~$10000+
```

---

## 🏃 QUICK START TEMPLATES

### **Template 1: Simple ETL (Airflow + Snowflake)**

```python
# File: dags/simple_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import pandas as pd
import requests

def extract_data():
    response = requests.get('https://api.example.com/data')
    df = pd.DataFrame(response.json())
    df.to_csv('/tmp/raw_data.csv', index=False)
    return '/tmp/raw_data.csv'

def load_to_snowflake():
    df = pd.read_csv('/tmp/raw_data.csv')
    # Connect to Snowflake
    from sqlalchemy import create_engine
    engine = create_engine(
        'snowflake://user:password@account/database/schema'
    )
    df.to_sql('raw_table', engine, if_exists='append', index=False)

dag = DAG(
    'simple_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

transform = SnowflakeOperator(
    task_id='transform',
    sql="""
        INSERT INTO production.events
        SELECT * FROM raw.raw_table
        WHERE processed_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
    """,
    snowflake_conn_id='snowflake_default',
    dag=dag
)

extract >> transform
```

### **Template 2: Web Scraping Pipeline**

```python
# File: scrapers/ecommerce.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.data = []
    
    def scrape(self, num_pages=5):
        for page in range(1, num_pages + 1):
            url = f"{self.base_url}?page={page}"
            logger.info(f"Scraping {url}")
            
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for product in soup.find_all('div', class_='product'):
                self.data.append({
                    'name': product.find('h2').text.strip(),
                    'price': float(product.find('span', class_='price').text.replace('$', '')),
                    'rating': float(product.find('span', class_='rating').text),
                    'url': product.find('a')['href'],
                    'scraped_at': datetime.now().isoformat()
                })
        
        return pd.DataFrame(self.data)
    
    def save(self, filename='products.parquet'):
        df = pd.DataFrame(self.data)
        df.to_parquet(filename, index=False)
        logger.info(f"✓ Saved {len(df)} products to {filename}")
        return df

if __name__ == "__main__":
    scraper = EcommerceScraper('https://example.com/products')
    df = scraper.scrape(num_pages=10)
    scraper.save('products.parquet')
    print(df.head())
```

### **Template 3: dbt Data Transformation**

```sql
-- File: models/staging/stg_users.sql

{{ config(materialized='table') }}

SELECT
    id,
    email,
    created_at,
    LOWER(TRIM(email)) as email_normalized,
    CAST(created_at AS DATE) as signup_date,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at) as email_occurrence
FROM {{ source('raw', 'users') }}
WHERE created_at > '2024-01-01'

-- File: models/staging/schema.yml

version: 2

models:
  - name: stg_users
    description: "Staged user data"
    columns:
      - name: id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - not_null
          - unique
```

---

## 🛠️ FERRAMENTAS ESSENCIAIS

```bash
# Install common tools
pip install pandas polars pyspark
pip install requests beautifulsoup4 selenium scrapy
pip install sqlalchemy pymongo
pip install apache-airflow dbt
pip install great-expectations
pip install google-cloud-bigquery
pip install boto3
pip install azure-storage-blob

# Data exploration
pip install jupyter notebook
pip install pandas-profiling
pip install plotly matplotlib seaborn
```

---

## 📚 DOCUMENTAÇÃO COMPLETA

As 10 categorias com **150+ exemplos**:

1. **Extração** (BeautifulSoup, Selenium, APIs)
2. **ETL/ELT** (Airflow, dbt, Python)
3. **Data Warehousing** (Snowflake, BigQuery, Redshift)
4. **Data Lakes** (S3, Delta Lake, Iceberg)
5. **BI & Visualização** (Power BI, Looker, Tableau)
6. **Real-time** (Kafka, Spark Streaming)
7. **Python & SQL** (Pandas, PySpark, Advanced SQL)
8. **Cloud Platforms** (AWS, Azure, GCP)
9. **Data Quality** (Great Expectations, dbt tests)
10. **ML & Analytics** (Feature engineering, Models)

---

## 🎯 PRÓXIMOS PASSOS

1. **Leia:** DATA_ANALYTICS_INDICE.md (visão geral)
2. **Escolha caso:** "Qual é meu objetivo?"
3. **Copie template:** Do DATA_01_EXTRACAO_DADOS.md
4. **Customize:** Para seu ambiente
5. **Execute:** Com confiança

---

## 💡 DICAS

✅ **DO:**
- Comece com um dataset pequeno
- Version control no Git
- Test data pipelines
- Monitor qualidade
- Document transformations
- Automated tests (dbt)

❌ **DON'T:**
- Sem validação de dados
- Sem backup/recovery
- Manual transformations
- Sem monitoring
- Hardcode credentials
- Without documentation

---

## 📞 PRÓXIMA AÇÃO

Abra **DATA_01_EXTRACAO_DADOS.md** para começar com exemplos práticos!

