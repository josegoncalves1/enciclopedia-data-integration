# 1️⃣ EXTRAÇÃO DE DADOS — GUIA COMPLETO

## 📌 SUMÁRIO
- 1.1 Web Scraping (BeautifulSoup, Selenium, Scrapy)
- 1.2 APIs (REST, GraphQL, Authentication)
- 1.3 Databases (SQL, NoSQL)
- 1.4 Files (CSV, JSON, Parquet, Excel)
- 1.5 Cloud Storage (S3, GCS, Azure Blob)
- 1.6 Distributed Crawling
- 1.7 Data Validation & Cleaning

---

# 1.1 WEB SCRAPING

## 📦 EXEMPLO 1: BeautifulSoup - Site Estático

```python
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.data = []
    
    def fetch_page(self, url, retries=3):
        """Fetch page com retry logic"""
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                logger.info(f"✓ Fetched: {url}")
                return response.text
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"✗ Failed to fetch {url}")
                    return None
    
    def parse_products(self, html):
        """Parse produto list"""
        soup = BeautifulSoup(html, 'html.parser')
        products = []
        
        for item in soup.find_all('div', class_='product-item'):
            try:
                product = {
                    'name': item.find('h2', class_='product-name').text.strip(),
                    'price': item.find('span', class_='price').text.strip(),
                    'rating': item.find('span', class_='rating').text.strip(),
                    'url': item.find('a', class_='product-link')['href'],
                    'scraped_at': datetime.now().isoformat()
                }
                products.append(product)
            except AttributeError as e:
                logger.warning(f"Failed to parse item: {e}")
                continue
        
        return products
    
    def scrape_multiple_pages(self, urls):
        """Scrape múltiplas páginas"""
        for url in urls:
            html = self.fetch_page(url)
            if html:
                products = self.parse_products(html)
                self.data.extend(products)
        
        return pd.DataFrame(self.data)
    
    def save_data(self, filename='products.csv'):
        """Save para CSV"""
        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False)
        logger.info(f"✓ Saved {len(df)} rows to {filename}")
        return df

# Usar
if __name__ == "__main__":
    scraper = WebScraper('https://example.com')
    
    urls = [
        'https://example.com/products?page=1',
        'https://example.com/products?page=2',
        'https://example.com/products?page=3',
    ]
    
    df = scraper.scrape_multiple_pages(urls)
    scraper.save_data('products.csv')
    
    print(df.head())
```

---

## 📦 EXEMPLO 2: Selenium - Site Dinâmico (JavaScript)

```python
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time

class DynamicWebScraper:
    def __init__(self, headless=True):
        options = Options()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 10)
    
    def scrape_dynamic_page(self, url):
        """Scrape page que usa JavaScript"""
        try:
            self.driver.get(url)
            
            # Wait for dynamic content to load
            self.wait.until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, 'dynamic-item'))
            )
            
            # Scroll para carregar mais (infinite scroll)
            for _ in range(5):
                self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(2)
            
            # Parse HTML
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            items = []
            for item in soup.find_all('div', class_='dynamic-item'):
                items.append({
                    'title': item.find('h3').text,
                    'price': item.find('span', class_='price').text,
                })
            
            return items
        
        finally:
            self.driver.quit()
    
    def scrape_with_interactions(self, url):
        """Scrape com clicks e interactions"""
        self.driver.get(url)
        
        # Click "Load More" button
        try:
            load_more_btn = self.wait.until(
                EC.element_to_be_clickable((By.ID, 'load-more'))
            )
            load_more_btn.click()
            time.sleep(3)
        except:
            print("No load more button found")
        
        # Select dropdown option
        dropdown = self.driver.find_element(By.ID, 'sort-dropdown')
        dropdown.select_by_value('price-low-high')
        time.sleep(2)
        
        return self.driver.page_source

# Usar
scraper = DynamicWebScraper(headless=True)
items = scraper.scrape_dynamic_page('https://example.com/products')
print(pd.DataFrame(items))
```

---

## 📦 EXEMPLO 3: Scrapy Framework - Production-scale

```python
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import logging

# Define item
class ProductItem(scrapy.Item):
    name = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    url = scrapy.Field()
    scraped_at = scrapy.Field()

# Spider
class ProductSpider(CrawlSpider):
    name = 'products'
    allowed_domains = ['example.com']
    start_urls = ['https://example.com/products']
    
    # Rules for following links
    rules = (
        Rule(
            LinkExtractor(restrict_xpaths='//a[@class="next-page"]'),
            callback='parse_items',
            follow=True
        ),
    )
    
    def parse_items(self, response):
        """Parse product items"""
        for product in response.css('div.product-item'):
            item = ProductItem()
            item['name'] = product.css('h2::text').get().strip()
            item['price'] = product.css('span.price::text').get().strip()
            item['rating'] = product.css('span.rating::text').get().strip()
            item['url'] = response.urljoin(product.css('a::attr(href)').get())
            item['scraped_at'] = datetime.now().isoformat()
            
            yield item

# Pipeline para processar items
class ProductPipeline:
    def __init__(self):
        self.file = None
    
    def open_spider(self, spider):
        self.file = open('products.jsonl', 'w')
    
    def close_spider(self, spider):
        self.file.close()
    
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + '\n'
        self.file.write(line)
        return item

# settings.py
SPIDER_MODULES = ['myapp.spiders']
NEWSPIDER_MODULE = 'myapp.spiders'
PIPELINES = {
    'myapp.pipelines.ProductPipeline': 300,
}
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 30

# Usar
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0',
    'ROBOTSTXT_OBEY': True,
})
process.crawl(ProductSpider)
process.start()
```

---

# 1.2 APIs

## 📦 EXEMPLO 4: REST API com Autenticação

```python
import requests
import json
from requests.auth import HTTPBasicAuth
from functools import wraps
import time

class APIClient:
    def __init__(self, base_url, api_key=None, auth_type='bearer'):
        self.base_url = base_url
        self.api_key = api_key
        self.auth_type = auth_type
        self.session = requests.Session()
    
    def _get_headers(self):
        """Prepare headers with auth"""
        headers = {
            'User-Agent': 'DataExtractor/1.0',
            'Content-Type': 'application/json'
        }
        
        if self.api_key:
            if self.auth_type == 'bearer':
                headers['Authorization'] = f'Bearer {self.api_key}'
            elif self.auth_type == 'api-key':
                headers['X-API-Key'] = self.api_key
        
        return headers
    
    def rate_limit(func):
        """Decorator para rate limiting"""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            time.sleep(0.5)  # 2 requests/sec max
            return func(self, *args, **kwargs)
        return wrapper
    
    @rate_limit
    def get(self, endpoint, params=None):
        """GET request"""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
    
    @rate_limit
    def post(self, endpoint, data=None):
        """POST request"""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.post(
                url,
                headers=self._get_headers(),
                json=data,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return None
    
    def paginate(self, endpoint, limit=None):
        """Handle pagination"""
        page = 1
        count = 0
        
        while True:
            response = self.get(endpoint, params={'page': page, 'per_page': 100})
            
            if not response or 'data' not in response:
                break
            
            for item in response['data']:
                yield item
                count += 1
                if limit and count >= limit:
                    return
            
            if 'next_page' not in response or response['next_page'] is None:
                break
            
            page += 1

# Usar
client = APIClient(
    base_url='https://api.example.com/v1',
    api_key='your-api-key',
    auth_type='bearer'
)

# Single request
users = client.get('users', params={'role': 'admin'})
print(users)

# Paginated
all_users = list(client.paginate('users', limit=1000))
df = pd.DataFrame(all_users)
```

---

## 📦 EXEMPLO 5: GraphQL API

```python
import requests
import json

class GraphQLClient:
    def __init__(self, endpoint, api_key=None):
        self.endpoint = endpoint
        self.api_key = api_key
    
    def execute(self, query, variables=None):
        """Execute GraphQL query"""
        headers = {
            'Content-Type': 'application/json',
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        payload = {
            'query': query,
            'variables': variables or {}
        }
        
        response = requests.post(
            self.endpoint,
            headers=headers,
            json=payload
        )
        
        return response.json()

# Query
query = """
    query GetUsers($limit: Int!) {
        users(limit: $limit) {
            id
            name
            email
            createdAt
        }
    }
"""

# Usar
client = GraphQLClient(
    endpoint='https://api.example.com/graphql',
    api_key='your-token'
)

result = client.execute(query, variables={'limit': 100})

if 'errors' in result:
    print(f"Error: {result['errors']}")
else:
    df = pd.DataFrame(result['data']['users'])
    print(df)
```

---

# 1.3 BANCO DE DADOS

## 📦 EXEMPLO 6: Extract from SQL Databases

```python
import pandas as pd
from sqlalchemy import create_engine, inspect
import logging

class DatabaseExtractor:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)
        self.inspector = inspect(self.engine)
    
    def get_tables(self):
        """List all tables"""
        return self.inspector.get_table_names()
    
    def extract_table(self, table_name, limit=None):
        """Extract table data"""
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        df = pd.read_sql(query, self.engine)
        print(f"✓ Extracted {len(df)} rows from {table_name}")
        return df
    
    def extract_query(self, sql_query, chunksize=10000):
        """Execute custom query (streaming for large datasets)"""
        chunks = pd.read_sql(sql_query, self.engine, chunksize=chunksize)
        
        for chunk in chunks:
            yield chunk
    
    def extract_incremental(self, table_name, last_modified_col, last_timestamp):
        """Extract apenas dados novos (incremental)"""
        query = f"""
            SELECT * FROM {table_name}
            WHERE {last_modified_col} > '{last_timestamp}'
            ORDER BY {last_modified_col}
        """
        
        return pd.read_sql(query, self.engine)

# PostgreSQL
pg_extractor = DatabaseExtractor(
    'postgresql://user:password@localhost:5432/mydb'
)

# MySQL
mysql_extractor = DatabaseExtractor(
    'mysql+pymysql://user:password@localhost:3306/mydb'
)

# SQL Server
mssql_extractor = DatabaseExtractor(
    'mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server'
)

# Usar
df = pg_extractor.extract_table('users', limit=1000)
df.to_parquet('users.parquet')

# Incremental
import datetime
last_run = datetime.datetime(2024, 1, 1)
new_data = pg_extractor.extract_incremental('events', 'updated_at', last_run)
```

---

## 📦 EXEMPLO 7: NoSQL (MongoDB)

```python
from pymongo import MongoClient
import pandas as pd

class MongoExtractor:
    def __init__(self, connection_string):
        self.client = MongoClient(connection_string)
    
    def extract_collection(self, db_name, collection_name, query=None, limit=None):
        """Extract from MongoDB collection"""
        db = self.client[db_name]
        collection = db[collection_name]
        
        cursor = collection.find(query or {})
        
        if limit:
            cursor = cursor.limit(limit)
        
        data = list(cursor)
        
        # Remove MongoDB ID for cleaner data
        for doc in data:
            doc.pop('_id', None)
        
        return pd.DataFrame(data)
    
    def extract_aggregation(self, db_name, collection_name, pipeline):
        """Run aggregation pipeline"""
        db = self.client[db_name]
        collection = db[collection_name]
        
        result = collection.aggregate(pipeline)
        return pd.DataFrame(list(result))

# Usar
extractor = MongoExtractor('mongodb://localhost:27017')

# Simple extract
df = extractor.extract_collection('mydb', 'users')

# Aggregation
pipeline = [
    {'$match': {'status': 'active'}},
    {'$group': {'_id': '$country', 'count': {'$sum': 1}}},
    {'$sort': {'count': -1}},
]
df = extractor.extract_aggregation('mydb', 'users', pipeline)
```

---

# 1.4 FILES

## 📦 EXEMPLO 8: CSV, JSON, Parquet

```python
import pandas as pd
import json
import pyarrow.parquet as pq

class FileExtractor:
    @staticmethod
    def read_csv(filepath, **kwargs):
        """Read CSV (com dtypes otimizadas)"""
        df = pd.read_csv(filepath, **kwargs)
        return df
    
    @staticmethod
    def read_json(filepath):
        """Read JSON lines (streaming-friendly)"""
        data = []
        with open(filepath, 'r') as f:
            for line in f:
                data.append(json.loads(line))
        return pd.DataFrame(data)
    
    @staticmethod
    def read_json_file(filepath):
        """Read JSON array"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    
    @staticmethod
    def read_parquet(filepath, columns=None):
        """Read Parquet (column filtering)"""
        table = pq.read_table(filepath, columns=columns)
        return table.to_pandas()
    
    @staticmethod
    def read_excel(filepath, sheet_name=0):
        """Read Excel"""
        return pd.read_excel(filepath, sheet_name=sheet_name)
    
    @staticmethod
    def read_large_csv(filepath, chunksize=100000):
        """Stream large CSV"""
        chunks = pd.read_csv(filepath, chunksize=chunksize)
        for chunk in chunks:
            yield chunk

# Usar
# CSV
df = FileExtractor.read_csv('data.csv', dtype={'id': 'int32', 'amount': 'float32'})

# JSON Lines
df = FileExtractor.read_json('data.jsonl')

# Parquet (efficient)
df = FileExtractor.read_parquet('data.parquet', columns=['id', 'name', 'amount'])

# Large file streaming
for chunk in FileExtractor.read_large_csv('huge_file.csv', chunksize=50000):
    process_chunk(chunk)
```

---

# 1.5 CLOUD STORAGE

## 📦 EXEMPLO 9: AWS S3 Extract

```python
import boto3
import pandas as pd
from io import BytesIO

class S3Extractor:
    def __init__(self, bucket_name, region='us-east-1'):
        self.s3 = boto3.client('s3', region_name=region)
        self.bucket = bucket_name
    
    def list_objects(self, prefix=''):
        """List all objects in bucket"""
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )
        
        objects = []
        for obj in response.get('Contents', []):
            objects.append({
                'Key': obj['Key'],
                'Size': obj['Size'],
                'LastModified': obj['LastModified']
            })
        
        return pd.DataFrame(objects)
    
    def read_csv(self, key):
        """Read CSV from S3"""
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_csv(obj['Body'])
    
    def read_parquet(self, key):
        """Read Parquet from S3"""
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return pd.read_parquet(BytesIO(obj['Body'].read()))
    
    def read_json_lines(self, key):
        """Read JSONL from S3"""
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        lines = obj['Body'].read().decode('utf-8').split('\n')
        data = [json.loads(line) for line in lines if line]
        return pd.DataFrame(data)
    
    def download_file(self, key, filepath):
        """Download file from S3"""
        self.s3.download_file(self.bucket, key, filepath)

# Usar
s3_extractor = S3Extractor('my-data-bucket')

# List files
files = s3_extractor.list_objects('raw/2024/')
print(files)

# Read data
df = s3_extractor.read_csv('raw/users.csv')
df = s3_extractor.read_parquet('processed/sales.parquet')
```

---

## 📦 EXEMPLO 10: Google Cloud Storage

```python
from google.cloud import storage
import pandas as pd
from io import BytesIO

class GCSExtractor:
    def __init__(self, bucket_name, project_id=None):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
    
    def read_csv(self, blob_name):
        """Read CSV from GCS"""
        blob = self.bucket.blob(blob_name)
        data = blob.download_as_bytes()
        return pd.read_csv(BytesIO(data))
    
    def read_parquet(self, blob_name):
        """Read Parquet from GCS"""
        blob = self.bucket.blob(blob_name)
        data = blob.download_as_bytes()
        return pd.read_parquet(BytesIO(data))
    
    def upload_data(self, df, blob_name):
        """Upload data to GCS"""
        blob = self.bucket.blob(blob_name)
        
        if blob_name.endswith('.csv'):
            blob.upload_from_string(
                df.to_csv(index=False),
                content_type='text/csv'
            )
        elif blob_name.endswith('.parquet'):
            blob.upload_from_string(
                df.to_parquet(index=False),
                content_type='application/octet-stream'
            )

# Usar
gcs_extractor = GCSExtractor('my-bucket')
df = gcs_extractor.read_csv('data/users.csv')
```

---

## 📦 EXEMPLO 11: Azure Blob Storage

```python
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO

class AzureBlobExtractor:
    def __init__(self, connection_string, container_name):
        self.client = BlobServiceClient.from_connection_string(connection_string)
        self.container = self.client.get_container_client(container_name)
    
    def read_csv(self, blob_name):
        """Read CSV from Azure Blob"""
        blob = self.container.get_blob_client(blob_name)
        data = blob.download_blob().readall()
        return pd.read_csv(BytesIO(data))
    
    def list_blobs(self, prefix=''):
        """List blobs in container"""
        blobs = self.container.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blobs]

# Usar
extractor = AzureBlobExtractor(
    'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...',
    'my-container'
)

df = extractor.read_csv('data/file.csv')
```

---

# 1.6 DISTRIBUTED CRAWLING

## 📦 EXEMPLO 12: Distributed Scraping com Redis Queue

```python
from rq import Queue
from redis import Redis
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Worker function
def scrape_url(url):
    """Worker que scrapa uma URL"""
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        return {
            'url': url,
            'title': soup.title.string if soup.title else None,
            'status': 'success'
        }
    except Exception as e:
        return {
            'url': url,
            'error': str(e),
            'status': 'failed'
        }

# Producer (enfileira jobs)
redis_conn = Redis()
q = Queue(connection=redis_conn)

urls = [
    'https://example.com/page1',
    'https://example.com/page2',
    # ... thousands of URLs
]

jobs = []
for url in urls:
    job = q.enqueue(scrape_url, url, timeout=30)
    jobs.append(job)

# Monitor progress
from rq.job import JobStatus

completed = 0
results = []

while completed < len(jobs):
    for job in jobs:
        if job.get_status() == JobStatus.FINISHED:
            results.append(job.result)
            jobs.remove(job)
            completed += 1
    
    print(f"Progress: {completed}/{len(jobs)}")
    time.sleep(5)

# Save results
df = pd.DataFrame(results)
df.to_csv('scraped_data.csv', index=False)
```

---

# 1.7 DATA VALIDATION

## 📦 EXEMPLO 13: Validar Dados Extraídos

```python
import pandas as pd
from pandas import DataFrame
import logging

class DataValidator:
    def __init__(self, df):
        self.df = df
        self.errors = []
    
    def check_completeness(self, columns):
        """Check for null values"""
        for col in columns:
            null_count = self.df[col].isnull().sum()
            if null_count > 0:
                self.errors.append(
                    f"Column '{col}' has {null_count} null values"
                )
        return self
    
    def check_duplicates(self, subset=None):
        """Check for duplicates"""
        dup_count = self.df.duplicated(subset=subset).sum()
        if dup_count > 0:
            self.errors.append(f"Found {dup_count} duplicate rows")
        return self
    
    def check_data_types(self, dtype_dict):
        """Check column data types"""
        for col, expected_type in dtype_dict.items():
            if self.df[col].dtype != expected_type:
                self.errors.append(
                    f"Column '{col}' has type {self.df[col].dtype}, expected {expected_type}"
                )
        return self
    
    def check_value_ranges(self, column, min_val, max_val):
        """Check if values are in range"""
        invalid = (self.df[column] < min_val) | (self.df[column] > max_val)
        if invalid.sum() > 0:
            self.errors.append(
                f"Column '{column}' has {invalid.sum()} values outside range [{min_val}, {max_val}]"
            )
        return self
    
    def check_regex(self, column, pattern):
        """Check if values match regex"""
        import re
        invalid = ~self.df[column].astype(str).str.match(pattern)
        if invalid.sum() > 0:
            self.errors.append(
                f"Column '{column}' has {invalid.sum()} values not matching pattern '{pattern}'"
            )
        return self
    
    def report(self):
        """Print validation report"""
        if self.errors:
            print("❌ Validation failed:")
            for error in self.errors:
                print(f"  - {error}")
            return False
        else:
            print("✓ All validation checks passed")
            return True

# Usar
df = pd.read_csv('extracted_data.csv')

validator = DataValidator(df)
validator.check_completeness(['id', 'name', 'email']) \
         .check_duplicates(['id']) \
         .check_data_types({'id': 'int64', 'name': 'object'}) \
         .check_value_ranges('price', 0, 10000) \
         .check_regex('email', r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') \
         .report()
```

---

## RESUMO CATEGORIA 1

✅ **Coberto:**
- Web Scraping (BeautifulSoup, Selenium, Scrapy)
- APIs (REST, GraphQL, Auth)
- Databases (SQL, NoSQL)
- Files (CSV, JSON, Parquet, Excel)
- Cloud Storage (S3, GCS, Azure)
- Distributed crawling
- Data validation

⚠️ **Poderia ser expandido:**
- Custom connectors
- FTP/SFTP
- Email attachments
- PDF extraction
- Image OCR
- Async crawling
- Proxy rotation

---

**Próxima categoria:** 02_ETL_ELT_PIPELINES.md
