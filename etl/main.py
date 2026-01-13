import pandas as pd
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv
from airflow.models import Variable
from sqlalchemy import create_engine, text
from azure_load import AzureDataLakeHandler

# Load Environment Variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')
BASE_DIR = Path("/opt/airflow/data")
ETL_DIR = Path("/opt/airflow/etl/data")

# Cloud & DB Settings
AZURE_CONF = {
    "tenant_id": Variable.get("AZURE_TENANT_ID"),
    "client_id": Variable.get("AZURE_CLIENT_ID"),
    "client_secret": Variable.get("AZURE_CLIENT_SECRET"),
    "account_name": Variable.get("AZURE_ACCOUNT_NAME"),
    "container_name": Variable.get("AZURE_CONTAINER_NAME")
}

AWS_CONF = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "bucket": os.getenv("S3_BUCKET_NAME")
}

DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

def run_etl():
    # Initialize Clients
    s3_client = boto3.client('s3', aws_access_key_id=AWS_CONF["access_key"], aws_secret_access_key=AWS_CONF["secret_key"])
    azure = AzureDataLakeHandler(**AZURE_CONF)
    
    entities = ["customers", "loans", "payments"]

    for entity in entities:
        print(f"Processing: {entity}")
        local_raw_path = BASE_DIR / f"{entity}.csv"

        # Step 1: AWS S3 Backup
        print(f"Step 1: AWS S3 upload")
        s3_client.upload_file(str(local_raw_path), AWS_CONF["bucket"], f"backup/{entity}.csv")

        # Step 2: Azure Bronze Layer (Raw)
        print(f"Step 2: Azure Raw upload")
        azure.upload_file(local_raw_path, f"raw/{entity}/{entity}.csv")

        # Step 3: Data Cleaning & Silver Layer (Staging)
        print(f"Step 3: Data cleaning and Azure Silver upload")
        df = pd.read_csv(local_raw_path)
        df_cleaned = df.fillna(0) # Fill missing values
        
        staging_path = ETL_DIR / "staging" / entity / f"{entity}.csv"
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        df_cleaned.to_csv(staging_path, index=False)
        azure.upload_file(staging_path, f"staging/{entity}/{entity}.csv")

        # Step 4: Load to PostgreSQL
        print(f"Step 4: Loading to PostgreSQL")
        df_cleaned.to_sql(entity, engine, if_exists='replace', index=False)

    # Step 5: Create Data Mart (Gold Layer)
    print("Step 5: Creating Data Mart")
    mart_sql = """
    DROP TABLE IF EXISTS mart_auto_loan_summary;
    CREATE TABLE mart_auto_loan_summary AS
    SELECT 
        c.customer_id,
        COUNT(l.loan_id) AS total_loans,
        SUM(p.payment_amount) AS total_payment_received,
        MAX(l.loan_amount) AS max_loan_limit
    FROM customers c
    LEFT JOIN loans l ON c.customer_id = l.customer_id
    LEFT JOIN payments p ON l.loan_id = p.loan_id
    GROUP BY 1;
    """
    with engine.begin() as conn:
        conn.execute(text(mart_sql))
    
    print("ETL Finished")

if __name__ == "__main__":
    run_etl()
