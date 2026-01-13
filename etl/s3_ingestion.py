import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

# 1. Path Setup
current_dir = Path(__file__).parent 
root_dir = current_dir.parent
load_dotenv(dotenv_path=root_dir / '.env')

# 2. AWS Config
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET_NAME")

# 3. S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region
)

# 4. Source Data Path
base_path = root_dir / "data" / "archive"

def upload_all_files():
    # Find local csv files
    csv_files = list(base_path.glob('*.csv'))
    
    if not csv_files:
        print(f"!! No files found in: {base_path}")
        return

    print(f">> Found {len(csv_files)} files. Starting upload to S3...")

    for file_path in csv_files:
        file_name = file_path.name
        s3_path = f"raw/{file_name}"
        
        try:
            # Upload to S3 raw folder
            s3_client.upload_file(str(file_path), bucket_name, s3_path)
            print(f"OK: {file_name}")
        except Exception as e:
            print(f"Error: {file_name} -> {e}")

if __name__ == "__main__":
    upload_all_files()
