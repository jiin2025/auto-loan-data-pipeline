import boto3
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

# Initialize S3 Client
s3 = boto3.client('s3', 
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

bucket_name = os.getenv("S3_BUCKET_NAME")

# List objects in 'raw/' folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw/')

print(f"--- S3 Bucket: {bucket_name} ---")

if 'Contents' in response:
    for obj in response['Contents']:
        # Format size to KB
        file_size = round(obj['Size']/1024, 2)
        print(f"File: {obj['Key']} ({file_size} KB)")
else:
    print(">> Bucket is empty or 'raw/' prefix not found.")
