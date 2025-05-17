import boto3
import os
import json
from botocore.exceptions import NoCredentialsError

with open('config/aws_config.json') as f:
    aws_config = json.load(f)

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_config['aws_access_key_id'],
    aws_secret_access_key=aws_config['aws_secret_access_key'],
    region_name=aws_config['region_name']
)

BUCKET_NAME = 'ecom-data-pipeline-bucket'
S3_PREFIX = 'raw/json/'        

LOCAL_DIR = 'data/raw/json/'

def upload_file_to_s3(file_path, bucket, s3_key):
    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"✅ Uploaded {file_path} to s3://{bucket}/{s3_key}")
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
    except NoCredentialsError:
        print("❌ AWS credentials not found.")

def main():
    for filename in os.listdir(LOCAL_DIR):
        if filename.endswith('.json'):
            local_path = os.path.join(LOCAL_DIR, filename)
            s3_key = os.path.join(S3_PREFIX, filename)
            upload_file_to_s3(local_path, BUCKET_NAME, s3_key)

if __name__ == '__main__':
    main()
