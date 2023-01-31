from boto3 import resource
import boto3
import pandas as pd
import io
import os

import concurrent.futures

def read_s3_bucket(bucket_name):
    s3 = resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.all()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(read_s3_file, bucket_name, obj.key) for obj in objects]
        contents = [future.result() for future in concurrent.futures.as_completed(futures)]
    return pd.concat([pd.read_csv(io.StringIO(content)) for content in contents], ignore_index=True)

def upload_to_s3(folder, filename, bucket_name):
    # Connect to S3
    s3 = boto3.client('s3')
    file_path = os.path.join(folder, filename)
    s3.upload_file(file_path, bucket_name, filename)
    print(f'Successfully uploaded {filename} to {bucket_name}.')
    # os.remove(file_path)

def read_s3_file(bucket_name, file_key):
    s3 = resource('s3')
    obj = s3.Object(bucket_name, file_key)
    try:
        file_content = obj['Body'].read().decode('utf-8')
    except:
        file_content = obj['Body'].read().decode('ISO-8859-1')
    return file_content