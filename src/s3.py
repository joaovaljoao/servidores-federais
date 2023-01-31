import boto3
import os

def upload_to_s3(folder, filename, bucket_name):
    s3 = boto3.client('s3')
    file_path = os.path.join(folder, filename)
    s3.upload_file(file_path, bucket_name, filename)
    print(f'Successfully uploaded {filename} to {bucket_name}.')
