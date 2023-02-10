import boto3
import os

def upload_to_s3(folder, filename, bucket_name):
    s3 = boto3.client('s3')
    file_path = os.path.join(folder, filename)
    s3.upload_file(file_path, bucket_name, filename)
    print(f'Successfully uploaded {filename} to {bucket_name}.')

def download_from_s3(bucket_name, folder):
    s3 = boto3.client('s3')
    files = []
    for obj in s3.list_objects(Bucket=bucket_name)['Contents']:
        filename = obj['Key']
        if filename.endswith('.csv'):
            files.append(filename)
            s3.download_file(bucket_name, filename, os.path.join(folder, filename))
            print(f'Successfully downloaded {filename} from {bucket_name}.')
    return files\

def download_file_from_s3(bucket_name, folder, filename):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, filename, os.path.join(folder, filename))
    print(f'Successfully downloaded {filename} from {bucket_name}.')
    
