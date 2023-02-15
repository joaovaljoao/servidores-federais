import boto3
import os

def upload_file_to_s3(filename, bucket_name, s3_key):
    s3 = boto3.resource('s3')
    with open(filename, 'rb') as data:
        s3.Bucket(bucket_name).put_object(Key=s3_key, Body=data)
    os.remove(filename)

def get_bucket_files(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return [file.key for file in bucket.objects.all()]