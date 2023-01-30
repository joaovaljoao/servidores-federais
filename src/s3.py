import boto3
import os

def upload_to_s3(folder, filename, bucket_name):
    # Connect to S3
    s3 = boto3.client('s3')
    file_path = os.path.join(folder, filename)
    s3.upload_file(file_path, bucket_name, filename)
    print(f'Successfully uploaded {filename} to {bucket_name}.')



def read_s3_file(bucket_name, file_name):
    # Connect to S3
    s3 = boto3.client('s3')
    
    # Get the object from the bucket
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    
    # Read the contents of the file
    file_content = obj['Body'].read().decode('utf-8')
    
    # Return the contents of the file
    return file_content