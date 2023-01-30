import os
import boto3
import pandas as pd
import io

from filter_csv import filter_csv, read_and_clean_data
from download import download_servidores
from rds_utils import create_table_in_rds
from s3 import upload_to_s3, read_s3_file

def main():
    

    folder = 'output/'
    # create folder if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)

    bucket_name = 'servidores-ufob'

    # download the file
    for ano in range(2013, 2023):
        for mes in range(1, 13):
            file = download_servidores(2022, 12)
            if file.endswith('_Cadastro.csv'):
                # filter the file
                filter_csv(os.path.join(folder, file))
                filtered_file = file.replace('.csv', '_ufob.csv')
                # upload filtered file to S3
                upload_to_s3(folder, filtered_file, bucket_name)
                # delete original file
                os.remove(os.path.join(folder, file))
                # delete filtered file
                os.remove(os.path.join(folder, filtered_file))

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    df = pd.DataFrame()
    for obj in bucket.objects.all():
        content = read_s3_file(bucket_name, obj.key)
        df = pd.concat([df, pd.read_csv(io.StringIO(content))], ignore_index=True)
    
    df = read_and_clean_data(df)
    create_table_in_rds(df)

if __name__ == '__main__':
    main()
