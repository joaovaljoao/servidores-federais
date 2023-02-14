import boto3
import os
import pandas as pd
import io
from download import download_servidores
from csv_utils import filter_csv

def upload(folder, filename, bucket_name):
    s3 = boto3.client('s3')
    file_path = os.path.join(folder, filename)
    s3.upload_file(file_path, bucket_name, filename)
    print(f'Successfully uploaded {filename} to {bucket_name}.')

def upload_to_s3(ano, mes, stage_folder, bucket_name):
    download_servidores(ano, mes, stage_folder)
    filename = f'{ano}{mes:02d}_Cadastro.csv'
    filter_csv(stage_folder + filename)
    ufob_filename = f'{ano}{mes:02d}_Cadastro_ufob.csv'
    upload(stage_folder, ufob_filename, bucket_name)