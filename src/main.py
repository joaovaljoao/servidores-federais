import os
from filter_csv import filter_csv
from download import download_servidores
from s3 import upload_to_s3, read_s3_file
import pandas as pd
import io

if __name__ == '__main__':
    # download the filed from 2013 until 12/2022
    # for ano in range(2013, 2023):
    #     for mes in range(1, 13):
    #         download_servidores(ano, mes)

    folder = 'output/'
    #list all the files in the directory
    # sort files
    # files_sorted = sorted(os.listdir(folder))
    # for file in files_sorted:
    #     if file.endswith('_Cadastro.csv'):
    #         # filter the file
    #         filter_csv(os.path.join(folder, file))
    #         # os.remove(folder + file)

    #     # upload files to s3
    #     bucket_name = 'servidores-ufob'
    #     upload_to_s3(folder, bucket_name)


    bucket_name = 'servidores-ufob'
    file_name = '201507_Cadastro_ufob.csv'
    content = read_s3_file(bucket_name, file_name)
    df = pd.read_csv(io.StringIO(content), encoding='latin-1', low_memory=False, sep=';')
    print(df.head())

