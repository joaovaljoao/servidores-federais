from s3 import upload_to_s3
from csv_utils import filter_csv, read_and_clean_data, concatenate_csv_files
from rds_utils import create_table_in_rds
from download import download_servidores
import os
import pandas as pd

def main():
    bucket_name = 'servidores-ufob'    
    folder = 'output/'

    for ano in range(2022, 2023):
        for mes in range(11, 13):
            # download the file
            download_servidores(ano, mes, folder)
            file = f'{ano}{mes:02d}_Cadastro.csv'
            if file.endswith('_Cadastro.csv'):
                filtered_file = filter_csv(folder + file)
                upload_to_s3(folder, filtered_file, bucket_name)
                read_and_clean_data(folder + filtered_file)
    
    concatenate_csv_files(folder, 'servidores_ufob.csv')
        # drop duplicates
    df = pd.read_csv('servidores_ufob.csv')

    df.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)

    create_table_in_rds(df)


if __name__ == '__main__':
    main() 
