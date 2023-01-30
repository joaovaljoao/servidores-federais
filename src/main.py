from s3 import upload_to_s3, read_s3_bucket
from csv_utils import filter_file, read_and_clean_data
from rds_utils import create_table_in_rds
from download import download_servidores

def main():
    bucket_name = 'servidores-ufob'    
    folder = 'output/'

    for ano in range(2022, 2023):
        for mes in range(12, 13):
            # download the file
            download_servidores(ano, mes, folder)
            file = f'{ano}{mes:02d}_Cadastro.csv'
            if file.endswith('_Cadastro.csv'):
                filtered_file = filter_file(folder, file)
                upload_to_s3(folder, filtered_file, bucket_name)
                df = read_s3_bucket(bucket_name)
                df = read_and_clean_data(df)
                create_table_in_rds(df)

if __name__ == '__main__':
    main()
