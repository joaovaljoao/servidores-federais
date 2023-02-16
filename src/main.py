from download import download_servidores
import s3_aws
import os
import transforms as tf
from clean import filter_csv
import postgres
import pandas as pd
import boto3
from io import StringIO



def main():
    bucket_name = 'servidores-federais-ufob'
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name)
    files_in_bucket = [file["Key"] for file in response["Contents"]]
    for year in range(2013, 2023):
        for month in range(1, 13):
            filename = f'{year}{month:02}_Cadastro_ufob.csv'
            if filename in files_in_bucket:
                print(f'File {filename} already exists in S3')
            else:
                download_servidores(year, month)
                filter_csv('data/raw/', filename)
                s3_aws.upload_file_to_s3('data/filtered/' + filename, bucket_name, filename)


    # create an empty DataFrame to hold all of the data
    concat_df = pd.DataFrame()

    for file in response["Contents"]:
        # read the file from S3
        s3_object = s3.get_object(Bucket=bucket_name, Key=file["Key"])
        data = s3_object["Body"].read().decode('utf-8')

        # create a pandas DataFrame from the CSV data
        ano = file["Key"][:4]
        mes = file["Key"][4:6]

        df = pd.read_csv(StringIO(data), sep=";")
        df['ano'] = ano
        df['mes'] = mes


        # add the DataFrame to the all_data DataFrame
        concat_df = pd.concat([concat_df, df], ignore_index=True)
        df_cargos = tf.select_cargos_columns(tf.filter_by_tipo_vinculo(concat_df, 2))
        df_funcao = tf.select_funcao_columns(tf.filter_by_tipo_vinculo(concat_df, 1))

        unique_ids = tf.select_servidores_columns(concat_df)
        

    postgres.create_table(unique_ids, 'servidores')
    postgres.create_table(df_cargos, 'cargos')
    postgres.create_table(df_funcao, 'funcao')


if __name__ == '__main__':
    main()



