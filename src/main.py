from download import download_servidores
import s3_aws
import os
from clean import filter_csv, read_and_clean_data, concatenate_csv_files
import postgres
import pandas as pd

def main():
    bucket_name = 'servidores-federais-ufob'

    # for year in range(2013, 2014):
    #     for month in range(1, 2):
    #         download_servidores(year, month)
    

    # for file in os.listdir('data/raw'):
    #     filter_csv('data/raw/', file)

    # for file in os.listdir('data/filtered'):
    #     if file.endswith('.csv'):
    #         s3_aws.upload_file_to_s3('data/filtered/' + file, bucket_name, file

    # create an empty DataFrame to hold all of the data
    all_data = pd.DataFrame()

    # iterate over each file in the S3 bucket
    import boto3
    from io import StringIO

    s3 = boto3.client('s3')

    response = s3.list_objects_v2(Bucket=bucket_name)

    for file in response["Contents"]:
        # read the file from S3
        s3_object = s3.get_object(Bucket=bucket_name, Key=file["Key"])
        data = s3_object["Body"].read().decode('utf-8')

        # create a pandas DataFrame from the CSV data
        df = pd.read_csv(StringIO(data), sep=";")

        # add the DataFrame to the all_data DataFrame
        all_data = pd.concat([all_data, df], ignore_index=True)
        all_data.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)
        df = read_and_clean_data(all_data)
    df.to_csv('concatenated.csv', index=False)
    postgres.create_table(df, 'servidores')




if __name__ == '__main__':
    main()



