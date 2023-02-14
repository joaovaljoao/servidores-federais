import s3


def main():
    bucket_name = 'servidores-federais-ufob'
    stage_folder = 'data/'
    for ano in range(2022, 2023):
        for mes in range(12, 13):
            s3.upload_to_s3(ano, mes, stage_folder, bucket_name)

if __name__ == '__main__':
    main()



