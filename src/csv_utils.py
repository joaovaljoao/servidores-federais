import os
import pandas as pd



def filter_csv(file):
    chunk_size = 10**6
    new_file = file.replace('.csv', '_ufob.csv')
    header = True

    for chunk in pd.read_csv(file, encoding='latin-1', sep=';', chunksize=chunk_size):
        filtered = chunk[
            (chunk['COD_ORG_LOTACAO'] == 26447) | 
            (chunk['COD_ORG_EXERCICIO'] == 26447) | 
            (chunk['COD_UORG_LOTACAO'] == 26232000000732)
        ]
        filtered.to_csv(new_file, mode='a', index=False, header=header)
        header = False



def filter_file(folder, file):
    filter_csv(os.path.join(folder, file))
    print(f'File {file} filtered successfully!')
    filtered_file = file.replace('.csv', '_ufob.csv')
    os.remove(os.path.join(folder, file))
    print(f'File {file} deleted successfully!')
    return filtered_file



def read_and_clean_data(dataframe):
    df = dataframe
    df = df[['Id_SERVIDOR_PORTAL', 'NOME', 'CPF', 'MATRICULA']]
    df['CNPQ_ID'] = None
    df['CNPQ_WEB_ID'] = None
    df.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)
    return df
