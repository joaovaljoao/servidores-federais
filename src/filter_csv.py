import os
import pandas as pd

def filter_csv(file):
    df = pd.read_csv(file, encoding='latin-1', low_memory=False, sep=';')
    df = df[(df['COD_ORG_LOTACAO'] == 26447) | (df['COD_ORG_EXERCICIO'] == 26447) | (df['COD_UORG_LOTACAO'] == 26232000000732)]
    new_file = file.replace('.csv', '_ufob.csv')
    df.to_csv(new_file, index=False)
    return 


def read_and_clean_data(dataframe):
    df = dataframe
    df = df[['Id_SERVIDOR_PORTAL', 'NOME', 'CPF', 'MATRICULA']]
    df['CNPQ_ID'] = None
    df['CNPQ_WEB_ID'] = None
    df = df.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'])
    return df
   