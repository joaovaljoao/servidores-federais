import os
import pandas as pd

def filter_csv(file):
    df = pd.read_csv(file, encoding='latin-1', low_memory=False, sep=';')
    df = df[(df['COD_ORG_LOTACAO'] == 26447) | (df['COD_ORG_EXERCICIO'] == 26447) | (df['COD_UORG_LOTACAO'] == 26232000000732)]
    new_file = file.replace('.csv', '_ufob.csv')
    df.to_csv(new_file, index=False)
    return 

    