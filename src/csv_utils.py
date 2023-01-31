import os
import pandas as pd
import gc


def filter_csv(file):
    df = pd.read_csv(file, encoding='latin-1', low_memory=False, sep=';')
    df = df.query("COD_ORG_LOTACAO == 26447 or COD_ORG_EXERCICIO == 26447 or COD_UORG_LOTACAO == 26232000000732")
    new_file = file.replace('.csv', '_ufob.csv')
    df.to_csv(new_file, index=False)
    os.remove(file)
    return new_file.split('/')[-1]



# def filter_csv(file):
#     df = pd.read_csv(file, encoding='latin-1', low_memory=False, sep=';')
#     df = df[(df['COD_ORG_LOTACAO'] == 26447) | (df['COD_ORG_EXERCICIO'] == 26447) | (df['COD_UORG_LOTACAO'] == 26232000000732)]
#     new_file = file.replace('.csv', '_ufob.csv')
#     df.to_csv(new_file, index=False)
#     os.remove(file)
#     return  new_file.split('/')[-1]

def read_and_clean_data(filepath):
    df = pd.read_csv(filepath)
    df = df[['Id_SERVIDOR_PORTAL', 'NOME', 'CPF', 'MATRICULA']]
    df['CNPQ_ID'] = None
    df['CNPQ_WEB_ID'] = None
    df.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)
    df.to_csv(filepath, index=False)
    return

def concatenate_csv_files(folder_path, output_file):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    with open(output_file, 'w', encoding='utf-8') as f:
        first_file = True
        for csv_file in csv_files:
            df = pd.read_csv(os.path.join(folder_path, csv_file))
            if first_file:
                df.to_csv(f, index=False, header=True)
                first_file = False
            else:
                df.to_csv(f, index=False, header=False, mode='a')
    return 
