import os
import pandas as pd

import subprocess

def filter_csv(filename):
    new_file = filename.replace(".csv", "_ufob.csv")
    
    header_command = f"head -1 {filename} > {new_file}"
    filter_command = f"grep -a -E '26447|26232000000732' {filename} >> {new_file}"
    
    subprocess.run(header_command, shell=True, check=True)
    subprocess.run(filter_command, shell=True, check=True)

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
