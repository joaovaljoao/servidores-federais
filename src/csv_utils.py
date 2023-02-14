import os
import pandas as pd
import gc
import csv


def filter_csv(file, chunk_size=1000):
    filtered_data = []
    with open(file, 'r', encoding='latin1') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        filtered_data.append(header)
        chunk = []
        unique_ids = set()
        for i, row in enumerate(reader):
            if row[17] == '26447' or row[23] == '26447' or row[15] == '26232000000732':
                id_servidor_portal = row[0]
                if id_servidor_portal not in unique_ids:
                    chunk.append(row)
                    unique_ids.add(id_servidor_portal)
            if (i + 1) % chunk_size == 0:
                filtered_data.extend(chunk)
                chunk = []
        filtered_data.extend(chunk)
    
    new_file = file.replace('.csv', '_ufob.csv')
    with open(new_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerows(filtered_data)
        os.remove(file)
    return new_file.split('/')[-1]

def read_and_clean_data(filepath):
    df = pd.read_csv(filepath, encoding='latin-1', sep=';')
    df = df[['Id_SERVIDOR_PORTAL', 'NOME', 'CPF', 'MATRICULA']]
    df['CNPQ_ID'] = None
    df['CNPQ_WEB_ID'] = None
    df.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)
    df.to_csv(filepath, index=False)
    return

def concatenate_csv_files(folder_path, output_file):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    with open(output_file, 'w', encoding='latin1') as f:
        first_file = True
        for csv_file in csv_files:
            if csv_file.endswith('_ufob.csv'):
                df = pd.read_csv(os.path.join(folder_path, csv_file), encoding='latin1', sep=';')
                if first_file:
                    df.to_csv(f, index=False, header=True, encoding='latin1', sep=';')
                    print(f'File {csv_file} written to {output_file}')
                    first_file = False
                else:
                    df.to_csv(f, index=False, header=False, mode='a', encoding='latin1', sep=';' )
                    print(f'File {csv_file} appended to {output_file}')
    return 
