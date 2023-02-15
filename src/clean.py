import os
import pandas as pd
import gc
import csv

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder created: {folder_path}")

def filter_csv(folder, file, chunk_size=1000, suffix='_ufob.csv', cod_orgao='26447', cod_uorg='26232000000732'):
    filtered_data = []
    with open(folder + file, 'r', encoding='latin1') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        filtered_data.append(header)
        chunk = []

        for i, row in enumerate(reader):
            if row[17] == cod_orgao or str(row[23]) == '26447' or row[15] == cod_uorg:

                    chunk.append(row)
            if (i + 1) % chunk_size == 0:
                filtered_data.extend(chunk)
                chunk = []
        filtered_data.extend(chunk)
    filtred_folder = 'data/filtered'
    create_folder(filtred_folder)
    new_file = os.path.join(filtred_folder,  file.replace('.csv', suffix))
    with open(new_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerows(filtered_data)
        os.remove(folder + file)
    return new_file.split('/')[-1]

def concatenate_csv_files(folder_path, output_file):
    ano = output_file[:4]
    mes = output_file[5:7]
    create_folder(folder_path)
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    with open(output_file, 'w', encoding='latin1') as f:
        first_file = True
        for csv_file in csv_files:
            if csv_file.endswith('_ufob.csv'):
                df = pd.read_csv(os.path.join(folder_path, csv_file), encoding='latin1', sep=';')
                # cretae a new column with the year and month
                df['ano'] = ano
                df['mes'] = mes
                if first_file:
                    df.to_csv(f, index=False, header=True, encoding='latin1', sep=';')
                    print(f'File {csv_file} written to {output_file}')
                    first_file = False
                else:
                    df.to_csv(f, index=False, header=False, mode='a', encoding='latin1', sep=';' )
                    print(f'File {csv_file} appended to {output_file}')
    return 
