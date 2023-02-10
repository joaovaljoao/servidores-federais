import s3
import csv
from rds import Servidores
from download import download_servidores
import os
import pandas as pd

def main():
    bucket_name = 'servidores-ufob'    
    folder = 'stage/'

    # `read dfs from folder and create table called cargos
    files = os.listdir(folder)
    # create cargos.csv
    df_list = []
    for file in files:
        df = pd.read_csv(os.path.join(folder, file), encoding='utf-8', sep=';')
        df = df[df['TIPO_VINCULO'] == 'Cargo']
        df['ano'] = file[:4]
        df['mes'] = file[4:6]
        df = df[["Id_SERVIDOR_PORTAL", "DESCRICAO_CARGO", "CLASSE_CARGO", "REFERENCIA_CARGO", "PADRAO_CARGO", "NIVEL_CARGO", "COD_UORG_LOTACAO", "UORG_LOTACAO", "COD_ORG_LOTACAO", "ORG_LOTACAO", "COD_ORGSUP_LOTACAO", "ORGSUP_LOTACAO", "COD_UORG_EXERCICIO", "UORG_EXERCICIO", "COD_ORG_EXERCICIO", "ORG_EXERCICIO", "COD_ORGSUP_EXERCICIO", "ORGSUP_EXERCICIO", "TIPO_VINCULO", "SITUACAO_VINCULO", "REGIME_JURIDICO", "JORNADA_DE_TRABALHO", "DATA_INGRESSO_CARGOFUNCAO", "DATA_NOMEACAO_CARGOFUNCAO", "DATA_INGRESSO_ORGAO", "DOCUMENTO_INGRESSO_SERVICOPUBLICO", "DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO", "DIPLOMA_INGRESSO_ORGAO", "DIPLOMA_INGRESSO_SERVICOPUBLICO", "UF_EXERCICIO", "ano", "mes"]]
        df_list.append(df)
    df = pd.concat(df_list, axis=0, ignore_index=True)
    Servidores().create_table(df, 'cargos')



if __name__ == '__main__':
    df = pd.read_excel('Pasta1.xlsx')
    Servidores().create_table(df, 'servidores')
