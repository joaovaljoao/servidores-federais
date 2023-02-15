import pandas as pd


def filter_by_tipo_vinculo(df, tipo_vinculo):
    return df[df['COD_TIPO_VINCULO'] == tipo_vinculo]

def select_cargos_columns(df):
    return df[['Id_SERVIDOR_PORTAL', 'NOME', 'DESCRICAO_CARGO', 'CLASSE_CARGO', 'REFERENCIA_CARGO', 'PADRAO_CARGO', 'NIVEL_CARGO', 'COD_UORG_LOTACAO', 'UORG_LOTACAO', 'COD_ORG_LOTACAO', 'ORG_LOTACAO', 'COD_UORG_EXERCICIO', 'UORG_EXERCICIO', 'COD_ORG_EXERCICIO', 'ORG_EXERCICIO', 'SITUACAO_VINCULO', 'REGIME_JURIDICO', 'JORNADA_DE_TRABALHO', 'DATA_INGRESSO_CARGOFUNCAO', 'DATA_INGRESSO_ORGAO', 'DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO', 'ano', 'mes']]
    
def select_funcao_columns(df):
    return df[['Id_SERVIDOR_PORTAL', 'NOME', 'SIGLA_FUNCAO', 'NIVEL_FUNCAO', 'FUNCAO', 'CODIGO_ATIVIDADE', 'ATIVIDADE', 'OPCAO_PARCIAL', 'DATA_INGRESSO_CARGOFUNCAO', 'ano', 'mes']]

def select_servidores_columns(df):
    df = df[['Id_SERVIDOR_PORTAL', 'NOME', 'CPF', 'MATRICULA']]
    df_copy = df.copy()
    df_copy.drop_duplicates(subset=['Id_SERVIDOR_PORTAL'], inplace=True)
    return df_copy




