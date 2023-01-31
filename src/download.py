import zipfile
import requests
import os
import logging

def create_output_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Pasta criada: {folder_path}")

def download_file(url, filename):
    try:
        r = requests.get(url)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(r.content)
        logging.info("Download baixado com sucesso!")
        return True
    except requests.exceptions.HTTPError as errh:
        logging.error("Erro HTTP: %s", errh)
        return False
    except requests.exceptions.ConnectionError as errc:
        logging.error("Erro de conexão: %s", errc)
        return False
    except requests.exceptions.Timeout as errt:
        logging.error("Erro de timeout: %s", errt)
        return False
    except requests.exceptions.RequestException as err:
        logging.error("Algo deu errado: %s", err)
        return False

def extract_zip_file(zip_file, folder):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder)
        logging.info("Arquivo extraído com sucesso!")
        return True
    except zipfile.BadZipFile as e:
        logging.error("Erro: Arquivo zip corrompido ou inválido!")
        return False

def remove_files(folder, extension):
    for file in os.listdir(folder):
        if not (file.endswith(extension) or file.endswith('_ufob.csv')):
            os.remove(os.path.join(folder, file))

def download_servidores(ano, mes, folder, tipo='Servidores_SIAPE', extract=True):
    url = 'https://portaldatransparencia.gov.br/download-de-dados/servidores/{ano}{mes:02d}_{tipo}'.format(ano=ano, mes=mes, tipo=tipo)
    filename = f'{ano}{mes:02d}_{tipo}.zip'
    create_output_folder(folder)
    if download_file(url, folder + filename):
        if extract:
            extract_zip_file(folder + filename, folder)
            remove_files(folder, '_Cadastro.csv')
