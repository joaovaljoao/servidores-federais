import zipfile
import requests
import os
import logging
import time

def create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder created: {folder_path}")

def retry(tries, delay=1, backoff=2):
    """
    Decorator that retries a function or method until it returns True or the maximum number of attempts is reached.
    :param tries: the maximum number of times to try (not including the first attempt).
    :param delay: the number of seconds to wait before trying again.
    :param backoff: the factor by which to increase the delay after each failure.
    """
    def deco_retry(func):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 0:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as errh:
                    logging.error("HTTP error: %s", errh)
                except requests.exceptions.ConnectionError as errc:
                    logging.error("Connection error: %s", errc)
                except requests.exceptions.Timeout as errt:
                    logging.error("Timeout error: %s", errt)
                except requests.exceptions.RequestException as err:
                    logging.error("Something went wrong: %s", err)
                mtries -= 1
                time.sleep(mdelay)
                mdelay *= backoff
            return False
        return f_retry
    return deco_retry

@retry(tries=3)
def download_file(url, filename):
    try:
        r = requests.get(url)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(r.content)
        logging.info("Download successful!")
        return True
    except Exception as e:
        logging.error("Error downloading file: %s", e)
        return False


def extract_zip_file(zip_file, folder):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder)
            os.remove(zip_file)
        logging.info("File extracted successfully!")
        return True
    except zipfile.BadZipFile as e:
        logging.error("Error: Corrupt or invalid zip file!")
        return False

def remove_files(folder, extension):
    for file in os.listdir(folder):
        if not (file.endswith(extension) or file.endswith('_ufob.csv')):
            os.remove(os.path.join(folder, file))

def download_servidores(year, month, extract=True):
    folder = f"data/zipped/"
    create_folder(folder)
    url = f"https://portaldatransparencia.gov.br/download-de-dados/servidores/{year}{month:02d}_Servidores_SIAPE"
    filename = f"{year}{month:02d}_Servidores_SIAPE.zip"
    filepath = folder + filename
    if download_file(url, filepath):
        if extract:
            extract_folder = f"data/raw/"
            create_folder(extract_folder)
            extract_zip_file(filepath, extract_folder)
            remove_files(extract_folder, '_Cadastro.csv')

