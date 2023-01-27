import zipfile
import requests
import os

def download_servidores(ano, mes, tipo='Servidores_SIAPE'):
    url = 'https://portaldatransparencia.gov.br/download-de-dados/servidores/{ano}{mes:02d}_{tipo}'.format(ano=ano, mes=mes, tipo=tipo)
    filename = f'{ano}{mes:02d}_{tipo}.zip'
    folder = 'output/'
    try:
        r = requests.get(url)
        r.raise_for_status()
        with open(folder + filename, 'wb') as f:
            f.write(r.content)
        print("File downloaded successfully!")

        # extract files from zip file
        if zipfile.is_zipfile(folder + filename):
            try:
                with zipfile.ZipFile(folder + filename, 'r') as zip_ref:
                    zip_ref.extractall(folder)
                print("File extracted successfully!")
            except zipfile.BadZipFile as e:
                print("Error: Invalid zip file")
        else:
            print("Error: Not a valid zip file")

    except requests.exceptions.HTTPError as errh:
        print ("HTTP Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("Something went wrong:",err)
    # remove files that are not needed
    for file in os.listdir(folder):
        if not file.endswith('_Cadastro.csv'):
            os.remove(os.path.join(folder, file))
