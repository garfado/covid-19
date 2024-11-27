import os
import requests

# URLs
files_to_download = {
    "locations.csv": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/locations.csv",
    "vaccinations.json": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.json",
}

# camada Raw
raw_path = "/home/daniel/covid/datalake/raw"

# Garantir Raw existe
os.makedirs(raw_path, exist_ok=True)

# download
def download_file(file_name, url):
    file_path = os.path.join(raw_path, file_name)
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Arquivo '{file_name}' salvo com sucesso em {file_path}")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar '{file_name}': {e}")

# download arquivo
for file_name, url in files_to_download.items():
    download_file(file_name, url)
