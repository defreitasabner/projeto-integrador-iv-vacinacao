import os

DATA_DIR = os.path.join('..', 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
DOWNLOADED_DATA_DIR = os.path.join(DATA_DIR, 'downloaded')

# Determina de quanto em quantos porcento o progresso do download será logado
TAXA_LOG_DOWNLOAD = 10

def criar_estrutura_de_diretorios():
    os.makedirs(RAW_DATA_DIR, exist_ok = True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok = True)
    os.makedirs(DOWNLOADED_DATA_DIR, exist_ok = True)