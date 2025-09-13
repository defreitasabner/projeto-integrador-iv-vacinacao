import os

# Caminhos dos diretórios principais do projeto
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTH_DIR = os.path.join(PROJECT_ROOT_DIR, 'auth')
DATA_DIR = os.path.join(PROJECT_ROOT_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
DOWNLOADED_DATA_DIR = os.path.join(DATA_DIR, 'downloaded')

# Caminhos dos diretórios no Google Drive
GOOGLE_DRIVE_ROOT_DIR = 'projeto_integrador_iv'

# Determina de quanto em quantos porcento o progresso do download será logado
TAXA_LOG_DOWNLOAD = 10

def criar_estrutura_de_diretorios():
    os.makedirs(RAW_DATA_DIR, exist_ok = True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok = True)
    os.makedirs(DOWNLOADED_DATA_DIR, exist_ok = True)