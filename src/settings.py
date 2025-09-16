import os

# Caminhos dos diretórios principais do projeto
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTH_DIR = os.path.join(PROJECT_ROOT_DIR, 'auth')
DATA_DIR = os.path.join(PROJECT_ROOT_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'brutos')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processados')
VACINACAO_RAW_DATA_DIR = os.path.join(RAW_DATA_DIR, 'vacinacao')
VACINACAO_PROCESSED_DATA_DIR = os.path.join(PROCESSED_DATA_DIR, 'vacinacao')

# Caminhos dos diretórios no Google Drive
GOOGLE_DRIVE_ROOT_DIR = 'projeto_integrador_iv'

# Determina de quanto em quantos porcento o progresso do download será logado
TAXA_LOG_DOWNLOAD = 10

def criar_estrutura_basica_de_diretorios():
    os.makedirs(RAW_DATA_DIR, exist_ok = True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok = True)
    os.makedirs(VACINACAO_RAW_DATA_DIR, exist_ok = True)
    os.makedirs(VACINACAO_PROCESSED_DATA_DIR, exist_ok = True)