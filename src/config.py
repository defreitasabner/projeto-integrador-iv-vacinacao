import os

DATA_DIR = os.path.join('..', 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

def criar_diretorios():
    os.makedirs(RAW_DATA_DIR, exist_ok = True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok = True)