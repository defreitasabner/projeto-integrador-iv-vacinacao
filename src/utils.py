import os
import logging
import zipfile

import pandas as pd

from settings import PROCESSED_DATA_DIR


logger = logging.getLogger(__name__)

def obter_dataframe(
    caminho_dir_arquivos_extraidos: str, 
    colunas_selecionadas: list[str] = [],
    pandas_query: str = '',
    dtype: dict[str, str] = None,
    parse_dates: list[str] = [],
):
    if not os.path.exists(caminho_dir_arquivos_extraidos):
        raise FileNotFoundError(f'Diretório de arquivos extraídos não encontrado em: {caminho_dir_arquivos_extraidos}')
    logger.info(f'Abrindo arquivos presentes em: {caminho_dir_arquivos_extraidos}')
    
    arquivos_extraidos = os.listdir(caminho_dir_arquivos_extraidos)

    df = pd.DataFrame()
    if any(arquivo.endswith('.csv') for arquivo in arquivos_extraidos):
        arquivos_extraidos = [arquivo for arquivo in arquivos_extraidos if arquivo.endswith('.csv')]
        dfs_parciais = ( pd.read_csv(
            os.path.join(caminho_dir_arquivos_extraidos, arquivo_csv),
            usecols = colunas_selecionadas,
            dtype = dtype,
            parse_dates = parse_dates,
            encoding = 'latin-1', 
            sep = ';',
        ).query(pandas_query) for arquivo_csv in arquivos_extraidos )
        df = pd.concat(dfs_parciais, ignore_index = True)
    
    elif any(arquivo.endswith('.json') for arquivo in arquivos_extraidos):
        arquivos_extraidos = [arquivo for arquivo in arquivos_extraidos if arquivo.endswith('.json')]
        dfs_parciais = ( pd.read_json(
                os.path.join(caminho_dir_arquivos_extraidos, arquivo_json),
                convert_dates = parse_dates,
                dtype = dtype,
            ).query(pandas_query)[ colunas_selecionadas ] for arquivo_json in arquivos_extraidos )
        df = pd.concat(dfs_parciais, ignore_index = True)
    
    else:
        raise ValueError('Formato de arquivo não suportado. Utilize apenas .csv ou .json')
    
    return df

def salvar_arquivo_processado(
    dataframe: pd.DataFrame,
    nome_arquivo: str, 
    diretorio_destino: str
):
    if not os.path.exists(diretorio_destino):
        raise FileNotFoundError(f'Diretório de destino não encontrado em: {diretorio_destino}')
    logger.info(f'Salvando arquivo processado em: {os.path.join(diretorio_destino, nome_arquivo)}')
    dataframe.to_csv(
        os.path.join(diretorio_destino, nome_arquivo),
        index = False,
        sep = ';',
        encoding = 'latin-1',
    )

def extrair_arquivo_zip(caminho_arquivo_zip: str, diretorio_destino: str) -> str:
    logger.info(f'Extraindo arquivo zip: {caminho_arquivo_zip} para {diretorio_destino}')
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
            qtd_arquivos_extraidos = len(arquivo_zip.namelist())
            dir_arquivos_extraidos = os.path.join(
                diretorio_destino, 
                caminho_arquivo_zip.split(os.sep)[-1].replace('.zip', '').replace('.json', '')
            )
            if os.path.exists(dir_arquivos_extraidos) and len(os.listdir(dir_arquivos_extraidos)) == qtd_arquivos_extraidos:
                logger.info(f'Diretório {dir_arquivos_extraidos} já existe. Ignorando extração.')
                return dir_arquivos_extraidos
            arquivo_zip.extractall(dir_arquivos_extraidos)
            logger.info(f'Extração concluída! {qtd_arquivos_extraidos} arquivo(s) foram extraídos para {dir_arquivos_extraidos}')
            return dir_arquivos_extraidos
    except Exception as erro:
        logger.error(f'Erro ao tentar extrair {caminho_arquivo_zip}: {erro}')
        raise erro

def remover_arquivo(caminho_arquivo: str):
    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)
    else:
        raise FileNotFoundError(f'Arquivo não encontrado: {caminho_arquivo}')

def remover_diretorio(caminho_diretorio: str):
    if os.path.exists(caminho_diretorio):
        for arquivo in os.listdir(caminho_diretorio):
            caminho_arquivo = os.path.join(caminho_diretorio, arquivo)
            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)
            elif os.path.isdir(caminho_arquivo):
                remover_diretorio(caminho_arquivo)
        os.rmdir(caminho_diretorio)
    else:
        raise FileNotFoundError(f'Diretório não encontrado: {caminho_diretorio}')