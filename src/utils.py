import os
import logging
import zipfile

import pandas as pd

from settings import RAW_DATA_DIR, PROCESSED_DATA_DIR
from constants import COLUNAS_SELECIONADAS, DTYPES_COLUNAS_SELECIONADAS, PARSE_DATES_COLUNAS_SELECIONADAS


logger = logging.getLogger(__name__)

def obter_dataframe(
    caminho_arquivo: str, 
    colunas_selecionadas: list[str] = COLUNAS_SELECIONADAS, 
    dtype: dict[str, str] = DTYPES_COLUNAS_SELECIONADAS, 
    parse_dates: list[str] = PARSE_DATES_COLUNAS_SELECIONADAS,
):
    #TODO: Caminho de arquivos que pode vir da zip file extraída pode ser uma lista
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f'Arquivo não encontrado em: {caminho_arquivo}')
    logger.info(f'Abrindo arquivo em: {caminho_arquivo}')
    logger.info(f'Selecionando {len(colunas_selecionadas)} colunas: {", ".join(colunas_selecionadas)}')
    logger.info(f'Dtypes das colunas: {", ".join(set(dtype.values()))}')
    logger.info(f'Colunas com datas indicadas para conversão: {", ".join(parse_dates)}')
    
    if caminho_arquivo.endswith('.csv'):
        df = pd.read_csv(
            caminho_arquivo,
            usecols = colunas_selecionadas,
            dtype = dtype,
            parse_dates = parse_dates,
            encoding = 'latin-1', 
            sep = ';',
        )
    elif caminho_arquivo.endswith('.json'):
        df = pd.read_json(
            caminho_arquivo,
            convert_dates = parse_dates,
            dtype = dtype,
            lines = True,
        )
        df = df[ colunas_selecionadas ]
    else:
        raise ValueError('Formato de arquivo não suportado. Utilize apenas .csv ou .json')
    return df

def filtrar_registros_por_uf(df, uf):
    logger.info(f'Filtrando registros para a UF: {uf}')
    return df[ df['sg_uf_estabelecimento'] == uf ]

def salvar_arquivo_processado(
    dataframe,
    nome_arquivo, 
    data_processed_dir = PROCESSED_DATA_DIR
):
    logger.info(f'Salvando arquivo processado em: {os.path.join(data_processed_dir, nome_arquivo)}')
    dataframe.to_csv(
        os.path.join(data_processed_dir, nome_arquivo),
        index = False,
        sep = ';',
        encoding = 'latin-1',
    )

def extrair_arquivo_zip(caminho_arquivo_zip: str, diretorio_destino: str, remover_arquivo_zip: bool = False) -> str:
    logger.info(f'Extraindo arquivo zip: {caminho_arquivo_zip} para o diretório: {diretorio_destino}')
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
            # TODO: Arquivo JSON recebe mais do que um arquivo dentro do zip. Tratar isso.
            if arquivo_zip.namelist():
                logger.info(f'Conteúdo do arquivo zip: {", ".join(arquivo_zip.namelist())}')
            arquivo_zip.extractall(diretorio_destino)
            caminho_arquivo_extraido = os.path.join(diretorio_destino, arquivo_zip.namelist()[0])
            if not os.path.exists(caminho_arquivo_extraido):
                raise FileNotFoundError(f'Arquivo extraído não encontrado em: {caminho_arquivo_extraido}')
            logger.info(f'Extração concluída. Arquivo extraído para: {caminho_arquivo_extraido}')
            if remover_arquivo_zip:
                os.remove(caminho_arquivo_zip)
                logger.info(f'Arquivo zip original em {caminho_arquivo_zip} removido após extração.')
            return caminho_arquivo_extraido
    except Exception as erro:
        logger.error(f'Erro ao tentar extrair {caminho_arquivo_zip}: {erro}')
        raise erro