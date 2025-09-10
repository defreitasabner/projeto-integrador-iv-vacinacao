import os
import logging

import pandas as pd

from config import RAW_DATA_DIR, PROCESSED_DATA_DIR
from constants import COLUNAS_SELECIONADAS, DTYPES_COLUNAS_SELECIONADAS, PARSE_DATES_COLUNAS_SELECIONADAS

logger = logging.getLogger(__name__)

def abrir_arquivo_bruto(
    nome_arquivo: str, 
    colunas_selecionadas: list[str] = COLUNAS_SELECIONADAS, 
    dtype: dict[str, str] = DTYPES_COLUNAS_SELECIONADAS, 
    parse_dates: list[str] = PARSE_DATES_COLUNAS_SELECIONADAS, 
    data_raw_dir = RAW_DATA_DIR
):
    logger.info(f'Abrindo arquivo bruto em: {os.path.join(data_raw_dir, nome_arquivo)}')
    logger.info(f'Selecionando {len(colunas_selecionadas)} colunas: {", ".join(colunas_selecionadas)}')
    logger.info(f'Dtypes das colunas: {", ".join(set(dtype.values()))}')
    logger.info(f'Colunas com datas indicadas para conversão: {", ".join(parse_dates)}')
    
    df = pd.read_csv(
        os.path.join(data_raw_dir, nome_arquivo),
        usecols = colunas_selecionadas,
        dtype = dtype,
        parse_dates = parse_dates,
        encoding = 'latin-1', 
        sep = ';',
    )
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