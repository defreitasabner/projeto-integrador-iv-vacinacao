import os
import logging
import zipfile
import io
import shutil

import pandas as pd
import polars as pl

from settings import PROCESSED_DATA_DIR
from constants import SCHEMA_JSON_POLAR

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

def zip_to_dataframe(
    caminho_arquivo_zip: str, 
    colunas_selecionadas: list[str] = None,
    pandas_query: str = None,
    dtype: dict[str, str] = None,
    parse_dates: list[str] = None,
):
    if not os.path.exists(caminho_arquivo_zip):
        raise FileNotFoundError(f'Arquivo zip não encontrado em: {caminho_arquivo_zip}')
    
    logger.info(f'Gerando Dataframe a partir de: {caminho_arquivo_zip}')
    
    df = pd.DataFrame()
    dfs_parciais = []
    with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
        for nome_arquivo in arquivo_zip.namelist():
            with arquivo_zip.open(nome_arquivo, 'r') as arquivo:
                if nome_arquivo.endswith('.csv'):
                    df_parcial = pd.read_csv(
                        arquivo,
                        usecols= colunas_selecionadas,
                        dtype= dtype,
                        parse_dates= parse_dates,
                        encoding = 'latin-1',
                        sep= ';'
                    ).query(pandas_query)
                elif nome_arquivo.endswith('.json'):
                    df_parcial = pd.read_json(
                        arquivo,
                        convert_dates = parse_dates,
                        dtype = dtype
                    )
                    if pandas_query:
                        df_parcial = df_parcial.query(pandas_query)
                    if colunas_selecionadas:
                        df_parcial = df_parcial[colunas_selecionadas]
                else:
                    continue
                dfs_parciais.append(df_parcial)
    df = pd.concat(dfs_parciais, ignore_index=True)
    return df

def zip_json_to_dataframe(
    caminho_dados_brutos_extraidos: str, 
    colunas_selecionadas: list[str],
    dtypes_colunas: dict[str, str] = None,
    parse_dates_colunas: list[str] = None,
    diretorio_destino_tratados: str = None,
):
    if not os.path.exists(caminho_dados_brutos_extraidos):
        raise FileNotFoundError(f'Diretório de dados brutos extraídos não encontrado: {caminho_dados_brutos_extraidos}')
    
    logger.info(f'Gerando Dataframe a partir de: {caminho_dados_brutos_extraidos}')

    TEMP_DIR = os.path.join(diretorio_destino_tratados, 'temp')
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
    try:
        for nome_arquivo in os.listdir(caminho_dados_brutos_extraidos):
            caminho_arquivo_json = os.path.join(caminho_dados_brutos_extraidos, nome_arquivo)
            lf_parcial = pl.read_json(caminho_arquivo_json, schema = SCHEMA_JSON_POLAR).lazy()
            lf_parcial = lf_parcial.filter(pl.col("sg_uf_estabelecimento") == 'SP')
            lf_parcial = lf_parcial.select(colunas_selecionadas)
            lf_parcial.collect().write_parquet(
                os.path.join(TEMP_DIR, f'{nome_arquivo.replace(".json", "")}.parquet')
            )
            # Remove o arquivo JSON depois de processado
            os.remove(caminho_arquivo_json)
        df = pd.concat([
            pd.read_parquet(os.path.join(TEMP_DIR, arquivo)) 
            for arquivo in os.listdir(TEMP_DIR) 
            if arquivo.endswith('.parquet')
        ])
        df = df.astype(dtypes_colunas)
        for coluna in parse_dates_colunas:
            df[coluna] = pd.to_datetime(df[coluna], format = '%Y-%m-%d', errors = 'coerce')
        return df
    except Exception as e:
        logger.error(f'Erro ao processar arquivos JSON em {caminho_dados_brutos_extraidos}: {e}')
    finally:
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)

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

def extrair_arquivo_zip(caminho_arquivo_zip: str) -> str:
    logger.info(f'Extraindo arquivo zip: {caminho_arquivo_zip}')
    dir_arquivos_extraidos = os.path.join(
        os.path.dirname(caminho_arquivo_zip),
        'temp',
        os.path.basename(caminho_arquivo_zip).replace('.zip', '').replace('.json', '')
    )
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
            qtd_arquivos_extraidos = len(arquivo_zip.namelist())
            if os.path.exists(dir_arquivos_extraidos) and len(os.listdir(dir_arquivos_extraidos)) == qtd_arquivos_extraidos:
                logger.info(f'Diretório {dir_arquivos_extraidos} já existe. Ignorando extração.')
                return dir_arquivos_extraidos
            arquivo_zip.extractall(dir_arquivos_extraidos)
            logger.info(f'Extração concluída! {qtd_arquivos_extraidos} arquivo(s) foram extraídos para {dir_arquivos_extraidos}')
            return dir_arquivos_extraidos
    except Exception as erro:
        logger.error(f'Erro ao tentar extrair {caminho_arquivo_zip}: {erro}')
        if os.path.exists(dir_arquivos_extraidos):
            shutil.rmtree(dir_arquivos_extraidos)
        raise erro

def compactar_arquivos(diretorio_origem: str, nome_arquivo_zip: str) -> str:
    if not os.path.exists(diretorio_origem):
        raise FileNotFoundError(f'Diretório de origem não encontrado em: {diretorio_origem}')
    caminho_arquivo_zip = os.path.join(diretorio_origem, nome_arquivo_zip)
    logger.info(f'Compactando arquivos do diretório {diretorio_origem} em: {caminho_arquivo_zip}')
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as arquivo_zip:
            for root, _, files in os.walk(diretorio_origem):
                for file in files:
                    caminho_completo_arquivo = os.path.join(root, file)
                    arquivo_zip.write(
                        caminho_completo_arquivo, 
                        os.path.relpath(caminho_completo_arquivo, diretorio_origem)
                    )
        logger.info(f'Compactação concluída! Arquivo salvo em: {caminho_arquivo_zip}')
        return caminho_arquivo_zip
    except Exception as erro:
        logger.error(f'Erro ao tentar compactar arquivos do diretório {diretorio_origem}: {erro}')
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