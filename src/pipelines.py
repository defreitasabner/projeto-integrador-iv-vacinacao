import os
import json
from datetime import datetime

import logging

from utils import remover_arquivo, remover_diretorio, salvar_arquivo_processado, zip_to_dataframe
from settings import VACINACAO_RAW_DATA_DIR, VACINACAO_PROCESSED_DATA_DIR
from constants import ANOS_PIPELINE_INICIAL, COLUNAS_SELECIONADAS, DTYPES_COLUNAS_SELECIONADAS, QUERY_INICIAL, PARSE_DATES_COLUNAS_SELECIONADAS, SUFIXO_PIPELINE_INICIAL

logger = logging.getLogger(__name__)

#TODO: encapsular pipeline básica em uma classe recebendo Dowloader e GerenciadorGoogleDrive como parâmetros
def processar_dados_vacinacao_por_ano(
    ano: int,
    remover_arquivo_zip: bool = False
):

    if ano not in ANOS_PIPELINE_INICIAL:
        raise ValueError(f'Ano inválido: {ano}. Os anos válidos para a pipeline inicial são: {ANOS_PIPELINE_INICIAL}')

    logger.info(f'Iniciando pipeline de dados de vacinação para o ano de {ano} com os seguintes parâmetros:')
    logger.info(f'- Selecionando {len(COLUNAS_SELECIONADAS)} colunas: {", ".join(COLUNAS_SELECIONADAS)}')
    logger.info(f'- Dtypes das colunas: {", ".join(set(DTYPES_COLUNAS_SELECIONADAS.values()))}')
    logger.info(f'- Colunas com datas indicadas para conversão: {", ".join(PARSE_DATES_COLUNAS_SELECIONADAS)}')
    logger.info(f'- Filtro pandas aplicado: {QUERY_INICIAL if QUERY_INICIAL else "Nenhum"}')
    logger.info(f'- Sufixo adicionado aos arquivos processados: {SUFIXO_PIPELINE_INICIAL}')

    caminho_dados_brutos_ano = os.path.join(VACINACAO_RAW_DATA_DIR, str(ano))

    # Criação do diretório de dados processados para o ano e arquivo de metadados
    diretorio_tratado_ano = os.path.join(VACINACAO_PROCESSED_DATA_DIR, str(ano))
    if not os.path.exists(diretorio_tratado_ano):
        os.makedirs(diretorio_tratado_ano)
        metadados = {
            'tema': 'vacinacao',
            'ano': ano,
            'data_execucao_pipeline': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'colunas_selecionadas': COLUNAS_SELECIONADAS,
            'dtypes_colunas': DTYPES_COLUNAS_SELECIONADAS,
            'parse_dates_colunas': PARSE_DATES_COLUNAS_SELECIONADAS,
            'pandas_query_inicial': QUERY_INICIAL,
            'sufixo_arquivos': SUFIXO_PIPELINE_INICIAL
        }
        with open(os.path.join(diretorio_tratado_ano, 'metadados.json'), 'w') as f:
            json.dump(metadados, f, indent = 4)
    else:
        logger.warning(f'Diretório de dados processados para o ano {ano} já existe!' \
                       'Os arquivos já processados não serão sobrescritos.' \
                        'Assegure-se de que os parâmetros da pipeline são os mesmos dos arquivos já processados! '
                        'Para isso, confira o arquivo de metadados.')

    caminhos_arquivos_zip = [
        os.path.join(caminho_dados_brutos_ano, arquivo) 
            for arquivo in os.listdir(caminho_dados_brutos_ano) 
                if arquivo.endswith('.zip')
    ]
    for caminho_arquivo_zip in caminhos_arquivos_zip:
        
        nome_recurso = os.path.basename(caminho_arquivo_zip).replace('.zip', '').replace('.json', '')
        diretorio_destino_recurso = os.path.join(diretorio_tratado_ano, nome_recurso)
        if not os.path.exists(diretorio_destino_recurso):
            os.makedirs(diretorio_destino_recurso)
        nome_arquivo_processado = f'{nome_recurso}_{SUFIXO_PIPELINE_INICIAL}.csv'

        if not os.path.exists(os.path.join(diretorio_destino_recurso, nome_arquivo_processado)):
            logger.info(f'Iniciando processamento do recurso {nome_recurso}')
            try:
                df_dados_processados = zip_to_dataframe(
                    caminho_arquivo_zip = caminho_arquivo_zip, 
                    colunas_selecionadas = COLUNAS_SELECIONADAS, 
                    pandas_query = QUERY_INICIAL,
                    dtype = DTYPES_COLUNAS_SELECIONADAS,
                    parse_dates = PARSE_DATES_COLUNAS_SELECIONADAS
                )
                #TODO: Por enquanto está sendo salvo tudo apenas com a filtragem de SP, mas a ideia é seguir a modelagem
                salvar_arquivo_processado(
                    dataframe = df_dados_processados, 
                    nome_arquivo = nome_arquivo_processado, 
                    diretorio_destino = diretorio_destino_recurso
                )
                if remover_arquivo_zip:
                    remover_arquivo(caminho_arquivo_zip)
            except Exception as e:
                logger.error(f'Ocorreu um erro na pipeline de processamento do recurso {nome_recurso}: {e}')
                raise e
        else:
            logger.info(f'Arquivo processado {nome_arquivo_processado} já existe em {diretorio_destino_recurso}. Ignorando processamento.')