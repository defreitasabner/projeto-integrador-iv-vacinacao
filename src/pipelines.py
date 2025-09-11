import logging

from utils import obter_dataframe, remover_arquivo, remover_diretorio, salvar_arquivo_processado, extrair_arquivo_zip
from download import gerar_url_json, baixar_arquivo_zip_datasus
from settings import DOWNLOADED_DATA_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR
from constants import ANOS_PIPELINE_INICIAL, COLUNAS_SELECIONADAS, DTYPES_COLUNAS_SELECIONADAS, QUERY_INICIAL, PARSE_DATES_COLUNAS_SELECIONADAS, SUFIXO_PIPELINE_INICIAL


logger = logging.getLogger(__name__)


def baixar_e_processar_dados_aplicando_filtragem_inicial(
    mes: int,
    ano: int,
    remover_arquivo_zip: bool = False,
    remover_dados_brutos: bool = False
):
    """Baixa, extrai, processa e salva os dados de vacinação aplicando o filtro inicial definido em QUERY_INICIAL."""
    logger.info(f'Selecionando {len(COLUNAS_SELECIONADAS)} colunas: {", ".join(COLUNAS_SELECIONADAS)}')
    logger.info(f'Dtypes das colunas: {", ".join(set(DTYPES_COLUNAS_SELECIONADAS.values()))}')
    logger.info(f'Colunas com datas indicadas para conversão: {", ".join(PARSE_DATES_COLUNAS_SELECIONADAS)}')

    url_zip_datasus = gerar_url_json(mes, ano)
    nome_recurso = url_zip_datasus.split('/')[-1].replace('.zip', '').replace('.json', '')
    try:
        caminho_arquivo_zip = baixar_arquivo_zip_datasus(
            url = url_zip_datasus, 
            diretorio_destino = DOWNLOADED_DATA_DIR
        )
        caminho_dir_dados_brutos = extrair_arquivo_zip(
            caminho_arquivo_zip = caminho_arquivo_zip, 
            diretorio_destino = RAW_DATA_DIR
        )
        df_dados_processados = obter_dataframe(
            caminho_dir_arquivos_extraidos = caminho_dir_dados_brutos, 
            colunas_selecionadas = COLUNAS_SELECIONADAS, 
            pandas_query = QUERY_INICIAL,
            dtype = DTYPES_COLUNAS_SELECIONADAS,
            parse_dates = PARSE_DATES_COLUNAS_SELECIONADAS
        )
        salvar_arquivo_processado(
            dataframe = df_dados_processados, 
            nome_arquivo = f'{nome_recurso}_{SUFIXO_PIPELINE_INICIAL}.csv', 
            diretorio_destino = PROCESSED_DATA_DIR
        )
        if remover_arquivo_zip:
            remover_arquivo(caminho_arquivo_zip)
        if remover_dados_brutos:
            remover_diretorio(caminho_dir_dados_brutos)
    except Exception as e:
        logger.error(f'Ocorreu um erro na pipeline de processamento do recurso {nome_recurso}: {e}')
        raise e
    
def baixar_e_processar_todos_dados_aplicando_filtragem_inicial(
    anos: list[int] = ANOS_PIPELINE_INICIAL,
    remover_arquivo_zip: bool = False,
    remover_dados_brutos: bool = False
):
    """Baixa, extrai, processa e salva os dados de vacinação aplicando o filtro inicial definido em QUERY_INICIAL para todos os meses dos anos indicados."""
    for ano in anos:
        for mes in range(1, 13):
            baixar_e_processar_dados_aplicando_filtragem_inicial(
                mes = mes,
                ano = ano,
                remover_arquivo_zip = remover_arquivo_zip,
                remover_dados_brutos = remover_dados_brutos
            )