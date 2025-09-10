from utils import obter_dataframe, filtrar_registros_por_uf, salvar_arquivo_processado, extrair_arquivo_zip
from download import gerar_url_json, baixar_arquivo_zip_datasus

from settings import PROCESSED_DATA_DIR

def processar_dados_de_sp(nome_arquivo_bruto):
    uf_sp = 'SP'
    df = obter_dataframe(nome_arquivo_bruto)
    df = filtrar_registros_por_uf(df, uf_sp)
    salvar_arquivo_processado(df, f'vacinacao_jan_2021_{uf_sp.lower()}.csv')

def pipeline_baixa_filtra_por_uf_sp():
    url = gerar_url_json(mes = 1, ano = 2021)
    caminho_arquivo_zip = baixar_arquivo_zip_datasus(url)
    caminho_arquivo_extraido = extrair_arquivo_zip(caminho_arquivo_zip, diretorio_destino = PROCESSED_DATA_DIR)
    processar_dados_de_sp(caminho_arquivo_extraido)
