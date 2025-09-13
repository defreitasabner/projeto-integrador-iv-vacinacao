import os
import logging

import requests

from constants import SIGLA_MESES
from settings import TAXA_LOG_DOWNLOAD


logger = logging.getLogger(__name__)

class DataSUSDownloader:
    
    def __init__(self):
        pass

    def gerar_url_json(self, mes: int, ano: int):
        mes_str = SIGLA_MESES.get(mes)
        if not mes_str:
            raise ValueError(f'Mês inválido: {mes}. Deve ser um valor entre 1 e 12.')
        return f'https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/PNI/json/vacinacao_{mes_str}_{ano}.json.zip'

    def gerar_url_csv(self, mes: int, ano: int):
        mes_str = SIGLA_MESES.get(mes)
        if not mes_str:
            raise ValueError(f'Mês inválido: {mes}. Deve ser um valor entre 1 e 12.')
        return f'https://arquivosdadosabertos.saude.gov.br/dados/dbbni/vacinacao_{mes_str}_{ano}.zip'
    
    def baixar_arquivo_zip(url: str, diretorio_destino: str) -> str:
        nome_arquivo = url.split("/")[-1]
        caminho_arquivo = os.path.join(diretorio_destino, nome_arquivo)
        if os.path.exists(caminho_arquivo):
            logger.info(f'Arquivo {nome_arquivo} já existe em {diretorio_destino}. Ignorando download.')
            return caminho_arquivo
        else:
            logger.info(f'Arquivo {nome_arquivo} não encontrado em {diretorio_destino}. Iniciando download...')
            with requests.get(url, stream = True) as response:
                try:
                    response.raise_for_status()
                    logger.info(f'=> Baixando dados da URL: {url}')
                    tamanho_total = int(response.headers.get('content-length', 0))
                    total_baixado = 0
                    ultima_pct_logada = 0
                    with open(os.path.join(diretorio_destino, nome_arquivo), "wb") as file:
                        try:
                            for chunk in response.iter_content(chunk_size = 4096):
                                if chunk:
                                    file.write(chunk)
                                    total_baixado += len(chunk)
                                    if tamanho_total > 0:
                                        pct_baixada = (total_baixado / tamanho_total) * 100
                                        if pct_baixada != 0 and pct_baixada > ultima_pct_logada + TAXA_LOG_DOWNLOAD:
                                            logger.info(f'- Progresso do download: {pct_baixada:.2f}%')
                                            ultima_pct_logada += TAXA_LOG_DOWNLOAD
                        except Exception as e:
                            logger.error(f'Ocorreu um erro durante o download do arquivo {nome_arquivo}: {e}')
                            if os.path.exists(os.path.join(diretorio_destino, nome_arquivo)):
                                os.remove(caminho_arquivo)
                                logger.info(f'Arquivo {nome_arquivo} removido devido a erro de download.')
                            raise e
                    logger.info(f'Download concluído. Arquivo salvo em: {caminho_arquivo}')
                    return caminho_arquivo
                except requests.HTTPError as http_err:
                    logger.error(f'Erro HTTP ao tentar baixar o arquivo {nome_arquivo}. Talvez o arquivo não esteja mais disponível nessa URL: {http_err}')