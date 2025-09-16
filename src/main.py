import logging

import settings
import pipelines
import utils
from gerenciador_google_drive import GerenciadorGoogleDrive

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    #filename='pipeline.log',
    #filemode='a'
)

logger = logging.getLogger(__name__)

def main():
    # Preparação do ambiente local e remoto
    settings.criar_estrutura_basica_de_diretorios()

    google_drive = None
    # try:
    #     google_drive = GerenciadorGoogleDrive(
    #         diretorio_dados = settings.GOOGLE_DRIVE_ROOT_DIR, 
    #         diretorio_autenticacao = settings.AUTH_DIR
    #     )
    # except Exception as e:
    #     logger.error(f'Não foi possível inicializar o gerenciador do Google Drive: {e}')
    #     raise e

    # Execução da pipeline de dados
    pipelines.processar_dados_vacinacao_por_ano(
        ano = 2021,
        remover_arquivo_zip = False
    )

    # Upload dos dados processados para o Google Drive
    if google_drive:
        caminho_dados_processados_compactados = utils.compactar_arquivos(settings.PROCESSED_DATA_DIR, 'dados_vacinacao.zip')
        google_drive.upload_arquivo(caminho_dados_processados_compactados)
        utils.remover_arquivo(caminho_dados_processados_compactados)


if __name__ == '__main__':
    main()