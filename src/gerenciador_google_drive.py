import os
import logging

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from settings import AUTH_DIR

logger = logging.getLogger(__name__)

class GerenciadorGoogleDrive:
    
    def __init__(self, diretorio_dados: str, diretorio_autenticacao: str):
        gauth = GoogleAuth()
        credentials_file_path = os.path.join(diretorio_autenticacao, "credentials.json")
        if os.path.exists(credentials_file_path):
            gauth.LoadClientConfigFile(credentials_file_path)
        gauth.LocalWebserverAuth()
        self.__drive = GoogleDrive(gauth)
        self.__id_diretorio_dados = self.__obter_ou_criar_diretorio(diretorio_dados)

    def __obter_ou_criar_diretorio(self, nome_diretorio: str, id_diretorio_pai: str = None):
        query_ja_existe = f"title='{nome_diretorio}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if id_diretorio_pai:
            query_ja_existe += f" and '{id_diretorio_pai}' in parents"
        lista_diretorios = self.__drive.ListFile({'q': query_ja_existe}).GetList()
        if lista_diretorios:
            logger.info(f'Diretório "{nome_diretorio}" detectado no Google Drive. Ignorando criação.')
            return lista_diretorios[0]['id']
        else:
            metadados_diretorio = {
                'title': nome_diretorio,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if id_diretorio_pai:
                metadados_diretorio['parents'] = [{'id': id_diretorio_pai}]
            diretorio = self.__drive.CreateFile(metadados_diretorio)
            diretorio.Upload()
            return diretorio['id']

    def download_arquivo(self, nome_arquivo: str, diretorio: str = None):
        pass

    def listar_arquivos(self, diretorio: str):
        pass

    def upload_arquivo(self, caminho_arquivo: str):
        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(f'Arquivo não encontrado em {caminho_arquivo}.')
        nome_arquivo = os.path.basename(caminho_arquivo)
        metadados_arquivo = {'title': nome_arquivo, 'parents': [{'id': self.__id_diretorio_dados}]}
        arquivo = self.__drive.CreateFile(metadados_arquivo)
        arquivo.SetContentFile(caminho_arquivo)
        arquivo.Upload()
        logger.info(f'Arquivo {nome_arquivo} enviado para o Google Drive com sucesso.')

