import os
import subprocess
import logging
import shutil
import zipfile

import pandas as pd
import polars as pl

import settings
from constants import SCHEMA_JSON_POLAR
from modelagem.vacinacao import (TabelaDocumentos, TabelaPacientes, TabelaMunicipios, 
                                 TabelaEstabelecimentos, TabelaVacinas, TabelaRacasCores,
                                 TabelaTiposEstabelecimentos, TabelaNaturezasEstabelecimentos, TabelaDoses)

logger = logging.getLogger(__name__)

class PipelineVacinacao:
    def __init__(self, diretorio_dados_brutos_vacinacao: str, diretorio_dados_processados_vacinacao: str):
        # Definição dos caminhos dos arquivos e diretórios
        self.DIRETORIO_DADOS_BRUTOS_VACINACAO = diretorio_dados_brutos_vacinacao
        self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO = diretorio_dados_processados_vacinacao
        # Definição das colunas selecionadas para o processamento
        self.COLUNAS_SELECIONADAS = list(set([
            *TabelaDocumentos.COLUNAS,
            *TabelaPacientes.COLUNAS,
            *TabelaEstabelecimentos.COLUNAS,
            *TabelaMunicipios.COLUNAS,
            *TabelaVacinas.COLUNAS,
            *TabelaRacasCores.COLUNAS,
            *TabelaTiposEstabelecimentos.COLUNAS,
            *TabelaNaturezasEstabelecimentos.COLUNAS,
            *TabelaDoses.COLUNAS
        ]))

    # TODO: Isolar partes importantes em métodos privados e melhorar o fluxo para evitar repetições
    def processar_dados_por_ano(self, ano: int):
        # Garante que os diretórios de dados brutos e processados existam
        os.makedirs(self.DIRETORIO_DADOS_BRUTOS_VACINACAO, exist_ok = True)
        os.makedirs(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, exist_ok = True)
        
        # Verifica se o diretório de dados brutos do ano existe
        DIRETORIO_DADOS_BRUTOS_ANO = os.path.join(self.DIRETORIO_DADOS_BRUTOS_VACINACAO, str(ano))
        if not os.path.exists(DIRETORIO_DADOS_BRUTOS_ANO):
            raise ValueError(f'Diretório de dados brutos para o ano {ano} não existe: {DIRETORIO_DADOS_BRUTOS_ANO}')
        
        # Cria o diretório de dados processados do ano, se não existir
        DIRETORIO_DADOS_PROCESSADOS_ANO = os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, str(ano))
        if not os.path.exists(DIRETORIO_DADOS_PROCESSADOS_ANO):
            os.makedirs(DIRETORIO_DADOS_PROCESSADOS_ANO)

        logger.info(f"Processando dados de vacinação para o ano: {ano}")

        # Lista todos os arquivos .zip no diretório de dados brutos do ano
        CAMINHOS_ARQUIVOS_ZIP = [
            os.path.join(DIRETORIO_DADOS_BRUTOS_ANO, nome_arquivo) 
                for nome_arquivo in os.listdir(DIRETORIO_DADOS_BRUTOS_ANO)
                    if nome_arquivo.endswith('.zip')
        ]
        if len(CAMINHOS_ARQUIVOS_ZIP) == 0:
            raise ValueError(f'Nenhum arquivo .zip encontrado no diretório: {DIRETORIO_DADOS_BRUTOS_ANO}')
        logger.info(f'Foram encontrados {len(CAMINHOS_ARQUIVOS_ZIP)} arquivos no diretório {DIRETORIO_DADOS_BRUTOS_ANO}')
        
        # Processa cada arquivo .zip individualmente
        for caminho_arquivo_zip in CAMINHOS_ARQUIVOS_ZIP:
            nome_arquivo_zip = os.path.basename(caminho_arquivo_zip)
            DIRETORIO_DADOS_PROCESSADOS_MES = os.path.join(
                DIRETORIO_DADOS_PROCESSADOS_ANO, 
                nome_arquivo_zip.replace('.zip', '').replace('.json', '').replace('.csv', '')
            )
            
            if not os.path.exists(DIRETORIO_DADOS_PROCESSADOS_MES):
               os.mkdir(DIRETORIO_DADOS_PROCESSADOS_MES)
            else:
                if self.__verificar_existencia_das_tabelas_modeladas(DIRETORIO_DADOS_PROCESSADOS_MES):
                    logger.info(f'Os dados do arquivo {nome_arquivo_zip} já foram processados. Pulando para o próximo arquivo.')
                    continue
            
            # Cria o diretório temporário para armazenar os arquivos parquet intermediários
            # Caso ele já exista, verifica se todos os arquivos parquet já estão presentes
            # Caso os arquivos já estejam presentes, pula para a extração das tabelas modeladas
            DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET = os.path.join(DIRETORIO_DADOS_PROCESSADOS_MES, 'temp')
            if not os.path.exists(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET):
                os.makedirs(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)
            else:
                if len(os.listdir(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)) > 0:
                    parcialmente_processado = False
                    with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
                        arquivos_a_extrair = [nome_arquivo for nome_arquivo in arquivo_zip.namelist() if nome_arquivo.endswith('.json') or nome_arquivo.endswith('.csv')]
                        if len(os.listdir(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)) == len(arquivos_a_extrair):
                            parcialmente_processado = True
                    if parcialmente_processado:
                        logger.info(f'O diretório temporário de parquets {DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET} já contém todos arquivos parquet. Pulando para a extração das tabelas modeladas.')
                        self.__extrair_tabelas_modeladas(
                            diretorio_temporario_parquets = DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET,
                            diretorio_dados_processados_mes = DIRETORIO_DADOS_PROCESSADOS_MES
                        )
                        continue

            logger.info(f'Iniciando o processamento do arquivo: {nome_arquivo_zip}')
             
            DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS = os.path.join(
                os.path.dirname(caminho_arquivo_zip),
                'temp',
                nome_arquivo_zip.replace('.zip', '').replace('.json', '').replace('.csv', '')
            )
            if not os.path.exists(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS):
                os.makedirs(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)
            with zipfile.ZipFile(caminho_arquivo_zip, 'r') as arquivo_zip:
                if len(os.listdir(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)) == len(arquivo_zip.namelist()):
                    logger.info(f'Diretório {DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS} já existe. Ignorando extração.')
                else:
                    logger.info(f'Extraindo arquivos do zip para o diretório temporário: {DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS}')
                    arquivo_zip.extractall(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)
                    logger.info(f'Extração concluída! {len(arquivo_zip.namelist())} arquivo(s) foram extraídos para {DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS}')
            
            CAMINHOS_ARQUIVOS_BRUTOS_EXTRAIDOS = [
                os.path.join(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS, nome_arquivo)
                    for nome_arquivo in os.listdir(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)
                        if nome_arquivo.endswith('.json') or nome_arquivo.endswith('.csv')
            ]
            
            logger.info(f'Iniciando a conversão dos arquivos JSON extraídos para o formato parquet')
            if not os.path.exists(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET):    
                os.makedirs(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)
            for caminho_arquivo_bruto in CAMINHOS_ARQUIVOS_BRUTOS_EXTRAIDOS:
                logger.info(f'Filtrando e convertendo o arquivo {os.path.basename(caminho_arquivo_bruto)} para o formato parquet')
                self.__filtrar_e_converter_para_parquet(
                    caminho_arquivo = caminho_arquivo_bruto,
                    diretorio_destino = DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET,
                    ano_dados = ano
                )
                os.remove(caminho_arquivo_bruto)
            logger.info(f'Conversão para parquet finalizada com sucesso. Arquivos extraídos foram removidos durante o processo.')
            shutil.rmtree(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)

            logger.info(f'Iniciando a extração das tabelas modeladas para o mês: {nome_arquivo_zip}')
            self.__extrair_tabelas_modeladas(
                diretorio_temporario_parquets = DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET,
                diretorio_dados_processados_mes = DIRETORIO_DADOS_PROCESSADOS_MES
            )
            logger.info(f'Finalizado todo o processamento dos dados presente em: {nome_arquivo_zip}')
        logger.info(f'Finalizado todo o processamento dos dados de vacinação para o ano: {ano}')

    def agregar_dados_por_ano(self, ano: int):
        DIRETORIO_DADOS_PROCESSADOS_ANO = os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, str(ano))
        if not os.path.exists(DIRETORIO_DADOS_PROCESSADOS_ANO):
            raise ValueError(f'Diretório de dados processados para o ano {ano} não existe: {DIRETORIO_DADOS_PROCESSADOS_ANO}')
        
        DIRETORIO_DADOS_PROCESSADOS_ANO_AGREGADOS = os.path.join(
            DIRETORIO_DADOS_PROCESSADOS_ANO, 
            f'agregado_{str(ano)}'
        )
        if not os.path.exists(DIRETORIO_DADOS_PROCESSADOS_ANO_AGREGADOS):
            os.makedirs(DIRETORIO_DADOS_PROCESSADOS_ANO_AGREGADOS)
        
        logger.info(f'Iniciando a agregação dos dados processados para o ano: {ano}')

        DIRETORIOS_MESES_PROCESSADOS = [
            os.path.join(DIRETORIO_DADOS_PROCESSADOS_ANO, nome_diretorio)
                for nome_diretorio in os.listdir(DIRETORIO_DADOS_PROCESSADOS_ANO)
                    if os.path.isdir(os.path.join(DIRETORIO_DADOS_PROCESSADOS_ANO, nome_diretorio))
                        and nome_diretorio != f'agregado_{str(ano)}'
        ]
        
        if len(DIRETORIOS_MESES_PROCESSADOS) == 0:
            raise ValueError(f'Nenhum diretório de mês processado encontrado em: {DIRETORIO_DADOS_PROCESSADOS_ANO}')

        logger.info(f'Foram encontrados {len(DIRETORIOS_MESES_PROCESSADOS)} diretórios de meses processados em: {DIRETORIO_DADOS_PROCESSADOS_ANO}')

        for tabela in [
            TabelaDocumentos,
            TabelaPacientes,
            TabelaMunicipios,
            TabelaEstabelecimentos,
            TabelaVacinas,
            TabelaRacasCores,
            TabelaTiposEstabelecimentos,
            TabelaNaturezasEstabelecimentos,
            TabelaDoses
        ]:
            logger.info(f'Iniciando a agregação da tabela: {tabela.NOME}')
            lf_tabela_ano = pl.concat([
                pl.scan_parquet(os.path.join(diretorio_mes, f'{tabela.NOME}.parquet')).lazy()
                    for diretorio_mes in DIRETORIOS_MESES_PROCESSADOS
                        if os.path.exists(os.path.join(diretorio_mes, f'{tabela.NOME}.parquet'))
            ]).lazy()
            tabela.extrair(
                lf = lf_tabela_ano, 
                diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_ANO_AGREGADOS
            )
            logger.info(f'Tabela {tabela.NOME} agregada com sucesso para o ano: {ano}')

        logger.info(f'Agregação dos dados processados concluída para o ano: {ano}')

    def agregar_dados_todos_os_anos(self):
        diretorios_agregados_anos = []
        for nome_diretorio_ano in os.listdir(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO):
            if os.path.isdir(os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, nome_diretorio_ano)):
                for subdiretorio in os.listdir(os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, nome_diretorio_ano)):
                    if subdiretorio == f'agregado_{nome_diretorio_ano}':
                        diretorios_agregados_anos.append(os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, nome_diretorio_ano, subdiretorio))
        if len(diretorios_agregados_anos) == 0:
            raise ValueError(f'Nenhum diretório de anos agregados encontrado em: {self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO}')
        logger.info(f'Iniciando a agregação dos dados processados para todos os anos')
        for tabela in [
            TabelaDocumentos,
            TabelaPacientes,
            TabelaMunicipios,
            TabelaEstabelecimentos,
            TabelaVacinas,
            TabelaRacasCores,
            TabelaTiposEstabelecimentos,
            TabelaNaturezasEstabelecimentos,
            TabelaDoses
        ]:
            logger.info(f'Iniciando a agregação da tabela: {tabela.NOME}')
            lf_tabela_todos_anos = pl.concat([
                pl.scan_parquet(os.path.join(diretorio_ano, f'{tabela.NOME}.parquet')).lazy()
                    for diretorio_ano in diretorios_agregados_anos
                        if os.path.exists(os.path.join(diretorio_ano, f'{tabela.NOME}.parquet'))
            ]).lazy()
            tabela.extrair(
                lf = lf_tabela_todos_anos, 
                diretorio_destino = os.path.join(self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO, 'agregado_todos_anos')
            )
            logger.info(f'Tabela {tabela.NOME} agregada com sucesso para todos os anos')
        logger.info(f'Agregação dos dados processados concluída para todos os anos')

    def __verificar_existencia_das_tabelas_modeladas(self, diretorio_destino: str):
        arquivos_diretorio = os.listdir(diretorio_destino) if os.path.exists(diretorio_destino) else []
        return all([
            f'{TabelaDocumentos.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaPacientes.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaEstabelecimentos.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaMunicipios.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaVacinas.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaRacasCores.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaTiposEstabelecimentos.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaNaturezasEstabelecimentos.NOME}.parquet' in arquivos_diretorio,
            f'{TabelaDoses.NOME}.parquet' in arquivos_diretorio
        ]) 

    def __filtrar_e_converter_para_parquet(self, caminho_arquivo: str, diretorio_destino: str, ano_dados: int):
        nome_arquivo_parquet = ''
        lf = pl.LazyFrame()
        if caminho_arquivo.endswith('.json'):
            nome_arquivo_parquet = os.path.basename(caminho_arquivo).replace('.json', '.parquet')
            lf = pl.read_json(caminho_arquivo, schema = SCHEMA_JSON_POLAR).lazy()
        elif caminho_arquivo.endswith('.csv'):
            nome_arquivo_parquet = os.path.basename(caminho_arquivo).replace('.csv', '.parquet')
            caminho_arquivo_utf8 = caminho_arquivo.replace('.csv', '_utf8_temp.csv')
            subprocess.Popen(
                ['iconv', '-f', 'latin1', '-t', 'utf8', caminho_arquivo, '-o', caminho_arquivo_utf8],
            )
            lf = pl.scan_csv(caminho_arquivo_utf8, separator=';', schema = SCHEMA_JSON_POLAR, encoding='utf8')
        else:
            raise ValueError(f'Formato de arquivo não suportado para conversão: {caminho_arquivo}')
        lf = lf.pipe(TabelaMunicipios.pre_processamento)\
                .pipe(TabelaPacientes.pre_processamento, ano_dados)\
                .select(self.COLUNAS_SELECIONADAS)\
                .collect()\
                .write_parquet(os.path.join(diretorio_destino, nome_arquivo_parquet))

    def __extrair_tabelas_modeladas(self, diretorio_temporario_parquets: str, diretorio_dados_processados_mes: str):
            lf_mes = pl.scan_parquet(os.path.join(diretorio_temporario_parquets, '*.parquet')).lazy()
            TabelaDocumentos.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaPacientes.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaMunicipios.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaEstabelecimentos.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaVacinas.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaRacasCores.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaTiposEstabelecimentos.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaNaturezasEstabelecimentos.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            TabelaDoses.extrair(lf = lf_mes, diretorio_destino = diretorio_dados_processados_mes)
            logger.info(f'Tabelas modeladas extraídas com sucesso. Removendo arquivos temporários.')
            shutil.rmtree(diretorio_temporario_parquets)

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
    pipeline = PipelineVacinacao(
        diretorio_dados_brutos_vacinacao = settings.VACINACAO_RAW_DATA_DIR,
        diretorio_dados_processados_vacinacao = settings.VACINACAO_PROCESSED_DATA_DIR
    )
    #pipeline.processar_dados_por_ano(ano = 2023)
    #pipeline.agregar_dados_por_ano(ano = 2023)
    pipeline.agregar_dados_todos_os_anos()