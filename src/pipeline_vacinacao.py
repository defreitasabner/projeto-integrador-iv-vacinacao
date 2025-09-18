import os
import logging
import shutil
import zipfile

import polars as pl

import settings
from constants import SCHEMA_JSON_POLAR

logger = logging.getLogger(__name__)

class PipelineVacinacao:
    def __init__(self, diretorio_dados_brutos_vacinacao: str, diretorio_dados_processados_vacinacao: str):
        # Definição dos caminhos dos arquivos e diretórios
        self.DIRETORIO_DADOS_BRUTOS_VACINACAO = diretorio_dados_brutos_vacinacao
        self.DIRETORIO_DADOS_PROCESSADOS_VACINACAO = diretorio_dados_processados_vacinacao
        # Definição dos nomes das tabelas
        self.TABELA_DOCUMENTOS_VACINACAO_NOME = 'fato_documentos_vacinacao'
        self.TABELA_PACIENTES_NOME = 'dim_pacientes'
        self.TABELA_ESTABELECIMENTOS_NOME = 'dim_estabelecimentos'
        self.TABELA_MUNICIPIOS_NOME = 'dim_municipios'
        self.TABELA_RACAS_CORES_PACIENTE_NOME = 'dim_racas_cores_paciente'
        self.TABELA_TIPOS_ESTABELECIMENTO_NOME = 'dim_tipos_estabelecimento'
        self.TABELA_NATUREZAS_ESTABELECIMENTO_NOME = 'dim_naturezas_estabelecimento'
        self.TABELA_VACINAS_NOME = 'dim_vacinas'
        self.TABELA_DOSES_NOME = 'dim_doses'
        # Definição das colunas de cada tabela
        self.TABELA_DOCUMENTOS_VACINACAO_COLUNAS = ['co_documento', 'co_paciente', 'co_cnes_estabelecimento', 
                                            'co_vacina', 'co_dose_vacina', 'dt_vacina']
        self.TABELA_PACIENTES_COLUNAS = ['co_paciente', 'tp_sexo_paciente', 'nu_idade_paciente',
                                    'co_raca_cor_paciente', 'co_municipio_paciente']
        self.TABELA_ESTABELECIMENTOS_COLUNAS = ['co_municipio_estabelecimento', 'co_cnes_estabelecimento',
                                        'no_razao_social_estabelecimento', 'no_fantasia_estalecimento',
                                        'co_natureza_estabelecimento', 'co_tipo_estabelecimento']
        self.TABELA_MUNICIPIOS_COLUNAS = ['co_municipio_estabelecimento', 'no_municipio_estabelecimento',
                                  'co_municipio_paciente', 'no_municipio_paciente']
        self.TABELA_RACAS_CORES_PACIENTE_COLUNAS = ['co_raca_cor_paciente', 'no_raca_cor_paciente']
        self.TABELA_TIPOS_ESTABELECIMENTO_COLUNAS = ['co_tipo_estabelecimento', 'ds_tipo_estabelecimento']
        self.TABELA_NATUREZAS_ESTABELECIMENTO_COLUNAS = ['co_natureza_estabelecimento', 'ds_natureza_estabelecimento']
        self.TABELA_VACINAS_COLUNAS = ['co_vacina', 'sg_vacina', 'ds_vacina']
        self.TABELA_DOSES_COLUNAS = ['co_dose_vacina', 'ds_dose_vacina']
        # Definição das colunas selecionadas para o processamento
        self.COLUNAS_SELECIONADAS = list(set([
            *self.TABELA_DOCUMENTOS_VACINACAO_COLUNAS,
            *self.TABELA_PACIENTES_COLUNAS,
            *self.TABELA_ESTABELECIMENTOS_COLUNAS,
            *self.TABELA_MUNICIPIOS_COLUNAS,
            *self.TABELA_RACAS_CORES_PACIENTE_COLUNAS,
            *self.TABELA_TIPOS_ESTABELECIMENTO_COLUNAS,
            *self.TABELA_NATUREZAS_ESTABELECIMENTO_COLUNAS,
            *self.TABELA_VACINAS_COLUNAS,
            *self.TABELA_DOSES_COLUNAS
        ]))

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
                nome_arquivo_zip.replace('.zip', '').replace('.json', '')
            )
            if not os.path.exists(DIRETORIO_DADOS_PROCESSADOS_MES):
               os.mkdir(DIRETORIO_DADOS_PROCESSADOS_MES)
            else:
                if self.__verificar_existencia_das_tabelas_modeladas(DIRETORIO_DADOS_PROCESSADOS_MES):
                    logger.info(f'Os dados do arquivo {nome_arquivo_zip} já foram processados. Pulando para o próximo arquivo.')
                    continue
            
            logger.info(f'Iniciando o processamento do arquivo: {nome_arquivo_zip}')
             
            DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS = os.path.join(
                os.path.dirname(caminho_arquivo_zip),
                'temp',
                nome_arquivo_zip.replace('.zip', '').replace('.json', '')
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
            
            CAMINHOS_ARQUIVOS_JSON_BRUTOS_EXTRAIDOS = [
                os.path.join(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS, nome_arquivo)
                    for nome_arquivo in os.listdir(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)
                        if nome_arquivo.endswith('.json')
            ]
            DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET = os.path.join(DIRETORIO_DADOS_PROCESSADOS_MES, 'temp')
            if not os.path.exists(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET):
                os.makedirs(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)
            
            logger.info(f'Iniciando a conversão dos arquivos JSON extraídos para o formato parquet')
            for caminho_arquivo_json_bruto in CAMINHOS_ARQUIVOS_JSON_BRUTOS_EXTRAIDOS:
                logger.info(f'Filtrando e convertendo o arquivo JSON {os.path.basename(caminho_arquivo_json_bruto)} para o formato parquet')
                self.__filtrar_e_converter_para_parquet(
                    caminho_arquivo_json = caminho_arquivo_json_bruto,
                    diretorio_destino = DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET
                )
                os.remove(caminho_arquivo_json_bruto)
            logger.info(f'Conversão de JSON para parquet finalizada com sucesso. Arquivos JSON extraídos foram removidos durante o processo.')
            shutil.rmtree(DIRETORIO_TEMPORARIO_ARQUIVOS_BRUTOS_EXTRAIDOS)

            logger.info(f'Iniciando a extração das tabelas modeladas para o mês: {nome_arquivo_zip}')
            lf_mes = pl.scan_parquet(os.path.join(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET, '*.parquet')).lazy()
            self.__extrair_tabela_documentos(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_pacientes(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_estabelecimentos(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_municipios(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_racas_cores_paciente(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_tipos_estabelecimento(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_naturezas_estabelecimento(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_vacinas(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            self.__extrair_tabela_doses(lf = lf_mes, diretorio_destino = DIRETORIO_DADOS_PROCESSADOS_MES)
            logger.info(f'Tabelas modeladas extraídas com sucesso. Removendo arquivos temporários.')
            shutil.rmtree(DIRETORIO_TEMPORARIO_ARQUIVOS_PARQUET)
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
        
        tabelas = [
            self.TABELA_DOCUMENTOS_VACINACAO_NOME,
            self.TABELA_PACIENTES_NOME,
            self.TABELA_ESTABELECIMENTOS_NOME,
            self.TABELA_MUNICIPIOS_NOME,
            self.TABELA_RACAS_CORES_PACIENTE_NOME,
            self.TABELA_TIPOS_ESTABELECIMENTO_NOME,
            self.TABELA_NATUREZAS_ESTABELECIMENTO_NOME,
            self.TABELA_VACINAS_NOME,
            self.TABELA_DOSES_NOME
        ]
        
        for tabela in tabelas:
            logger.info(f'Agregando dados da tabela: {tabela}')
            caminhos_arquivos_parquet_tabela = []
            for nome_diretorio_mes in os.listdir(DIRETORIO_DADOS_PROCESSADOS_ANO):
                diretorio_mes = os.path.join(DIRETORIO_DADOS_PROCESSADOS_ANO, nome_diretorio_mes)
                if os.path.isdir(diretorio_mes):
                    caminho_arquivo_parquet = os.path.join(diretorio_mes, f'{tabela}.parquet')
                    if os.path.exists(caminho_arquivo_parquet):
                        caminhos_arquivos_parquet_tabela.append(caminho_arquivo_parquet)
            if len(caminhos_arquivos_parquet_tabela) == 0:
                logger.warning(f'Nenhum arquivo parquet encontrado para a tabela {tabela} no ano {ano}. Pulando agregação para esta tabela.')
                continue
            lf_tabela_ano = pl.scan_parquet(caminhos_arquivos_parquet_tabela).lazy()
            lf_tabela_ano.unique().collect().write_parquet(
                os.path.join(DIRETORIO_DADOS_PROCESSADOS_ANO_AGREGADOS, f'{tabela}.parquet')
            )
            logger.info(f'Tabela {tabela} agregada com sucesso para o ano {ano}.')
        logger.info(f'Agregação dos dados processados concluída para o ano: {ano}')

    def __verificar_existencia_das_tabelas_modeladas(self, diretorio_destino: str):
        arquivos_diretorio = os.listdir(diretorio_destino) if os.path.exists(diretorio_destino) else []
        return all([
            f'{self.TABELA_DOCUMENTOS_VACINACAO_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_PACIENTES_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_ESTABELECIMENTOS_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_MUNICIPIOS_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_RACAS_CORES_PACIENTE_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_TIPOS_ESTABELECIMENTO_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_NATUREZAS_ESTABELECIMENTO_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_VACINAS_NOME}.parquet' in arquivos_diretorio,
            f'{self.TABELA_DOSES_NOME}.parquet' in arquivos_diretorio
        ]) 

    def __filtrar_e_converter_para_parquet(self, caminho_arquivo_json: str, diretorio_destino: str):
        nome_arquivo_parquet = os.path.basename(caminho_arquivo_json).replace('.json', '.parquet')
        pl.read_json(caminho_arquivo_json, schema = SCHEMA_JSON_POLAR)\
            .lazy()\
            .filter(pl.col("sg_uf_estabelecimento") == 'SP')\
            .select(self.COLUNAS_SELECIONADAS)\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, nome_arquivo_parquet))
        
    def __extrair_tabela_documentos(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_DOCUMENTOS_VACINACAO_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_DOCUMENTOS_VACINACAO_NOME}.parquet'))

    def __extrair_tabela_pacientes(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_PACIENTES_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_PACIENTES_NOME}.parquet'))

    def __extrair_tabela_estabelecimentos(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_ESTABELECIMENTOS_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_ESTABELECIMENTOS_NOME}.parquet'))
        
    def __extrair_tabela_municipios(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        COLUNAS_MUNICIPIOS_ESTABELECIMENTO = ['co_municipio_estabelecimento', 'no_municipio_estabelecimento']
        municipios_estabelecimentos_lf = lf\
            .select(COLUNAS_MUNICIPIOS_ESTABELECIMENTO)\
            .rename({'no_municipio_estabelecimento': 'no_municipio'})\
            .rename({'co_municipio_estabelecimento': 'co_municipio'})\
            .unique()
        COLUNAS_MUNICIPIOS_PACIENTES = ['co_municipio_paciente', 'no_municipio_paciente']
        municipios_pacientes_lf = lf\
            .select(COLUNAS_MUNICIPIOS_PACIENTES)\
            .rename({'no_municipio_paciente': 'no_municipio'})\
            .rename({'co_municipio_paciente': 'co_municipio'})\
            .unique()
        lf_municipios = pl.concat([municipios_estabelecimentos_lf, municipios_pacientes_lf])\
            .unique()\
            .sort('co_municipio')
        lf_municipios.collect().write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_MUNICIPIOS_NOME}.parquet'))
        
    def __extrair_tabela_racas_cores_paciente(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_RACAS_CORES_PACIENTE_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_RACAS_CORES_PACIENTE_NOME}.parquet'))

    def __extrair_tabela_tipos_estabelecimento(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_TIPOS_ESTABELECIMENTO_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_TIPOS_ESTABELECIMENTO_NOME}.parquet'))

    def __extrair_tabela_naturezas_estabelecimento(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_NATUREZAS_ESTABELECIMENTO_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_NATUREZAS_ESTABELECIMENTO_NOME}.parquet'))

    def __extrair_tabela_vacinas(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_VACINAS_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_VACINAS_NOME}.parquet'))

    def __extrair_tabela_doses(self, lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(self.TABELA_DOSES_COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{self.TABELA_DOSES_NOME}.parquet'))

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
    pipeline = PipelineVacinacao(
        diretorio_dados_brutos_vacinacao = settings.VACINACAO_RAW_DATA_DIR,
        diretorio_dados_processados_vacinacao = settings.VACINACAO_PROCESSED_DATA_DIR
    )
    pipeline.processar_dados_por_ano(ano = 2021)
    pipeline.agregar_dados_por_ano(ano = 2021)