import os

import polars as pl


class TabelaDocumentos:
    NOME = 'fato_documentos_vacinacao'
    COLUNAS = ['co_documento', 'co_paciente', 'co_cnes_estabelecimento', 
        'co_vacina', 'co_dose_vacina', 'dt_vacina']
    
    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf\
            .select(TabelaDocumentos.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaDocumentos.NOME}.parquet'))