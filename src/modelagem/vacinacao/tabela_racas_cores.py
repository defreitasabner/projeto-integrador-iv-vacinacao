import os

import polars as pl


class TabelaRacasCores:
    NOME = 'dim_racas_cores_paciente'
    COLUNAS = ['co_raca_cor_paciente', 'no_raca_cor_paciente']

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(TabelaRacasCores.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaRacasCores.NOME}.parquet'))
