import os

import polars as pl


class TabelaDoses:
    NOME = 'dim_doses'
    COLUNAS = ['co_dose_vacina', 'ds_dose_vacina']

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(TabelaDoses.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaDoses.NOME}.parquet'))