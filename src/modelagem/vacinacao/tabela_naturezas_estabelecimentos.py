import os

import polars as pl


class TabelaNaturezasEstabelecimentos:
    NOME = 'dim_naturezas_estabelecimentos'
    COLUNAS = ['co_natureza_estabelecimento', 'ds_natureza_estabelecimento']

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(TabelaNaturezasEstabelecimentos.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaNaturezasEstabelecimentos.NOME}.parquet'))