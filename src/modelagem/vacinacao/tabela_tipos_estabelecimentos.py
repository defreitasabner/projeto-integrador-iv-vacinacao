import os

import polars as pl


class TabelaTiposEstabelecimentos:
    NOME = 'dim_tipos_estabelecimentos'
    COLUNAS = ['co_tipo_estabelecimento', 'ds_tipo_estabelecimento']

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf.select(TabelaTiposEstabelecimentos.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaTiposEstabelecimentos.NOME}.parquet'))