import os

import polars as pl


class TabelaVacinas:
    NOME = 'dim_vacinas'
    COLUNAS = ['co_vacina', 'sg_vacina', 'ds_vacina']

    @staticmethod
    def tratar_descricao_vacina(lf_vacinas: pl.LazyFrame) -> pl.LazyFrame:

        df_temp = lf_vacinas.clone().collect()
        agg_descricao_vacina = df_temp.group_by('co_vacina').agg([
            pl.col('sg_vacina').mode().sort().first().str.to_uppercase().alias('sg_vacina'),
            pl.col('ds_vacina').mode().sort().first().str.to_uppercase().alias('ds_vacina')
        ])

        return lf_vacinas\
            .drop('sg_vacina')\
            .drop('ds_vacina')\
            .join(
                on ='co_vacina', 
                other= agg_descricao_vacina.lazy(), 
                how='left'
            )

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str):
        lf\
            .select(TabelaVacinas.COLUNAS)\
            .pipe(TabelaVacinas.tratar_descricao_vacina)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaVacinas.NOME}.parquet'))