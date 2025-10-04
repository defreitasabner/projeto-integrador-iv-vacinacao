import os
import polars as pl

class TabelaEstabelecimentos:
    NOME = 'dim_estabelecimentos'
    COLUNAS = ['co_municipio_estabelecimento', 'co_cnes_estabelecimento',
        'no_razao_social_estabelecimento', 'no_fantasia_estalecimento',
        'co_natureza_estabelecimento', 'co_tipo_estabelecimento']
    
    @staticmethod
    def tratar_nome_fantasia(lf_estabelecimentos: pl.LazyFrame) -> pl.LazyFrame:

        df_temp = lf_estabelecimentos.clone().collect()
        agg_nome_fantasia = df_temp.group_by('co_cnes_estabelecimento').agg([
            pl.col('no_razao_social_estabelecimento').mode().sort().first().str.to_uppercase().alias('no_razao_social_estabelecimento'),
            pl.col('no_fantasia_estalecimento').mode().sort().first().str.to_uppercase().alias('no_fantasia_estalecimento'),
            pl.col('co_tipo_estabelecimento').mode().sort().first().alias('co_tipo_estabelecimento'),
        ])

        return lf_estabelecimentos\
            .drop('no_fantasia_estalecimento')\
            .drop('no_razao_social_estabelecimento')\
            .drop('co_tipo_estabelecimento')\
            .join(
                on ='co_cnes_estabelecimento', 
                other= agg_nome_fantasia.lazy(), 
                how='left'
            )

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf\
            .select(TabelaEstabelecimentos.COLUNAS)\
            .pipe(TabelaEstabelecimentos.tratar_nome_fantasia)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaEstabelecimentos.NOME}.parquet'))