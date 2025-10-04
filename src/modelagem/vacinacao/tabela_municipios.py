import os
import polars as pl


class TabelaMunicipios:
    NOME = 'dim_municipios'
    COLUNAS = ['co_municipio_estabelecimento', 'no_municipio_estabelecimento']
    __CODIGOS_MUNICIPIOS_GRANDE_SAO_PAULO = [
        '3503901', '3505708', '3506607', '3509007', '3509205', '3510609', '3513009',
        '3513801', '3515004', '3515103', '3515707', '3516309', '3518305', '3518800',
        '3522208', '3522505', '3523107', '3525003', '3526209', '3528502', '3529401',
        '3530607', '3534401', '3539103', '3539806', '3543303', '3544103', '3545001',
        '3546801', '3547304', '3547809', '3548708', '3548807', '3549953', '3550308',
        '3552502', '3552809', '3556453'
    ]
    __df_co_mun_ibge = pl.DataFrame({
        'codigo_faltando_digito': [cod[:-1] for cod in __CODIGOS_MUNICIPIOS_GRANDE_SAO_PAULO],
        'codigo_ibge': __CODIGOS_MUNICIPIOS_GRANDE_SAO_PAULO
    })

    @staticmethod
    def pre_processamento(lf: pl.LazyFrame) -> pl.LazyFrame:
        """ Realiza o pré-processamento necessário nos dados brutos de vacinação para futuramente gerar a tabela de municípios. 
        Deve ser chamado durante o processamento dos dados brutos de vacinação."""
        
        return lf\
            .join(
                TabelaMunicipios.__df_co_mun_ibge.lazy(),
                how='left',
                left_on='co_municipio_estabelecimento',
                right_on='codigo_faltando_digito'
            )\
            .with_columns([
                pl.col('codigo_ibge')
                    .fill_null(pl.col('co_municipio_estabelecimento'))
                    .alias('co_municipio_estabelecimento')
            ])\
            .filter(
                pl.col('co_municipio_estabelecimento').cast(pl.Int64).is_in(
                  [int(codigo) for codigo in TabelaMunicipios.__CODIGOS_MUNICIPIOS_GRANDE_SAO_PAULO]
                )
            )
    
    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf\
            .select(TabelaMunicipios.COLUNAS)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaMunicipios.NOME}.parquet'))