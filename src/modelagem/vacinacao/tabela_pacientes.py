import os
from datetime import datetime

import polars as pl


class TabelaPacientes:
    NOME = 'dim_pacientes'
    COLUNAS = ['co_paciente', 'tp_sexo_paciente', 
               'ano_nascimento_paciente', 'co_raca_cor_paciente']
    @staticmethod
    def pre_processamento(lf: pl.LazyFrame, ano_dados: int) -> pl.LazyFrame:
        """ Realiza o pré-processamento necessário nos dados brutos de vacinação para futuramente gerar a tabela de pacientes. 
        Deve ser chamado durante o processamento dos dados brutos de vacinação."""
        
        return lf\
            .with_columns([
                pl.col('nu_idade_paciente').map_elements(
                    lambda idade: ano_dados - int(idade) 
                        if (idade is not None) and (int(idade) >= 0 and int(idade) <= 120) 
                        else None,
                    return_dtype = pl.Int16
                ).alias('ano_nascimento_paciente'),
            ])\
            .filter(pl.col('ano_nascimento_paciente').is_not_null())

    @staticmethod
    def tratar_cor_raca(lf_pacientes: pl.LazyFrame) -> pl.LazyFrame:

        agg_cor_raca = lf_pacientes.group_by('co_paciente').agg([
            pl.col('co_raca_cor_paciente').mode().sort().first().alias('co_raca_cor_paciente')
        ])

        return lf_pacientes\
            .drop('co_raca_cor_paciente')\
            .join(
                on ='co_paciente', 
                other= agg_cor_raca.lazy(), 
                how='left'
            )
    
    @staticmethod
    def tratar_sexo(lf_pacientes: pl.LazyFrame) -> pl.LazyFrame:

        agg_sexo = lf_pacientes.group_by('co_paciente').agg([
            pl.col('tp_sexo_paciente').mode().sort().first().alias('tp_sexo_paciente')
        ])

        return lf_pacientes\
            .drop('tp_sexo_paciente')\
            .join(
                on ='co_paciente', 
                other= agg_sexo.lazy(), 
                how='left'
            )
    
    @staticmethod
    def tratar_ano_nascimento(lf_pacientes: pl.LazyFrame) -> pl.LazyFrame:

        agg_ano_nascimento = lf_pacientes.group_by('co_paciente').agg([
            pl.col('ano_nascimento_paciente').mode().sort(descending = True).first().alias('ano_nascimento_paciente')
        ])

        return lf_pacientes\
            .drop('ano_nascimento_paciente')\
            .join(
                on ='co_paciente', 
                other= agg_ano_nascimento.lazy(), 
                how='left'
            )

    @staticmethod
    def extrair(lf: pl.LazyFrame, diretorio_destino: str) -> None:
        lf\
            .select(TabelaPacientes.COLUNAS)\
            .pipe(TabelaPacientes.tratar_cor_raca)\
            .pipe(TabelaPacientes.tratar_sexo)\
            .pipe(TabelaPacientes.tratar_ano_nascimento)\
            .unique()\
            .collect()\
            .write_parquet(os.path.join(diretorio_destino, f'{TabelaPacientes.NOME}.parquet'))