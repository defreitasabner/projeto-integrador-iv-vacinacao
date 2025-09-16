import os

import pandas as pd


def extrair_tabela_uf_municipios(df: pd.DataFrame, diretorio_destino: str) -> None:
    NOME_TABELA_UF_MUNICIPIOS = 'tabela_uf_municipios.csv'
    CAMPOS_TABELA_UF_MUNICIPIO = ['sg_uf', 'cod_municipio']
    
    cols_uf_mun_estabelecimento = ['sg_uf_estabelecimento', 'co_municipio_estabelecimento'] 
    df_uf_mun_estabelecimento = df[cols_uf_mun_estabelecimento].copy().drop_duplicates().reset_index(drop = True)
    df_uf_mun_estabelecimento = df_uf_mun_estabelecimento.rename(
        columns = { k : v for k, v in zip(cols_uf_mun_estabelecimento, CAMPOS_TABELA_UF_MUNICIPIO)}
    )
    
    cols_uf_mun_paciente = ['sg_uf_paciente', 'co_municipio_paciente']
    df_uf_mun_paciente = df[cols_uf_mun_paciente].copy().drop_duplicates().reset_index(drop = True)
    df_uf_mun_paciente = df_uf_mun_paciente.rename(
        columns = { k : v for k, v in zip(cols_uf_mun_paciente, CAMPOS_TABELA_UF_MUNICIPIO)}
    )

    df_uf_mun = pd.concat([df_uf_mun_estabelecimento, df_uf_mun_paciente], ignore_index = True)
    df_uf_mun = df_uf_mun.drop_duplicates().reset_index(drop = True)

    if not os.path.exists(os.path.join(diretorio_destino, NOME_TABELA_UF_MUNICIPIOS)):
        df_uf_mun.to_csv(os.path.join(diretorio_destino, NOME_TABELA_UF_MUNICIPIOS), index = False)
    else:
        df_uf_mun_existente = pd.read_csv(os.path.join(diretorio_destino, NOME_TABELA_UF_MUNICIPIOS), dtype = 'category')
        df_uf_mun_atualizada = pd.concat([df_uf_mun_existente, df_uf_mun], ignore_index = True).drop_duplicates().reset_index(drop = True)
        df_uf_mun_atualizada.to_csv(os.path.join(diretorio_destino, NOME_TABELA_UF_MUNICIPIOS), index = False)


def extrair_tabela_municipios(df: pd.DataFrame) -> pd.DataFrame:
    CAMPOS_TABELA_MUNICIPIOS = ['cod_municipio', 'nome_municipio']
    
    cols_mun_estabelecimento = ['co_municipio_estabelecimento', 'no_municipio_estabelecimento'] 
    df_mun_estabelecimento = df[cols_mun_estabelecimento].copy().drop_duplicates().reset_index(drop = True)
    df_mun_estabelecimento = df_mun_estabelecimento.rename(
        columns = { k : v for k, v in zip(cols_mun_estabelecimento, CAMPOS_TABELA_MUNICIPIOS)}
    )

    cols_mun_paciente = ['co_municipio_paciente', 'no_municipio_paciente']
    df_mun_paciente = df[cols_mun_paciente].copy().drop_duplicates().reset_index(drop = True)
    df_mun_paciente = df_mun_paciente.rename(
        columns = { k : v for k, v in zip(cols_mun_paciente, CAMPOS_TABELA_MUNICIPIOS)}
    )

    df_mun = pd.concat([df_mun_estabelecimento, df_mun_paciente], ignore_index = True)
    return df_mun.drop_duplicates().reset_index(drop = True)

def extrair_tabela_estabelecimentos(df: pd.DataFrame) -> pd.DataFrame:
    CAMPOS_TABELA_ESTABELECIMENTOS = ['cod_municipio', 'cod_estabelecimento']
    
    cols_estabelecimento = ['co_municipio_estabelecimento', 'co_cnes_estabelecimento']
    df_estabelecimentos = df[cols_estabelecimento].copy().drop_duplicates().reset_index(drop = True)
    df_estabelecimentos = df_estabelecimentos.rename(
        columns = { k : v for k, v in zip(cols_estabelecimento, CAMPOS_TABELA_ESTABELECIMENTOS)}
    )

    return df_estabelecimentos.drop_duplicates().reset_index(drop = True)

def extrair_tabela_racas(df: pd.DataFrame) -> pd.DataFrame:
    CAMPOS_TABELA_RACAS = ['cod_raca', 'nome_raca']

    cols_raca = ['co_raca_cor_paciente', 'no_raca_cor_paciente']
    df_racas = df[cols_raca].copy().drop_duplicates().reset_index(drop = True)
    df_racas = df_racas.rename(
        columns = { k : v for k, v in zip(cols_raca, CAMPOS_TABELA_RACAS)}
    )

    return df_racas.drop_duplicates().reset_index(drop = True)