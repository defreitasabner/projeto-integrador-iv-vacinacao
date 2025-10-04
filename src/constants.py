import polars as pl


CODIGOS_MUNICIPIOS_GRANDE_SAO_PAULO: list[str] = [
    '3503901', '3505708', '3506607', '3509007', '3509205', '3510609', '3513009',
    '3513801', '3515004', '3515103', '3515707', '3516309', '3518305', '3518800',
    '3522208', '3522505', '3523107', '3525003', '3526209', '3528502', '3529401',
    '3530607', '3534401', '3539103', '3539806', '3543303', '3544103', '3545001',
    '3546801', '3547304', '3547809', '3548708', '3548807', '3549953', '3550308',
    '3552502', '3552809', '3556453'
]

COLUNAS_SELECIONADAS = [
    'co_documento',
    'co_paciente',
    'tp_sexo_paciente',
    'nu_idade_paciente',
    'co_raca_cor_paciente',
    'no_raca_cor_paciente',
    'sg_uf_paciente',
    'co_municipio_paciente',
    'no_municipio_paciente',
    'co_cnes_estabelecimento',
    'co_municipio_estabelecimento',
    'no_municipio_estabelecimento',
    'sg_uf_estabelecimento',
    'co_vacina',
    'sg_vacina',
    'dt_vacina',
    'ds_vacina',
    'co_dose_vacina',
    'ds_dose_vacina',
    'co_local_aplicacao',
    'ds_local_aplicacao',
    'co_estrategia_vacinacao',
    'ds_estrategia_vacinacao',
]

DTYPES_COLUNAS_SELECIONADAS = { 
    coluna: 'category' for coluna in COLUNAS_SELECIONADAS 
        if coluna != 'nu_idade_paciente' and coluna != 'dt_vacina'
}
DTYPES_COLUNAS_SELECIONADAS['nu_idade_paciente'] = 'Int64'

PARSE_DATES_COLUNAS_SELECIONADAS: list[str] = ['dt_vacina']

QUERY_INICIAL: str = 'sg_uf_estabelecimento == "SP"'

SUFIXO_PIPELINE_INICIAL: str = 'sp'

ANOS_PIPELINE_INICIAL: list[int] = [2021]

SIGLA_MESES: dict[int, str] = {
    1: 'jan',
    2: 'fev',
    3: 'mar',
    4: 'abr',
    5: 'mai',
    6: 'jun',
    7: 'jul',
    8: 'ago',
    9: 'set',
    10: 'out',
    11: 'nov',
    12: 'dez',
}

SCHEMA_JSON_POLAR = {
    "co_documento": pl.Utf8,
    "co_paciente": pl.Utf8,
    "tp_sexo_paciente": pl.Utf8,
    "co_raca_cor_paciente": pl.Utf8,
    "no_raca_cor_paciente": pl.Utf8,
    "co_municipio_paciente": pl.Utf8,
    "co_pais_paciente": pl.Utf8,
    "no_municipio_paciente": pl.Utf8,
    "no_pais_paciente": pl.Utf8,
    "sg_uf_paciente": pl.Utf8,
    "nu_cep_paciente": pl.Utf8,
    "ds_nacionalidade_paciente": pl.Utf8,
    "no_etnia_indigena_paciente": pl.Utf8,
    "co_etnia_indigena_paciente": pl.Utf8,
    "co_cnes_estabelecimento": pl.Utf8,
    "no_razao_social_estabelecimento": pl.Utf8,
    "no_fantasia_estalecimento": pl.Utf8,
    "co_municipio_estabelecimento": pl.Utf8,
    "no_municipio_estabelecimento": pl.Utf8,
    "sg_uf_estabelecimento": pl.Utf8,
    "co_troca_documento": pl.Utf8,
    "co_vacina": pl.Utf8,
    "sg_vacina": pl.Utf8,
    "dt_vacina": pl.Utf8,
    "co_dose_vacina": pl.Utf8,
    "ds_dose_vacina": pl.Utf8,
    "co_local_aplicacao": pl.Utf8,
    "ds_local_aplicacao": pl.Utf8,
    "co_via_administracao": pl.Utf8,
    "ds_via_administracao": pl.Utf8,
    "co_lote_vacina": pl.Utf8,
    "ds_vacina_fabricante": pl.Utf8,
    "dt_entrada_rnds": pl.Utf8,
    "co_sistema_origem": pl.Utf8,
    "ds_sistema_origem": pl.Utf8,
    "st_documento": pl.Utf8,
    "co_estrategia_vacinacao": pl.Utf8,
    "ds_estrategia_vacinacao": pl.Utf8,
    "co_origem_registro": pl.Utf8,
    "ds_origem_registro": pl.Utf8,
    "co_vacina_grupo_atendimento": pl.Utf8,
    "ds_vacina_grupo_atendimento": pl.Utf8,
    "co_vacina_categoria_atendimento": pl.Utf8,
    "ds_vacina_categoria_atendimento": pl.Utf8,
    "co_vacina_fabricante": pl.Utf8,
    "ds_vacina": pl.Utf8,
    "ds_condicao_maternal": pl.Utf8,
    "co_tipo_estabelecimento": pl.Utf8,
    "ds_tipo_estabelecimento": pl.Utf8,
    "co_natureza_estabelecimento": pl.Utf8,
    "ds_natureza_estabelecimento": pl.Utf8,
    "nu_idade_paciente": pl.Utf8,
    "co_condicao_maternal": pl.Utf8,
    "no_uf_paciente": pl.Utf8,
    "no_uf_estabelecimento": pl.Utf8,
    "dt_deletado_rnds": pl.Utf8,
}