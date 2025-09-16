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