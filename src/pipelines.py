from utils import abrir_arquivo_bruto, filtrar_registros_por_uf, salvar_arquivo_processado


def processar_dados_de_sp(nome_arquivo_bruto):
    uf_sp = 'SP'
    df = abrir_arquivo_bruto(nome_arquivo_bruto)
    df = filtrar_registros_por_uf(df, uf_sp)
    salvar_arquivo_processado(df, f'vacinacao_jan_2021_{uf_sp.lower()}.csv')