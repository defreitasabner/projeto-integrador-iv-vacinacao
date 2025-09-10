import logging

from settings import criar_diretorios
from pipelines import pipeline_baixa_filtra_por_uf_sp
from download import baixar_arquivo_zip_datasus

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    #filename='pipeline.log',
    #filemode='a'
)

def main():
    criar_diretorios()
    pipeline_baixa_filtra_por_uf_sp()
    #processar_dados_de_sp('vacinacao_jan_2021.csv')

if __name__ == '__main__':
    main()