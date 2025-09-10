import logging

from config import criar_diretorios
from pipelines import processar_dados_de_sp

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    #filename='pipeline.log',
    #filemode='a'
)

def main():
    criar_diretorios()
    processar_dados_de_sp('vacinacao_jan_2021.csv')

if __name__ == '__main__':
    main()