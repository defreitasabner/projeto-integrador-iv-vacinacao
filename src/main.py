import logging

import settings
import pipelines

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    #filename='pipeline.log',
    #filemode='a'
)

logger = logging.getLogger(__name__)

def main():
    settings.criar_estrutura_de_diretorios()
    pipelines.baixar_e_processar_todos_dados_aplicando_filtragem_inicial()

if __name__ == '__main__':
    main()