import logging


logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    #filename='pipeline.log',
    #filemode='a'
)

logger = logging.getLogger(__name__)

def main():
    # TODO: implementar CLI para acesso às funcionalidades das pipelines
    pass


if __name__ == '__main__':
    main()