import os

from core import config
from db.elastic import ESManager
from db.postgres import get_pg_conn
from services.etl_manager import ETLManager, ProducerTable
from services.state_manager import JsonFileStorage


def main():
    # Postgres
    pg_conn = get_pg_conn(config.DSN)

    # Elasticsearch
    es_manager = ESManager(config.ES_URL)

    for const in ProducerTable:
        storage = JsonFileStorage(os.path.join(
            config.BASEDIR, f'{const.value}_state.json'))

        etl = ETLManager(producer_table=const.value,
                         pg_conn=pg_conn, es_manager=es_manager, storage=storage, index_name=config.ES_INDEX_NAME)
        etl.start()



if __name__ == '__main__':
    # url = 'http://127.0.0.1:9200'
    # es_manager = ESManager(config.ES_URL)
    # es_manager.delete_es_index(index_name='movies')

    # with open(os.path.join(config.BASEDIR, 'db/es_schema.json'), 'r') as fp:
    #     es_manager.create_es_index(fp, index_name='movies')

    main()
