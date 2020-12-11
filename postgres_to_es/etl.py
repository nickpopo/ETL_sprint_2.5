import logging

import settings
from utilities import (JsonFileStorage, Movie, Person, State, backoff,
                       create_es_index, delete_es_index, dispatcher,
                       get_es_loader)
from utilities.coroutines import (enricher_coroutine, loader_coroutine,
                                  merger_coroutine, producer_generator,
                                  transform_coroutine)

logger = logging.getLogger(__name__)
logger.setLevel(settings.LOGGER_LEVEL)


class ETLManager:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def start(self):
        # filmwork -> merger -> transform -> loader
        # person -> enricher -> merger -> transform -> loader
        # genre -> enricher -> merger -> transform -> loader
        if self.table_name == 'filmwork':

            loader = loader_coroutine()
            transform = transform_coroutine(loader)
            merger = merger_coroutine(transform)
            producer_generator(merger, self.table_name)

        elif self.table_name in ('person', 'genre'):

            m2m_table_name = f'{self.table_name}s_filmworks'
            column_name = f'{self.table_name}_id'

            loader = loader_coroutine()
            transform = transform_coroutine(loader)
            merger = merger_coroutine(transform)            
            enricher = enricher_coroutine(merger, m2m_table_name, column_name)
            producer_generator(enricher, self.table_name)




if __name__ == '__main__':
    # delete_es_index('http://127.0.0.1:9200', 'movies')

    etl_filmwork = ETLManager(table_name='filmwork')
    etl_filmwork.start()

    etl_person = ETLManager(table_name='person')
    etl_person.start()

    etl_genre = ETLManager(table_name='genre')
    etl_genre.start()
