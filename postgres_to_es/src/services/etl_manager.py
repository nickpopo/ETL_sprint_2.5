from enum import Enum

from core import config
from db.elastic import ESManager
from psycopg2.extensions import connection as _connection

from services.coroutines import (enricher_coroutine, loader_coroutine,
                                 merger_coroutine, producer_generator,
                                 transform_coroutine)
from services.state_manager import BaseStorage


class ProducerTable(Enum):
    PERSON = 'person'
    GENRE = 'genre'
    FILMWORK = 'filmwork'


class ETLManager:
    def __init__(self, producer_table: str, pg_conn: _connection, es_manager: ESManager, storage: BaseStorage, index_name: str):
        self.producer_table = producer_table
        self.pg_conn = pg_conn
        self.es_manager = es_manager
        self.storage = storage
        self.index_name = index_name

    def start(self):

        if self.producer_table == ProducerTable.FILMWORK.value:

            loader = loader_coroutine(
                es_manager=self.es_manager, index_name=self.index_name)
            transform = transform_coroutine(loader)
            merger = merger_coroutine(transform, pg_conn=self.pg_conn)
            producer_generator(
                merger, table_name=self.producer_table, pg_conn=self.pg_conn, storage=self.storage)

        elif self.producer_table in (
            ProducerTable.PERSON.value, ProducerTable.GENRE.value):

            m2m_table_name = f'{self.producer_table}s_filmworks'
            column_name = f'{self.producer_table}_id'
         
            loader = loader_coroutine(
                es_manager=self.es_manager, index_name=self.index_name)
            transform = transform_coroutine(loader)
            merger = merger_coroutine(transform, pg_conn=self.pg_conn)
            enricher = enricher_coroutine(
                merger, m2m_table_name=m2m_table_name, column_name=column_name, pg_conn=self.pg_conn)
            producer_generator(
                enricher, table_name=self.producer_table, pg_conn=self.pg_conn, storage=self.storage)

        else:
            raise NotImplementedError
