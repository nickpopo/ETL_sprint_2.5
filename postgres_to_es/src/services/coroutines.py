from dataclasses import asdict
from datetime import datetime
from typing import Coroutine

from core import config
from db.elastic import ESManager
from models import Movie, Person
from psycopg2.extensions import connection as _connection

from services.decorators import coroutine
from services.helpers import dispatcher
from services.state_manager import BaseStorage, State

__all__ = ['producer_generator',
           'enricher_coroutine',
           'merger_coroutine',
           'transform_coroutine',
           'loader_coroutine',
           ]


def producer_generator(target: Coroutine, *, table_name: str, pg_conn: _connection, storage: BaseStorage):
    cursor = pg_conn.cursor()
    cursor.arraysize = config.LIMIT

    state_manager = State(storage=storage)
    default_date = str(datetime(year=1700, month=1, day=1))
    current_state = state_manager.state.get(table_name, default_date)

    sql = f"""
            SELECT id, updated_at
            FROM content.{table_name}
            WHERE updated_at >= %s
            ORDER BY updated_at
            """

    cursor.execute(sql, (current_state,))

    while True:
        batch_result = cursor.fetchmany()
        ids_list = [item['id'] for item in batch_result]

        if not ids_list:
            break

        target.send(ids_list)

        state_manager.set_state(
            key=table_name, value=str(batch_result[-1]['updated_at']))


@coroutine
def enricher_coroutine(target: Coroutine, *, m2m_table_name: str, column_name: str, pg_conn: _connection):
    cursor = pg_conn.cursor()
    cursor.arraysize = config.LIMIT

    while ids_list := (yield):
        sql = f"""
                SELECT DISTINCT filmwork_id FROM content.{m2m_table_name}
                WHERE {column_name} IN %s
                """
        cursor.execute(sql, (tuple(ids_list),))

        while True:
            batch_result = cursor.fetchmany()
            ids_list = [item['filmwork_id'] for item in batch_result]

            if not ids_list:
                break

            target.send(ids_list)


@coroutine
def merger_coroutine(target: Coroutine, *, pg_conn: _connection) -> None:
    cursor = pg_conn.cursor()

    while ids_list := (yield):

        cursor.execute("""
        SELECT content.filmwork.id, content.filmwork.title, content.filmwork.description, content.filmwork.rating,
            array_agg(DISTINCT content.genre.name) as genres,
            array_agg(DISTINCT ARRAY["content"."person"."id"::text, "content"."person"."full_name"]) FILTER (WHERE "content"."career"."name" = 'actor') AS "actors",
            array_agg(DISTINCT ARRAY["content"."person"."id"::text, "content"."person"."full_name"]) FILTER (WHERE "content"."career"."name" = 'writer') AS "writers",
            array_agg(DISTINCT ARRAY["content"."person"."id"::text, "content"."person"."full_name"]) FILTER (WHERE "content"."career"."name" = 'director') AS "directors"
        FROM content.filmwork
        LEFT JOIN content.genres_filmworks ON content.genres_filmworks.filmwork_id = content.filmwork.id
        LEFT JOIN content.genre ON content.genre.id = content.genres_filmworks.genre_id
        LEFT JOIN content.persons_filmworks ON content.persons_filmworks.filmwork_id = content.filmwork.id
        LEFT JOIN content.person ON content.person.id = content.persons_filmworks.person_id
        LEFT JOIN content.career ON content.career.id = content.persons_filmworks.role_id
        WHERE content.filmwork.id IN %s
        GROUP BY content.filmwork.id
        """, (tuple(ids_list),))

        raw_rows = cursor.fetchall()
        target.send(raw_rows)


@coroutine
def transform_coroutine(target):
    while raw_rows := (yield):
        movies = []
        for row in raw_rows:

            movies.append(Movie(
                id=row['id'],
                title=row['title'],
                description=row['description'],
                rating=(float(row['rating']) if row['rating'] else None),
                genres=dispatcher(row['genres'], 'genres'),
                actors_names=dispatcher(row['actors'], 'persons_names'),
                writers_names=dispatcher(row['actors'], 'persons_names'),
                directors_names=dispatcher(row['actors'], 'persons_names'),
                actors=dispatcher(row['actors'], 'persons'),
                writers=dispatcher(row['writers'], 'persons'),
                directors=dispatcher(row['directors'], 'persons')))

        target.send(movies)


@coroutine
def loader_coroutine(es_manager: ESManager, index_name: str):
    while movies_list := (yield):
        movies_list = (asdict(movie) for movie in movies_list)
        es_manager.load_to_es(
            records=movies_list, index_name=index_name)
