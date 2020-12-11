import logging
import os
from dataclasses import asdict, dataclass
from typing import Coroutine, Generator, Iterable, List, Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from . import (JsonFileStorage, Movie, Person, State, backoff, coroutine,
               create_es_index, dispatcher, get_es_loader)

import settings


@backoff()
def get_pg_conn(dsn: dict) -> _connection:
    return psycopg2.connect(**dsn, cursor_factory=DictCursor)


def producer_generator(target: Coroutine, table_name: str):
    pg_conn = get_pg_conn(settings.DSN)
    cursor = pg_conn.cursor()
    cursor.arraysize = settings.LIMIT

    storage = JsonFileStorage(os.path.join(
        settings.BASEDIR, f'{table_name}_state.json'))
    state_manager = State(storage=storage)

    current_state = state_manager.state.get(table_name)

    if not current_state:
        sql = f"""
                SELECT id, updated_at
                FROM content.{table_name}
                ORDER BY updated_at
                """
    else:
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

    cursor.close()
    pg_conn.close()


@coroutine
def enricher_coroutine(target: Coroutine, m2m_table_name: str, column_name: str):
    pg_conn = get_pg_conn(settings.DSN)
    cursor = pg_conn.cursor()
    cursor.arraysize = settings.LIMIT

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

    cursor.close()
    pg_conn.close()


@coroutine
def merger_coroutine(target: Coroutine) -> None:
    pg_conn = get_pg_conn(settings.DSN)
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

    cursor.close()
    pg_conn.close()


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
def loader_coroutine():
    es_loader = get_es_loader(
        url=settings.ES_URL)
    create_es_index(url=settings.ES_URL, index_name=settings.ES_INDEX_NAME)
    while movies_list := (yield):
        movies_list = (asdict(movie) for movie in movies_list)
        es_loader.load_to_es(
            records=movies_list, index_name=settings.ES_INDEX_NAME)
