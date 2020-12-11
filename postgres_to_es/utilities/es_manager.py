import json
import logging
import os
from typing import List
from urllib.parse import urljoin
from .decorators import backoff
import requests
import settings

logger = logging.getLogger(__name__)


def is_index_created(url: str, index_name: str) -> bool:
    response = requests.head(urljoin(url, index_name))

    if response.status_code == 200:
        logging.debug(f'Index `{index_name}` has already created.')
        return True
    else:
        return False


def delete_es_index(url: str, index_name: str):
    if is_index_created(url, index_name):
        response = requests.delete(urljoin(url, index_name))
        if response.status_code == 200:
            logging.info(f'Index `{index_name}` was successfully deleted.')
    else:
        logging.info(f'Index `{index_name}` doesn\'t exist.')


@backoff()
def create_es_index(url: str, index_name: str) -> None:

    with open(os.path.join(settings.BASEDIR, 'es_schema.json'), 'r') as fp:
        schema = json.load(fp)

    if not is_index_created(url, index_name):
        response = requests.put(
            urljoin(url, index_name),
            json=schema
        )

        if response.status_code == 200:
            logging.info(f'Index `{index_name}` was successfully created.')
        else:
            response_json = json.loads(response.content.decode())
            logging.error(response_json['error'])


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        Preare bulk request for Elasticsearch
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps(
                    {'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str) -> None:
        """
        It sends a request to Elasticsearch and prints errors about saving data.
        """
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


def get_es_loader(url: str) -> ESLoader:
    return ESLoader(url)
